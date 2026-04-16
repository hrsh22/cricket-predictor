from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"

EXTRA_2026_MATCHES = [
    {
        "source_match_id": "1527693",
        "event_slug": "cricipl-mum-roy-2026-04-12",
        "commentary_url": "https://www.espncricinfo.com/series/ipl-2026-1510719/mumbai-indians-vs-royal-challengers-bengaluru-20th-match-1527693/ball-by-ball-commentary",
    },
    {
        "source_match_id": "1529264",
        "event_slug": "cricipl-sun-raj-2026-04-13",
        "commentary_url": "https://www.espncricinfo.com/series/ipl-2026-1510719/sunrisers-hyderabad-vs-rajasthan-royals-21st-match-1529264/ball-by-ball-commentary",
    },
]


@dataclass
class SeasonSource:
    season: int
    csv_path: Path


def main() -> None:
    output_2026 = DATA_DIR / "analysis" / "polymarket-event-study-2026.csv"
    output_2025_2026 = DATA_DIR / "analysis" / "polymarket-event-study-2025-2026.csv"

    season_rows = load_base_rows(
        [
            SeasonSource(2025, DATA_DIR / "polymarket-ball-odds-ipl-2025.csv"),
            SeasonSource(2026, DATA_DIR / "polymarket-ball-odds-ipl-2026.csv"),
        ]
    )
    season_rows = ensure_extra_2026_rows(season_rows)

    all_feature_rows = build_feature_rows(season_rows)
    rows_2026 = [row for row in all_feature_rows if row["season"] == "2026"]
    rows_2025_2026 = [
        row for row in all_feature_rows if row["season"] in {"2025", "2026"}
    ]

    output_2026.parent.mkdir(parents=True, exist_ok=True)
    write_csv(output_2026, rows_2026)
    write_csv(output_2025_2026, rows_2025_2026)

    summary = {
        "output_2026": str(output_2026),
        "rows_2026": len(rows_2026),
        "matches_2026": len({row["source_match_id"] for row in rows_2026}),
        "output_2025_2026": str(output_2025_2026),
        "rows_2025_2026": len(rows_2025_2026),
        "matches_2025_2026": len({row["source_match_id"] for row in rows_2025_2026}),
    }
    print(json.dumps(summary, indent=2))


def load_base_rows(sources: list[SeasonSource]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for source in sources:
        with source.csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row["season"] = str(source.season)
                rows.append(row)
    return rows


def ensure_extra_2026_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    existing_ids = {row["source_match_id"] for row in rows if row["season"] == "2026"}
    enriched_rows = list(rows)

    for extra in EXTRA_2026_MATCHES:
        if extra["source_match_id"] in existing_ids:
            continue

        timeline = fetch_timeline(extra["event_slug"], extra["commentary_url"])
        for row in timeline["rows"]:
            enriched_rows.append(
                {
                    "season": "2026",
                    "match_date": row["timestamp"][:10],
                    "source_match_id": extra["source_match_id"],
                    "match_slug": derive_match_slug(
                        extra["commentary_url"], extra["source_match_id"]
                    ),
                    "event_slug": extra["event_slug"],
                    "commentary_url": extra["commentary_url"],
                    "delivery_source_mode": timeline["deliverySourceMode"],
                    "inning": str(row["inning"]),
                    "batting_team": row["battingTeam"],
                    "bowling_team": row["bowlingTeam"],
                    "ball": row["ball"],
                    "event": row["event"],
                    "commentary": row["commentary"] or "",
                    "timestamp": row["timestamp"],
                    "timestamp_source": row["timestampSource"],
                    "primary_team": row["primaryTeam"],
                    "primary_before_pct": to_str(row["primaryBefore"]),
                    "primary_after_pct": to_str(row["primaryAfter"]),
                    "primary_delta_pct": to_str(row["primaryDelta"]),
                    "secondary_team": row["secondaryTeam"],
                    "secondary_before_pct": to_str(row["secondaryBefore"]),
                    "secondary_after_pct": to_str(row["secondaryAfter"]),
                    "secondary_delta_pct": to_str(row["secondaryDelta"]),
                    "pricing_source_before": row["pricingSourceBefore"] or "",
                    "pricing_source_after": row["pricingSourceAfter"] or "",
                }
            )

    return enriched_rows


def fetch_timeline(event_slug: str, commentary_url: str) -> dict:
    command = [
        "npx",
        "tsx",
        "scripts/report/ball-odds-timeline.ts",
        "--event-slug",
        event_slug,
        "--commentary-url",
        commentary_url,
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def derive_match_slug(commentary_url: str, match_id: str) -> str:
    marker = f"-{match_id}/"
    prefix = commentary_url.split(marker, 1)[0]
    return prefix.rsplit("/", 1)[-1] + f"-{match_id}"


def to_str(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def parse_event(event: str) -> dict[str, object]:
    parts = event.split("+") if event else [""]
    total = wickets = wides = noballs = byes = legbyes = bat_runs = 0
    for part in parts:
        if part == "W":
            wickets += 1
            continue
        if part.endswith("wd"):
            value = int(part[:-2]) if part[:-2] else 1
            wides += value
            total += value
            continue
        if part.endswith("nb"):
            value = int(part[:-2]) if part[:-2] else 1
            noballs += value
            total += value
            continue
        if part.endswith("lb"):
            value = int(part[:-2]) if part[:-2] else 1
            legbyes += value
            total += value
            continue
        if part.endswith("b"):
            value = int(part[:-1]) if part[:-1] else 1
            byes += value
            total += value
            continue
        if part.isdigit():
            value = int(part)
            bat_runs += value
            total += value
    return {
        "total_runs": total,
        "bat_runs": bat_runs,
        "wickets": wickets,
        "wides": wides,
        "noballs": noballs,
        "byes": byes,
        "legbyes": legbyes,
        "legal": wides == 0 and noballs == 0,
    }


def build_feature_rows(raw_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_match: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in raw_rows:
        by_match[(row["season"], row["source_match_id"])].append(row)

    feature_rows: list[dict[str, str]] = []
    for (season, match_id), match_rows in by_match.items():
        match_rows.sort(key=lambda row: (int(row["inning"]), float(row["ball"])))
        innings1_total: int | None = None
        state = {
            1: {"runs": 0, "wkts": 0, "legal": 0},
            2: {"runs": 0, "wkts": 0, "legal": 0},
        }
        previous_event = ""
        previous_two_event = ""
        boundary_streak = six_streak = dot_streak = 0
        wickets_in_over: dict[tuple[int, int], int] = defaultdict(int)

        for index, row in enumerate(match_rows):
            inning = int(row["inning"])
            over = int(float(row["ball"]))
            event_info = parse_event(row["event"])

            batting_is_primary = row["primary_team"] == row["batting_team"]
            before = parse_optional_float(
                row["primary_before_pct"]
                if batting_is_primary
                else row["secondary_before_pct"]
            )
            after = parse_optional_float(
                row["primary_after_pct"]
                if batting_is_primary
                else row["secondary_after_pct"]
            )
            delta = parse_optional_float(
                row["primary_delta_pct"]
                if batting_is_primary
                else row["secondary_delta_pct"]
            )

            state_before = state[inning]
            target = (
                innings1_total + 1
                if inning == 2 and innings1_total is not None
                else None
            )
            balls_remaining = 120 - state_before["legal"] if inning == 2 else None
            required_runs = (
                target - state_before["runs"] if target is not None else None
            )
            required_rr = (
                required_runs * 6 / balls_remaining
                if required_runs is not None and balls_remaining and balls_remaining > 0
                else None
            )
            current_rr = (
                state_before["runs"] * 6 / state_before["legal"]
                if state_before["legal"] > 0
                else None
            )

            feature_rows.append(
                {
                    "season": season,
                    "match_date": row["match_date"],
                    "source_match_id": match_id,
                    "match_slug": row["match_slug"],
                    "event_slug": row["event_slug"],
                    "inning": row["inning"],
                    "ball": row["ball"],
                    "over": str(over),
                    "batting_team": row["batting_team"],
                    "fielding_team": row["bowling_team"],
                    "event": row["event"],
                    "commentary": row["commentary"],
                    "timestamp": row["timestamp"],
                    "timestamp_source": row["timestamp_source"],
                    "pricing_source_before": row["pricing_source_before"],
                    "pricing_source_after": row["pricing_source_after"],
                    "batting_before_pct": fmt(before),
                    "batting_after_pct": fmt(after),
                    "batting_delta_pct": fmt(delta),
                    "runs_before": str(state_before["runs"]),
                    "wkts_before": str(state_before["wkts"]),
                    "legal_balls_before": str(state_before["legal"]),
                    "current_rr_before": fmt(current_rr),
                    "target": fmt(target),
                    "required_runs_before": fmt(required_runs),
                    "balls_remaining_before": fmt(balls_remaining),
                    "required_rr_before": fmt(required_rr),
                    "phase": classify_phase(over, inning),
                    "prev_event": previous_event,
                    "prev2_event": previous_two_event,
                    "boundary_streak_before": str(boundary_streak),
                    "six_streak_before": str(six_streak),
                    "dot_streak_before": str(dot_streak),
                    "wickets_in_over_before": str(wickets_in_over[(inning, over)]),
                    "is_wicket": bool_str(event_info["wickets"] > 0),
                    "is_six": bool_str(event_info["bat_runs"] == 6),
                    "is_four": bool_str(event_info["bat_runs"] == 4),
                    "is_boundary": bool_str(event_info["bat_runs"] in (4, 6)),
                    "is_dot": bool_str(
                        event_info["total_runs"] == 0 and event_info["wickets"] == 0
                    ),
                    "is_extra": bool_str(
                        (
                            event_info["wides"]
                            + event_info["noballs"]
                            + event_info["byes"]
                            + event_info["legbyes"]
                        )
                        > 0
                    ),
                    "runs_scored": str(event_info["total_runs"]),
                    "bat_runs": str(event_info["bat_runs"]),
                    "wides": str(event_info["wides"]),
                    "noballs": str(event_info["noballs"]),
                    "byes": str(event_info["byes"]),
                    "legbyes": str(event_info["legbyes"]),
                }
            )

            state_before["runs"] += int(event_info["total_runs"])
            state_before["wkts"] += int(event_info["wickets"])
            if bool(event_info["legal"]):
                state_before["legal"] += 1
            if int(event_info["wickets"]) > 0:
                wickets_in_over[(inning, over)] += int(event_info["wickets"])

            if inning == 1 and index + 1 < len(match_rows):
                next_inning = int(match_rows[index + 1]["inning"])
                if next_inning == 2 and innings1_total is None:
                    innings1_total = state_before["runs"]

            boundary_streak = (
                boundary_streak + 1 if int(event_info["bat_runs"]) in (4, 6) else 0
            )
            six_streak = six_streak + 1 if int(event_info["bat_runs"]) == 6 else 0
            dot_streak = (
                dot_streak + 1
                if int(event_info["total_runs"]) == 0
                and int(event_info["wickets"]) == 0
                else 0
            )
            previous_two_event, previous_event = previous_event, row["event"]

    append_forward_drifts(feature_rows)
    return feature_rows


def append_forward_drifts(rows: list[dict[str, str]]) -> None:
    by_match: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_match[(row["season"], row["source_match_id"])].append(row)

    for group in by_match.values():
        group.sort(key=lambda row: (int(row["inning"]), float(row["ball"])))
        for index, row in enumerate(group):
            same_inning = [
                candidate
                for candidate in group[index + 1 :]
                if candidate["inning"] == row["inning"]
            ]
            next1 = same_inning[0] if len(same_inning) >= 1 else None
            next3 = same_inning[2] if len(same_inning) >= 3 else None
            next6 = same_inning[5] if len(same_inning) >= 6 else None

            after = parse_optional_float(row["batting_after_pct"])
            row["next1_drift_pct"] = fmt(
                compute_drift(after, next1, "batting_after_pct")
            )
            row["next3_drift_pct"] = fmt(
                compute_drift(after, next3, "batting_after_pct")
            )
            row["next6_drift_pct"] = fmt(
                compute_drift(after, next6, "batting_after_pct")
            )
            anomaly = (
                parse_optional_float(row["batting_delta_pct"]) is not None
                and abs(parse_optional_float(row["batting_delta_pct"]) or 0) >= 7
                and row["is_wicket"] == "false"
                and row["phase"] != "death_2nd"
            )
            tradable = (
                row["pricing_source_before"] == "trade"
                and row["pricing_source_after"] == "trade"
                and parse_optional_float(row["batting_before_pct"]) is not None
                and 5 < (parse_optional_float(row["batting_before_pct"]) or 0) < 95
                and not anomaly
            )
            row["anomaly_flag"] = bool_str(anomaly)
            row["tradable_trade_row"] = bool_str(tradable)


def compute_drift(
    after: float | None, next_row: dict[str, str] | None, key: str
) -> float | None:
    if after is None or next_row is None:
        return None
    next_after = parse_optional_float(next_row[key])
    if next_after is None:
        return None
    return next_after - after


def classify_phase(over: int, inning: int) -> str:
    base = "powerplay" if over < 6 else "middle" if over < 15 else "death"
    return f"{base}_{'2nd' if inning == 2 else '1st'}"


def parse_optional_float(value: str) -> float | None:
    return None if value == "" else float(value)


def fmt(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def bool_str(value: bool) -> str:
    return "true" if value else "false"


def write_csv(path: Path, rows: Iterable[dict[str, str]]) -> None:
    rows = list(rows)
    if not rows:
        raise RuntimeError(f"No rows to write for {path}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
