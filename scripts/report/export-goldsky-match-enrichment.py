from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests


GOLDSKY_ENDPOINT = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)


@dataclass
class MatchRow:
    inning: int
    ball: str
    batting_team: str
    event: str
    timestamp: str
    current_before: float | None
    current_after: float | None
    current_before_source: str
    current_after_source: str
    primary_team: str
    secondary_team: str


@dataclass
class GoldskyTrade:
    timestamp: int
    token_id: str
    outcome_name: str
    price: float
    transaction_hash: str


def main() -> None:
    if len(sys.argv) != 6:
        raise SystemExit(
            "Usage: python3 scripts/report/export-goldsky-match-enrichment.py "
            "<season_csv> <event_slug> <match_id> <output_csv> <output_json>"
        )

    season_csv = Path(sys.argv[1])
    event_slug = sys.argv[2]
    match_id = sys.argv[3]
    output_csv = Path(sys.argv[4])
    output_json = Path(sys.argv[5])

    match_rows = load_match_rows(season_csv, match_id)
    if not match_rows:
        raise SystemExit(f"No rows found for match id {match_id} in {season_csv}")

    metadata = fetch_event_metadata(event_slug)
    goldsky_trades = fetch_goldsky_trades(metadata)
    enriched_rows = enrich_rows(match_rows, metadata, goldsky_trades)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    write_csv(output_csv, enriched_rows)
    write_json(output_json, summarize(enriched_rows, goldsky_trades, metadata))

    print(output_csv)
    print(output_json)


def load_match_rows(path: Path, match_id: str) -> list[MatchRow]:
    rows: list[MatchRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["source_match_id"] != match_id:
                continue
            batting_is_primary = row["primary_team"] == row["batting_team"]
            current_before = parse_float(
                row["primary_before_pct"]
                if batting_is_primary
                else row["secondary_before_pct"]
            )
            current_after = parse_float(
                row["primary_after_pct"]
                if batting_is_primary
                else row["secondary_after_pct"]
            )
            rows.append(
                MatchRow(
                    inning=int(row["inning"]),
                    ball=row["ball"],
                    batting_team=row["batting_team"],
                    event=row["event"],
                    timestamp=row["timestamp"],
                    current_before=current_before,
                    current_after=current_after,
                    current_before_source=row["pricing_source_before"],
                    current_after_source=row["pricing_source_after"],
                    primary_team=row["primary_team"],
                    secondary_team=row["secondary_team"],
                )
            )
    rows.sort(key=lambda row: (row.inning, float(row.ball)))
    return rows


def fetch_event_metadata(event_slug: str) -> dict:
    response = requests.get(
        f"https://gamma-api.polymarket.com/events?slug={event_slug}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        raise RuntimeError(f"No event found for slug {event_slug}")
    event = payload[0]
    market = event["markets"][0]
    token_ids = (
        json.loads(market["clobTokenIds"])
        if isinstance(market["clobTokenIds"], str)
        else market["clobTokenIds"]
    )
    outcomes = (
        json.loads(market["outcomes"])
        if isinstance(market["outcomes"], str)
        else market["outcomes"]
    )
    return {
        "event_slug": event_slug,
        "start_ts": int(
            event["startDateTimestamp"] if "startDateTimestamp" in event else 0
        )
        if isinstance(event.get("startDateTimestamp"), (int, str))
        else int(parse_datetime(event["startDate"]) // 1000),
        "end_ts": int(parse_datetime(event["endDate"]) // 1000),
        "market_id": market["id"],
        "condition_id": market["conditionId"],
        "token_map": dict(zip(token_ids, outcomes)),
    }


def fetch_goldsky_trades(metadata: dict) -> list[GoldskyTrade]:
    token_ids = list(metadata["token_map"].keys())
    start_ts = metadata["start_ts"] - 3600
    end_ts = metadata["end_ts"] + 12 * 3600
    query = {
        "query": """
        query($tokens: [String!], $start: String!, $end: String!) {
          orderFilledEvents(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where: {
              or: [
                { makerAssetId_in: $tokens, timestamp_gte: $start, timestamp_lte: $end },
                { takerAssetId_in: $tokens, timestamp_gte: $start, timestamp_lte: $end }
              ]
            }
          ) {
            id
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            timestamp
            transactionHash
          }
        }
        """,
        "variables": {
            "tokens": token_ids,
            "start": str(start_ts),
            "end": str(end_ts),
        },
    }
    response = requests.post(GOLDSKY_ENDPOINT, json=query, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(json.dumps(payload["errors"], indent=2))

    aggregated: dict[tuple[str, str, int], dict[str, float | str | int]] = {}
    for row in payload["data"]["orderFilledEvents"]:
        token_id = None
        if row["makerAssetId"] in metadata["token_map"] and row["takerAssetId"] == "0":
            token_id = row["makerAssetId"]
            token_amount = float(row["makerAmountFilled"])
            usdc_amount = float(row["takerAmountFilled"])
        elif (
            row["takerAssetId"] in metadata["token_map"] and row["makerAssetId"] == "0"
        ):
            token_id = row["takerAssetId"]
            token_amount = float(row["takerAmountFilled"])
            usdc_amount = float(row["makerAmountFilled"])
        if token_id is None or token_amount <= 0:
            continue
        key = (row["transactionHash"], token_id, int(row["timestamp"]))
        record = aggregated.setdefault(
            key,
            {
                "transaction_hash": row["transactionHash"],
                "token_id": token_id,
                "timestamp": int(row["timestamp"]),
                "token_amount": 0.0,
                "usdc_amount": 0.0,
            },
        )
        record["token_amount"] = float(record["token_amount"]) + token_amount
        record["usdc_amount"] = float(record["usdc_amount"]) + usdc_amount

    trades: list[GoldskyTrade] = []
    for record in aggregated.values():
        token_amount = float(record["token_amount"])
        usdc_amount = float(record["usdc_amount"])
        price = usdc_amount / token_amount
        trades.append(
            GoldskyTrade(
                timestamp=int(record["timestamp"]),
                token_id=str(record["token_id"]),
                outcome_name=metadata["token_map"][str(record["token_id"])],
                price=round(price, 6),
                transaction_hash=str(record["transaction_hash"]),
            )
        )
    trades.sort(
        key=lambda trade: (trade.timestamp, trade.transaction_hash, trade.token_id)
    )
    return trades


def enrich_rows(
    match_rows: list[MatchRow], metadata: dict, trades: list[GoldskyTrade]
) -> list[dict[str, str]]:
    outcome_to_team = map_outcomes_to_teams(metadata, match_rows[0])
    unified = []
    for trade in trades:
        outcome_team = outcome_to_team.get(trade.outcome_name, trade.outcome_name)
        if outcome_team == match_rows[0].primary_team:
            primary_price = trade.price
        elif outcome_team == match_rows[0].secondary_team:
            primary_price = 1 - trade.price
        else:
            continue
        unified.append(
            {
                "timestamp": trade.timestamp,
                "primary_price_pct": round(primary_price * 100, 3),
                "transaction_hash": trade.transaction_hash,
                "outcome_name": trade.outcome_name,
            }
        )
    unified.sort(key=lambda row: (row["timestamp"], row["transaction_hash"]))

    enriched: list[dict[str, str]] = []
    for index, row in enumerate(match_rows):
        ball_ts = parse_datetime(row.timestamp) // 1000
        next_ts = (
            parse_datetime(match_rows[index + 1].timestamp) // 1000
            if index + 1 < len(match_rows)
            and match_rows[index + 1].inning == row.inning
            else ball_ts + 120
        )

        before_trade = latest_trade_before(unified, ball_ts)
        after_trade = earliest_trade_in_window(unified, ball_ts, next_ts)
        improved_before = (
            before_trade["primary_price_pct"]
            if before_trade is not None
            else row.current_before
        )
        improved_after = (
            after_trade["primary_price_pct"]
            if after_trade is not None
            else row.current_after
        )

        enriched.append(
            {
                "inning": str(row.inning),
                "ball": row.ball,
                "event": row.event,
                "batting_team": row.batting_team,
                "current_before_pct": fmt(row.current_before),
                "current_after_pct": fmt(row.current_after),
                "current_before_source": row.current_before_source,
                "current_after_source": row.current_after_source,
                "goldsky_trade_before_pct": fmt(
                    before_trade["primary_price_pct"] if before_trade else None
                ),
                "goldsky_trade_after_pct": fmt(
                    after_trade["primary_price_pct"] if after_trade else None
                ),
                "improved_before_pct": fmt(improved_before),
                "improved_after_pct": fmt(improved_after),
                "before_improved": str(
                    row.current_before_source != "trade" and before_trade is not None
                ).lower(),
                "after_improved": str(
                    row.current_after_source != "trade" and after_trade is not None
                ).lower(),
                "goldsky_before_tx": before_trade["transaction_hash"]
                if before_trade
                else "",
                "goldsky_after_tx": after_trade["transaction_hash"]
                if after_trade
                else "",
                "timestamp": row.timestamp,
            }
        )
    return enriched


def map_outcomes_to_teams(metadata: dict, row: MatchRow) -> dict[str, str]:
    result = {}
    for outcome in metadata["token_map"].values():
        lowered = outcome.strip().lower()
        if lowered in row.primary_team.lower() or row.primary_team.lower().endswith(
            lowered
        ):
            result[outcome] = row.primary_team
        elif (
            lowered in row.secondary_team.lower()
            or row.secondary_team.lower().endswith(lowered)
        ):
            result[outcome] = row.secondary_team
        else:
            result[outcome] = outcome
    return result


def latest_trade_before(trades: list[dict], ball_ts: int):
    candidate = None
    for trade in trades:
        if trade["timestamp"] >= ball_ts:
            break
        if trade["timestamp"] >= ball_ts - 300:
            candidate = trade
    return candidate


def earliest_trade_in_window(trades: list[dict], start_ts: int, end_ts: int):
    for trade in trades:
        if trade["timestamp"] <= start_ts:
            continue
        if trade["timestamp"] > end_ts:
            break
        return trade
    return None


def summarize(
    enriched_rows: list[dict[str, str]], trades: list[GoldskyTrade], metadata: dict
) -> dict:
    before_improved = sum(
        1 for row in enriched_rows if row["before_improved"] == "true"
    )
    after_improved = sum(1 for row in enriched_rows if row["after_improved"] == "true")
    return {
        "event_slug": metadata["event_slug"],
        "market_id": metadata["market_id"],
        "condition_id": metadata["condition_id"],
        "goldsky_trade_rows": len(trades),
        "goldsky_unique_transactions": len(
            {trade.transaction_hash for trade in trades}
        ),
        "before_rows_improved": before_improved,
        "after_rows_improved": after_improved,
        "rows_with_any_improvement": sum(
            1
            for row in enriched_rows
            if row["before_improved"] == "true" or row["after_improved"] == "true"
        ),
    }


def write_csv(path: Path, rows: Iterable[dict[str, str]]) -> None:
    rows = list(rows)
    if not rows:
        raise RuntimeError("No enriched rows to write")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_float(value: str) -> float | None:
    if value == "":
        return None
    return float(value)


def parse_datetime(value: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def fmt(value: float | None) -> str:
    return (
        ""
        if value is None
        else (
            str(int(value))
            if value.is_integer()
            else f"{value:.3f}".rstrip("0").rstrip(".")
        )
    )


if __name__ == "__main__":
    main()
