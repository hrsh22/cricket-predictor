from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests


ROOT = Path(__file__).resolve().parents[2]
ANALYSIS_DIR = ROOT / "data" / "analysis"


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "Usage: python3 scripts/report/export-goldsky-wallet-season.py <season_csv> <output_folder> <output_xlsx>"
        )

    season_csv = Path(sys.argv[1])
    output_folder = Path(sys.argv[2])
    output_xlsx = Path(sys.argv[3])
    wallet_summary_dir = output_folder.parent / f"{output_folder.name}-wallet-summaries"

    matches = load_matches(season_csv)
    goldsky = load_goldsky_module()

    output_folder.mkdir(parents=True, exist_ok=True)
    wallet_summary_dir.mkdir(parents=True, exist_ok=True)

    exported = []
    for match in matches:
        event = goldsky.fetch_event(match["event_slug"])
        market = goldsky.pick_market(event, "moneyline")
        fills = fetch_goldsky_fills_timeboxed(event, market["token_ids"])
        normalized = goldsky.normalize_fills(fills, market["token_map"])
        wallet_rows, wallet_side_rows, summary = goldsky.summarize_wallets(normalized)
        segment_by_wallet = {row["wallet"]: row["segment"] for row in wallet_rows}

        annotated_fills = []
        for fill in normalized:
            annotated_fills.append(
                {
                    "match_date": match["match_date"],
                    "source_match_id": match["source_match_id"],
                    "match_slug": match["match_slug"],
                    "event_slug": match["event_slug"],
                    **fill,
                    "maker_segment": segment_by_wallet.get(fill["maker"], "unknown"),
                    "taker_segment": segment_by_wallet.get(fill["taker"], "unknown"),
                }
            )

        trade_csv = (
            output_folder
            / f"{match['match_date']}_{match['source_match_id']}_{slugify(match['team_a'])}_vs_{slugify(match['team_b'])}.csv"
        )
        wallet_csv = (
            wallet_summary_dir
            / f"{match['match_date']}_{match['source_match_id']}_{slugify(match['team_a'])}_vs_{slugify(match['team_b'])}.csv"
        )

        write_csv(trade_csv, annotated_fills)
        write_csv(wallet_csv, wallet_rows)

        exported.append(
            {
                "source_match_id": match["source_match_id"],
                "event_slug": match["event_slug"],
                "fill_rows": len(annotated_fills),
                "wallet_rows": len(wallet_rows),
                "unique_txs": summary["unique_transaction_hashes"],
            }
        )

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "report" / "pack-match-csvs-xlsx.py"),
            str(output_folder),
            str(output_xlsx),
        ],
        cwd=ROOT,
        check=True,
    )

    summary_path = output_folder.parent / f"{output_folder.name}-summary.json"
    summary_path.write_text(json.dumps(exported, indent=2) + "\n", encoding="utf-8")

    print(output_folder)
    print(wallet_summary_dir)
    print(output_xlsx)
    print(summary_path)


def load_matches(path: Path) -> list[dict[str, str]]:
    rows = list(csv.DictReader(path.open("r", encoding="utf-8", newline="")))
    unique = {}
    for row in rows:
        if not row.get("event_slug"):
            continue
        unique[row["source_match_id"]] = {
            "source_match_id": row["source_match_id"],
            "match_date": row["match_date"],
            "match_slug": row["match_slug"],
            "event_slug": row["event_slug"],
            "team_a": row["batting_team"],
            "team_b": row["fielding_team"],
        }
    return sorted(
        unique.values(), key=lambda row: (row["match_date"], row["source_match_id"])
    )


def load_goldsky_module():
    module_path = ROOT / "scripts" / "report" / "export-goldsky-wallet-report.py"
    spec = importlib.util.spec_from_file_location("goldsky_wallet_report", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load Goldsky wallet report module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fetch_goldsky_fills_timeboxed(event: dict, token_ids: list[str]) -> list[dict]:
    start_ts = int(parse_iso_timestamp(event["startDate"]).timestamp()) - 24 * 60 * 60
    end_ts = int(parse_iso_timestamp(event["endDate"]).timestamp()) + 24 * 60 * 60
    chunk_seconds = 6 * 60 * 60
    endpoint = (
        "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
        "subgraphs/orderbook-subgraph/0.0.1/gn"
    )
    deduped: dict[str, dict] = {}

    chunk_start = start_ts
    while chunk_start <= end_ts:
        chunk_end = min(chunk_start + chunk_seconds - 1, end_ts)
        last_ts: str | None = None

        for _page in range(100):
            if last_ts is None:
                where = (
                    "{ or:[ "
                    "{ makerAssetId_in:$tokens, timestamp_gte:$start, timestamp_lte:$end }, "
                    "{ takerAssetId_in:$tokens, timestamp_gte:$start, timestamp_lte:$end } ] }"
                )
            else:
                where = (
                    "{ or:[ "
                    "{ makerAssetId_in:$tokens, timestamp_gt:$lastTs, timestamp_lte:$end }, "
                    "{ takerAssetId_in:$tokens, timestamp_gt:$lastTs, timestamp_lte:$end } ] }"
                )
            query = {
                "query": f"""
                query($tokens:[String!],$start:String!,$end:String!,$lastTs:String!) {{
                  orderFilledEvents(
                    first:1000,
                    orderBy:timestamp,
                    orderDirection:asc,
                    where:{where}
                  ) {{
                    id
                    maker
                    taker
                    makerAssetId
                    takerAssetId
                    makerAmountFilled
                    takerAmountFilled
                    timestamp
                    transactionHash
                  }}
                }}
                """,
                "variables": {
                    "tokens": token_ids,
                    "start": str(chunk_start),
                    "end": str(chunk_end),
                    "lastTs": last_ts or "0",
                },
            }
            response = requests.post(endpoint, json=query, timeout=60)
            response.raise_for_status()
            payload = response.json()
            if payload.get("errors"):
                raise RuntimeError(json.dumps(payload["errors"], indent=2))
            batch = payload["data"]["orderFilledEvents"]
            if not batch:
                break
            for row in batch:
                deduped[row["id"]] = row
            last_ts = batch[-1]["timestamp"]
            if len(batch) < 1000:
                break
            time.sleep(0.05)

        chunk_start = chunk_end + 1

    return sorted(deduped.values(), key=lambda row: (int(row["timestamp"]), row["id"]))


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write for {path}")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def slugify(value: str) -> str:
    text = value.lower()
    chars = [character if character.isalnum() else "-" for character in text]
    slug = "".join(chars)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "match"


def parse_iso_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


if __name__ == "__main__":
    main()
