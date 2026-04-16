from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests


GAMMA_URL = "https://gamma-api.polymarket.com/events?slug={slug}"
GOLDSKY_ENDPOINT = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)


def main() -> None:
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "Usage: python3 scripts/report/export-goldsky-wallet-report.py <event_slug> <output_dir> [market_type]"
        )

    event_slug = sys.argv[1]
    output_dir = Path(sys.argv[2])
    market_type = sys.argv[3] if len(sys.argv) == 4 else "moneyline"

    event = fetch_event(event_slug)
    market = pick_market(event, market_type)
    fills = fetch_goldsky_fills(market["token_ids"])
    normalized = normalize_fills(fills, market["token_map"])

    output_dir.mkdir(parents=True, exist_ok=True)
    fill_csv = output_dir / f"{event_slug}-{market_type}-goldsky-fills.csv"
    wallet_csv = output_dir / f"{event_slug}-{market_type}-wallet-summary.csv"
    wallet_side_csv = (
        output_dir / f"{event_slug}-{market_type}-wallet-outcome-sides.csv"
    )
    summary_json = output_dir / f"{event_slug}-{market_type}-wallet-summary.json"
    report_md = output_dir / f"{event_slug}-{market_type}-wallet-report.md"

    write_csv(fill_csv, normalized)

    wallet_rows, wallet_side_rows, summary = summarize_wallets(normalized)
    write_csv(wallet_csv, wallet_rows)
    write_csv(wallet_side_csv, wallet_side_rows)
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    report_md.write_text(build_report(summary, event, market_type), encoding="utf-8")

    print(fill_csv)
    print(wallet_csv)
    print(wallet_side_csv)
    print(summary_json)
    print(report_md)


def fetch_event(event_slug: str) -> dict:
    response = requests.get(
        GAMMA_URL.format(slug=event_slug),
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        raise RuntimeError(f"No Polymarket event found for slug {event_slug}")
    return payload[0]


def pick_market(event: dict, market_type: str) -> dict:
    market = next(
        (
            entry
            for entry in event.get("markets", [])
            if entry.get("sportsMarketType") == market_type
        ),
        None,
    )
    if market is None:
        raise RuntimeError(
            f"No market of type {market_type!r} found for event {event.get('slug')}"
        )
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
        "id": market["id"],
        "slug": market["slug"],
        "question": market["question"],
        "condition_id": market["conditionId"],
        "token_ids": token_ids,
        "token_map": dict(zip(token_ids, outcomes)),
    }


def fetch_goldsky_fills(token_ids: list[str]) -> list[dict]:
    all_rows: list[dict] = []
    last_id: str | None = None
    for _page in range(100):
        if last_id is None:
            where = (
                "{ or:[ { makerAssetId_in:$tokens }, { takerAssetId_in:$tokens } ] }"
            )
        else:
            where = (
                "{ or:[ { id_gt:$lastId, makerAssetId_in:$tokens }, "
                "{ id_gt:$lastId, takerAssetId_in:$tokens } ] }"
            )
        query = {
            "query": f"""
            query($tokens:[String!],$lastId:String!) {{
              orderFilledEvents(
                first:1000,
                orderBy:id,
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
            "variables": {"tokens": token_ids, "lastId": last_id or ""},
        }
        response = requests.post(GOLDSKY_ENDPOINT, json=query, timeout=60)
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(json.dumps(payload["errors"], indent=2))
        batch = payload["data"]["orderFilledEvents"]
        if not batch:
            break
        all_rows.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < 1000:
            break
        time.sleep(0.05)
    return all_rows


def normalize_fills(
    rows: list[dict], token_map: dict[str, str]
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        maker_asset = row["makerAssetId"]
        taker_asset = row["takerAssetId"]
        maker_amount = float(row["makerAmountFilled"])
        taker_amount = float(row["takerAmountFilled"])
        token_id = None
        token_amount = None
        usdc_amount = None
        outcome = None
        if maker_asset in token_map and taker_asset == "0":
            token_id = maker_asset
            token_amount = maker_amount
            usdc_amount = taker_amount
            outcome = token_map[maker_asset]
        elif taker_asset in token_map and maker_asset == "0":
            token_id = taker_asset
            token_amount = taker_amount
            usdc_amount = maker_amount
            outcome = token_map[taker_asset]
        elif maker_asset in token_map:
            token_id = maker_asset
            token_amount = maker_amount
            usdc_amount = taker_amount
            outcome = token_map[maker_asset]
        elif taker_asset in token_map:
            token_id = taker_asset
            token_amount = taker_amount
            usdc_amount = maker_amount
            outcome = token_map[taker_asset]
        else:
            continue
        if not token_amount:
            continue
        price = usdc_amount / token_amount
        normalized.append(
            {
                "fill_id": row["id"],
                "transaction_hash": row["transactionHash"],
                "timestamp": row["timestamp"],
                "maker": row["maker"],
                "taker": row["taker"],
                "token_id": token_id,
                "outcome": outcome,
                "contracts": round(token_amount, 6),
                "usdc_notional": round(usdc_amount, 6),
                "price": round(price, 6),
            }
        )
    return normalized


def summarize_wallets(
    normalized: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict]:
    wallets: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "fill_count": 0,
            "maker_fills": 0,
            "taker_fills": 0,
            "usdc_notional": 0.0,
            "contracts": 0.0,
            "outcomes": set(),
            "times": [],
        }
    )
    wallet_outcome_side: dict[tuple[str, str, str], dict[str, object]] = defaultdict(
        lambda: {"fill_count": 0, "usdc_notional": 0.0, "contracts": 0.0}
    )

    for fill in normalized:
        for wallet_key, role in ((fill["maker"], "maker"), (fill["taker"], "taker")):
            wallet = str(wallet_key)
            entry = wallets[wallet]
            entry["fill_count"] = int(entry["fill_count"]) + 1
            entry["maker_fills"] = int(entry["maker_fills"]) + (
                1 if role == "maker" else 0
            )
            entry["taker_fills"] = int(entry["taker_fills"]) + (
                1 if role == "taker" else 0
            )
            entry["usdc_notional"] = float(entry["usdc_notional"]) + float(
                fill["usdc_notional"]
            )
            entry["contracts"] = float(entry["contracts"]) + float(fill["contracts"])
            cast_outcomes = entry["outcomes"]
            assert isinstance(cast_outcomes, set)
            cast_outcomes.add(str(fill["outcome"]))
            cast_times = entry["times"]
            assert isinstance(cast_times, list)
            cast_times.append(int(fill["timestamp"]))

            side_key = (wallet, str(fill["outcome"]), role)
            side_entry = wallet_outcome_side[side_key]
            side_entry["fill_count"] = int(side_entry["fill_count"]) + 1
            side_entry["usdc_notional"] = float(side_entry["usdc_notional"]) + float(
                fill["usdc_notional"]
            )
            side_entry["contracts"] = float(side_entry["contracts"]) + float(
                fill["contracts"]
            )

    total_wallet_notional = sum(
        float(entry["usdc_notional"]) for entry in wallets.values()
    )
    wallet_rows: list[dict[str, object]] = []
    for wallet, entry in wallets.items():
        fill_count = int(entry["fill_count"])
        maker_fills = int(entry["maker_fills"])
        taker_fills = int(entry["taker_fills"])
        maker_ratio = maker_fills / fill_count if fill_count else 0.0
        outcomes = entry["outcomes"]
        times = entry["times"]
        assert isinstance(outcomes, set)
        assert isinstance(times, list)
        active_minutes = (max(times) - min(times)) / 60 if times else 0.0
        if maker_ratio >= 0.6 and len(outcomes) == 2 and fill_count >= 20:
            segment = "likely_market_maker"
        elif maker_ratio <= 0.1 and len(outcomes) == 2 and fill_count >= 100:
            segment = "likely_aggressive_taker_or_router"
        elif maker_ratio <= 0.25 and (len(outcomes) == 1 or fill_count <= 5):
            segment = "likely_directional_or_retail"
        else:
            segment = "mixed_or_unclear"
        wallet_rows.append(
            {
                "wallet": wallet,
                "fill_count": fill_count,
                "maker_fills": maker_fills,
                "taker_fills": taker_fills,
                "maker_ratio": round(maker_ratio, 3),
                "outcome_count": len(outcomes),
                "usdc_notional": round(float(entry["usdc_notional"]), 6),
                "share_wallet_notional_pct": round(
                    (float(entry["usdc_notional"]) / total_wallet_notional * 100)
                    if total_wallet_notional
                    else 0,
                    3,
                ),
                "contracts": round(float(entry["contracts"]), 6),
                "active_minutes": round(active_minutes, 1),
                "segment": segment,
            }
        )
    wallet_rows.sort(key=lambda row: float(row["usdc_notional"]), reverse=True)

    side_rows: list[dict[str, object]] = []
    for (wallet, outcome, role), entry in wallet_outcome_side.items():
        side_rows.append(
            {
                "wallet": wallet,
                "outcome": outcome,
                "role": role,
                "fill_count": int(entry["fill_count"]),
                "usdc_notional": round(float(entry["usdc_notional"]), 6),
                "contracts": round(float(entry["contracts"]), 6),
            }
        )
    side_rows.sort(key=lambda row: float(row["usdc_notional"]), reverse=True)

    summary = {
        "trade_source": "goldsky_orderfilled",
        "fill_row_count": len(normalized),
        "unique_transaction_hashes": len(
            {fill["transaction_hash"] for fill in normalized}
        ),
        "distinct_wallets": len(wallet_rows),
        "total_wallet_side_usdc_notional": round(total_wallet_notional, 6),
        "top_wallets": wallet_rows[:10],
        "segment_counts": dict(Counter(str(row["segment"]) for row in wallet_rows)),
        "note": "Wallet notional is maker+taker side activity. Use for concentration and segmentation, not exact market turnover.",
    }

    return wallet_rows, side_rows, summary


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write for {path}")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_report(summary: dict, event: dict, market_type: str) -> str:
    lines = [
        "# Goldsky Wallet Report\n\n",
        "## Market\n",
        f"- Event slug: {event['slug']}\n",
        f"- Event title: {event['title']}\n",
        f"- Market type: {market_type}\n",
        f"- Fill rows fetched: **{summary['fill_row_count']}**\n",
        f"- Unique transaction hashes: **{summary['unique_transaction_hashes']}**\n",
        f"- Distinct maker/taker wallets: **{summary['distinct_wallets']}**\n",
        f"- Wallet-side USDC notional: **{summary['total_wallet_side_usdc_notional']}**\n\n",
        "## Top wallets by wallet-side notional\n",
    ]
    for row in summary["top_wallets"]:
        lines.append(
            f"- `{row['wallet']}` — notional **{row['usdc_notional']}**, share **{row['share_wallet_notional_pct']}%**, fills **{row['fill_count']}**, maker_ratio **{row['maker_ratio']}**, segment **{row['segment']}**\n"
        )
    lines.extend(
        [
            "\n## Segmentation note\n",
            "- `likely_market_maker`: high maker ratio, both outcomes, sustained fill count.\n",
            "- `likely_aggressive_taker_or_router`: very high taker dominance with large repeated flow.\n",
            "- `likely_directional_or_retail`: lower-count, narrower participation.\n",
            "- `mixed_or_unclear`: does not strongly fit one bucket.\n",
        ]
    )
    return "".join(lines)


if __name__ == "__main__":
    main()
