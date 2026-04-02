import type { MarketSnapshot } from "../domain/market.js";
import type { JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";

export interface RawCricketSnapshotInsert {
  provider: string;
  sourceMatchId: string;
  snapshotTime: string;
  matchStatus: string | null;
  inningsNumber: number | null;
  overNumber: number | null;
  payload: JsonObject;
}

export interface RawMarketSnapshotRecord extends MarketSnapshot {
  id: number;
  createdAt: string;
}

export interface RawCricketSnapshotRecord extends RawCricketSnapshotInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
}

export interface RawSnapshotRepository {
  saveMarketSnapshot(
    snapshot: MarketSnapshot,
  ): Promise<RawMarketSnapshotRecord>;
  saveCricketSnapshot(
    snapshot: RawCricketSnapshotInsert,
  ): Promise<RawCricketSnapshotRecord>;
}

export function createRawSnapshotRepository(
  executor: SqlExecutor,
): RawSnapshotRepository {
  return {
    async saveMarketSnapshot(
      snapshot: MarketSnapshot,
    ): Promise<RawMarketSnapshotRecord> {
      const result = await executor.query<RawMarketSnapshotRow>(
        `
          insert into raw_market_snapshots (
            competition,
            source_market_id,
            market_slug,
            event_slug,
            snapshot_time,
            market_status,
            yes_outcome_name,
            no_outcome_name,
            outcome_probabilities,
            last_traded_price,
            liquidity,
            payload
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
          on conflict (source_market_id, snapshot_time) do update set
            market_slug = excluded.market_slug,
            event_slug = excluded.event_slug,
            market_status = excluded.market_status,
            yes_outcome_name = excluded.yes_outcome_name,
            no_outcome_name = excluded.no_outcome_name,
            outcome_probabilities = excluded.outcome_probabilities,
            last_traded_price = excluded.last_traded_price,
            liquidity = excluded.liquidity,
            payload = excluded.payload
          returning
            id,
            competition,
            source_market_id,
            market_slug,
            event_slug,
            snapshot_time,
            market_status,
            yes_outcome_name,
            no_outcome_name,
            outcome_probabilities,
            last_traded_price,
            liquidity,
            payload,
            created_at
        `,
        [
          snapshot.competition,
          snapshot.sourceMarketId,
          snapshot.marketSlug,
          snapshot.eventSlug,
          snapshot.snapshotTime,
          snapshot.marketStatus,
          snapshot.yesOutcomeName,
          snapshot.noOutcomeName,
          snapshot.outcomeProbabilities,
          snapshot.lastTradedPrice,
          snapshot.liquidity,
          snapshot.payload,
        ],
      );

      return mapRawMarketSnapshotRow(result.rows[0] as RawMarketSnapshotRow);
    },

    async saveCricketSnapshot(
      snapshot: RawCricketSnapshotInsert,
    ): Promise<RawCricketSnapshotRecord> {
      const result = await executor.query<RawCricketSnapshotRow>(
        `
          insert into raw_cricket_snapshots (
            competition,
            provider,
            source_match_id,
            snapshot_time,
            match_status,
            innings_number,
            over_number,
            payload
          ) values ($1, $2, $3, $4, $5, $6, $7, $8)
          on conflict (source_match_id, snapshot_time) do update set
            provider = excluded.provider,
            match_status = excluded.match_status,
            innings_number = excluded.innings_number,
            over_number = excluded.over_number,
            payload = excluded.payload
          returning
            id,
            competition,
            provider,
            source_match_id,
            snapshot_time,
            match_status,
            innings_number,
            over_number,
            payload,
            created_at
        `,
        [
          "IPL",
          snapshot.provider,
          snapshot.sourceMatchId,
          snapshot.snapshotTime,
          snapshot.matchStatus,
          snapshot.inningsNumber,
          snapshot.overNumber,
          snapshot.payload,
        ],
      );

      return mapRawCricketSnapshotRow(result.rows[0] as RawCricketSnapshotRow);
    },
  };
}

interface RawMarketSnapshotRow {
  id: string | number;
  competition: "IPL";
  source_market_id: string;
  market_slug: string;
  event_slug: string | null;
  snapshot_time: Date;
  market_status: string | null;
  yes_outcome_name: string | null;
  no_outcome_name: string | null;
  outcome_probabilities: Record<string, number>;
  last_traded_price: string | number | null;
  liquidity: string | number | null;
  payload: JsonObject;
  created_at: Date;
}

interface RawCricketSnapshotRow {
  id: string | number;
  competition: "IPL";
  provider: string;
  source_match_id: string;
  snapshot_time: Date;
  match_status: string | null;
  innings_number: number | null;
  over_number: string | number | null;
  payload: JsonObject;
  created_at: Date;
}

function mapRawMarketSnapshotRow(
  row: RawMarketSnapshotRow,
): RawMarketSnapshotRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    sourceMarketId: row.source_market_id,
    marketSlug: row.market_slug,
    eventSlug: row.event_slug,
    snapshotTime: row.snapshot_time.toISOString(),
    marketStatus: row.market_status,
    yesOutcomeName: row.yes_outcome_name,
    noOutcomeName: row.no_outcome_name,
    outcomeProbabilities: row.outcome_probabilities,
    lastTradedPrice:
      row.last_traded_price === null ? null : Number(row.last_traded_price),
    liquidity: row.liquidity === null ? null : Number(row.liquidity),
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}

function mapRawCricketSnapshotRow(
  row: RawCricketSnapshotRow,
): RawCricketSnapshotRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    provider: row.provider,
    sourceMatchId: row.source_match_id,
    snapshotTime: row.snapshot_time.toISOString(),
    matchStatus: row.match_status,
    inningsNumber: row.innings_number,
    overNumber: row.over_number === null ? null : Number(row.over_number),
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}
