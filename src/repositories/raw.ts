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

export interface RawPolymarketPriceHistoryInsert {
  sourceEventId: string;
  sourceMarketId: string;
  eventSlug: string;
  marketSlug: string;
  conditionId: string;
  marketType: string | null;
  tokenId: string;
  outcomeName: string;
  outcomeIndex: 0 | 1;
  pointTime: string;
  price: number;
  queryStartTime: string | null;
  queryEndTime: string | null;
  fidelityMinutes: number | null;
  payload: JsonObject;
}

export interface RawPolymarketTradeInsert {
  tradeKey: string;
  sourceEventId: string;
  sourceMarketId: string;
  eventSlug: string;
  marketSlug: string;
  conditionId: string;
  marketType: string | null;
  tokenId: string;
  outcomeName: string;
  outcomeIndex: 0 | 1;
  tradeTime: string;
  price: number;
  size: number;
  side: "BUY" | "SELL";
  transactionHash: string | null;
  proxyWallet: string | null;
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

export interface RawPolymarketPriceHistoryRecord extends RawPolymarketPriceHistoryInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
}

export interface RawPolymarketTradeRecord extends RawPolymarketTradeInsert {
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
  savePolymarketPriceHistoryPoint(
    point: RawPolymarketPriceHistoryInsert,
  ): Promise<RawPolymarketPriceHistoryRecord>;
  savePolymarketTrade(
    trade: RawPolymarketTradeInsert,
  ): Promise<RawPolymarketTradeRecord>;
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

    async savePolymarketPriceHistoryPoint(
      point: RawPolymarketPriceHistoryInsert,
    ): Promise<RawPolymarketPriceHistoryRecord> {
      const result = await executor.query<RawPolymarketPriceHistoryRow>(
        `
          insert into raw_polymarket_price_history (
            competition,
            source_event_id,
            source_market_id,
            event_slug,
            market_slug,
            condition_id,
            market_type,
            token_id,
            outcome_name,
            outcome_index,
            point_time,
            price,
            query_start_time,
            query_end_time,
            fidelity_minutes,
            payload
          ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
          )
          on conflict (token_id, point_time) do update set
            source_event_id = excluded.source_event_id,
            source_market_id = excluded.source_market_id,
            event_slug = excluded.event_slug,
            market_slug = excluded.market_slug,
            condition_id = excluded.condition_id,
            market_type = excluded.market_type,
            outcome_name = excluded.outcome_name,
            outcome_index = excluded.outcome_index,
            price = excluded.price,
            query_start_time = excluded.query_start_time,
            query_end_time = excluded.query_end_time,
            fidelity_minutes = excluded.fidelity_minutes,
            payload = excluded.payload
          returning
            id,
            competition,
            source_event_id,
            source_market_id,
            event_slug,
            market_slug,
            condition_id,
            market_type,
            token_id,
            outcome_name,
            outcome_index,
            point_time,
            price,
            query_start_time,
            query_end_time,
            fidelity_minutes,
            payload,
            created_at
        `,
        [
          "IPL",
          point.sourceEventId,
          point.sourceMarketId,
          point.eventSlug,
          point.marketSlug,
          point.conditionId,
          point.marketType,
          point.tokenId,
          point.outcomeName,
          point.outcomeIndex,
          point.pointTime,
          point.price,
          point.queryStartTime,
          point.queryEndTime,
          point.fidelityMinutes,
          point.payload,
        ],
      );

      return mapRawPolymarketPriceHistoryRow(
        result.rows[0] as RawPolymarketPriceHistoryRow,
      );
    },

    async savePolymarketTrade(
      trade: RawPolymarketTradeInsert,
    ): Promise<RawPolymarketTradeRecord> {
      const result = await executor.query<RawPolymarketTradeRow>(
        `
          insert into raw_polymarket_trades (
            competition,
            trade_key,
            source_event_id,
            source_market_id,
            event_slug,
            market_slug,
            condition_id,
            market_type,
            token_id,
            outcome_name,
            outcome_index,
            trade_time,
            price,
            size,
            side,
            transaction_hash,
            proxy_wallet,
            payload
          ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
          )
          on conflict (trade_key) do update set
            source_event_id = excluded.source_event_id,
            source_market_id = excluded.source_market_id,
            event_slug = excluded.event_slug,
            market_slug = excluded.market_slug,
            condition_id = excluded.condition_id,
            market_type = excluded.market_type,
            token_id = excluded.token_id,
            outcome_name = excluded.outcome_name,
            outcome_index = excluded.outcome_index,
            trade_time = excluded.trade_time,
            price = excluded.price,
            size = excluded.size,
            side = excluded.side,
            transaction_hash = excluded.transaction_hash,
            proxy_wallet = excluded.proxy_wallet,
            payload = excluded.payload
          returning
            id,
            competition,
            trade_key,
            source_event_id,
            source_market_id,
            event_slug,
            market_slug,
            condition_id,
            market_type,
            token_id,
            outcome_name,
            outcome_index,
            trade_time,
            price,
            size,
            side,
            transaction_hash,
            proxy_wallet,
            payload,
            created_at
        `,
        [
          "IPL",
          trade.tradeKey,
          trade.sourceEventId,
          trade.sourceMarketId,
          trade.eventSlug,
          trade.marketSlug,
          trade.conditionId,
          trade.marketType,
          trade.tokenId,
          trade.outcomeName,
          trade.outcomeIndex,
          trade.tradeTime,
          trade.price,
          trade.size,
          trade.side,
          trade.transactionHash,
          trade.proxyWallet,
          trade.payload,
        ],
      );

      return mapRawPolymarketTradeRow(result.rows[0] as RawPolymarketTradeRow);
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

interface RawPolymarketPriceHistoryRow {
  id: string | number;
  competition: "IPL";
  source_event_id: string;
  source_market_id: string;
  event_slug: string;
  market_slug: string;
  condition_id: string;
  market_type: string | null;
  token_id: string;
  outcome_name: string;
  outcome_index: number;
  point_time: Date;
  price: string | number;
  query_start_time: Date | null;
  query_end_time: Date | null;
  fidelity_minutes: number | null;
  payload: JsonObject;
  created_at: Date;
}

interface RawPolymarketTradeRow {
  id: string | number;
  competition: "IPL";
  trade_key: string;
  source_event_id: string;
  source_market_id: string;
  event_slug: string;
  market_slug: string;
  condition_id: string;
  market_type: string | null;
  token_id: string;
  outcome_name: string;
  outcome_index: number;
  trade_time: Date;
  price: string | number;
  size: string | number;
  side: "BUY" | "SELL";
  transaction_hash: string | null;
  proxy_wallet: string | null;
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

function mapRawPolymarketPriceHistoryRow(
  row: RawPolymarketPriceHistoryRow,
): RawPolymarketPriceHistoryRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    sourceEventId: row.source_event_id,
    sourceMarketId: row.source_market_id,
    eventSlug: row.event_slug,
    marketSlug: row.market_slug,
    conditionId: row.condition_id,
    marketType: row.market_type,
    tokenId: row.token_id,
    outcomeName: row.outcome_name,
    outcomeIndex: row.outcome_index === 0 ? 0 : 1,
    pointTime: row.point_time.toISOString(),
    price: Number(row.price),
    queryStartTime:
      row.query_start_time === null ? null : row.query_start_time.toISOString(),
    queryEndTime:
      row.query_end_time === null ? null : row.query_end_time.toISOString(),
    fidelityMinutes: row.fidelity_minutes,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}

function mapRawPolymarketTradeRow(
  row: RawPolymarketTradeRow,
): RawPolymarketTradeRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    tradeKey: row.trade_key,
    sourceEventId: row.source_event_id,
    sourceMarketId: row.source_market_id,
    eventSlug: row.event_slug,
    marketSlug: row.market_slug,
    conditionId: row.condition_id,
    marketType: row.market_type,
    tokenId: row.token_id,
    outcomeName: row.outcome_name,
    outcomeIndex: row.outcome_index === 0 ? 0 : 1,
    tradeTime: row.trade_time.toISOString(),
    price: Number(row.price),
    size: Number(row.size),
    side: row.side,
    transactionHash: row.transaction_hash,
    proxyWallet: row.proxy_wallet,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}
