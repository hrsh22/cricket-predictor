import type { CanonicalMatch } from "../domain/match.js";
import type { MarketSnapshot } from "../domain/market.js";
import type { JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";

export type MappingStatus = "resolved" | "ambiguous" | "unresolved";

export interface ResolverCandidate {
  canonicalMatchId: number;
  matchSlug: string;
  confidence: number;
}

export interface MarketMatchMappingInsert {
  sourceMarketId: string;
  sourceMarketSnapshotId: number;
  canonicalMatchId: number | null;
  mappingStatus: MappingStatus;
  confidence: number | null;
  resolverVersion: string;
  reason: string;
  payload: JsonObject;
}

export interface MarketMatchMappingRecord extends MarketMatchMappingInsert {
  id: number;
  competition: "IPL";
  matchSlug: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface MatchResolutionMarketSnapshot extends MarketSnapshot {
  id: number;
}

export interface MatchResolutionCandidate extends CanonicalMatch {
  id: number;
}

export interface MatchingRepository {
  listLatestMarketSnapshots(): Promise<MatchResolutionMarketSnapshot[]>;
  listCanonicalMatchesForWindow(input: {
    from: string;
    to: string;
  }): Promise<MatchResolutionCandidate[]>;
  saveMarketMatchMapping(
    mapping: MarketMatchMappingInsert,
  ): Promise<MarketMatchMappingRecord>;
  listScorerEligibleMappings(input?: {
    minimumConfidence?: number;
  }): Promise<MarketMatchMappingRecord[]>;
}

interface LatestMarketSnapshotRow {
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
}

interface MatchResolutionCandidateRow {
  id: string | number;
  competition: "IPL";
  match_slug: string;
  source_match_id: string | null;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  status: CanonicalMatch["status"];
  toss_winner_team_name: string | null;
  toss_decision: CanonicalMatch["tossDecision"];
  winning_team_name: string | null;
  result_type: CanonicalMatch["resultType"];
}

interface MarketMatchMappingRow {
  id: string | number;
  competition: "IPL";
  source_market_id: string;
  source_market_snapshot_id: string | number;
  canonical_match_id: string | number | null;
  match_slug: string | null;
  mapping_status: MappingStatus;
  confidence: string | number | null;
  resolver_version: string;
  reason: string;
  payload: JsonObject;
  created_at: Date;
  updated_at: Date;
}

export function createMatchingRepository(
  executor: SqlExecutor,
): MatchingRepository {
  return {
    async listLatestMarketSnapshots(): Promise<
      MatchResolutionMarketSnapshot[]
    > {
      const result = await executor.query<LatestMarketSnapshotRow>(`
        select distinct on (source_market_id)
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
          payload
        from raw_market_snapshots
        where competition = 'IPL'
        order by source_market_id asc, snapshot_time desc, id desc
      `);

      return result.rows.map((row) =>
        mapLatestMarketSnapshotRow(row as LatestMarketSnapshotRow),
      );
    },

    async listCanonicalMatchesForWindow(input: {
      from: string;
      to: string;
    }): Promise<MatchResolutionCandidate[]> {
      const result = await executor.query<MatchResolutionCandidateRow>(
        `
          select
            id,
            competition,
            match_slug,
            source_match_id,
            season,
            scheduled_start,
            team_a_name,
            team_b_name,
            venue_name,
            status,
            toss_winner_team_name,
            toss_decision,
            winning_team_name,
            result_type
          from canonical_matches
          where competition = 'IPL'
            and scheduled_start between $1::timestamptz and $2::timestamptz
            and status in ('scheduled', 'in_progress')
          order by scheduled_start asc, id asc
        `,
        [input.from, input.to],
      );

      return result.rows.map((row) =>
        mapMatchResolutionCandidateRow(row as MatchResolutionCandidateRow),
      );
    },

    async saveMarketMatchMapping(
      mapping: MarketMatchMappingInsert,
    ): Promise<MarketMatchMappingRecord> {
      const result = await executor.query<MarketMatchMappingRow>(
        `
          with upserted as (
            insert into market_match_mappings (
              competition,
              source_market_id,
              source_market_snapshot_id,
              canonical_match_id,
              mapping_status,
              confidence,
              resolver_version,
              reason,
              payload,
              updated_at
            ) values ('IPL', $1, $2, $3, $4, $5, $6, $7, $8, now())
            on conflict (source_market_id) do update set
              source_market_snapshot_id = excluded.source_market_snapshot_id,
              canonical_match_id = excluded.canonical_match_id,
              mapping_status = excluded.mapping_status,
              confidence = excluded.confidence,
              resolver_version = excluded.resolver_version,
              reason = excluded.reason,
              payload = excluded.payload,
              updated_at = now()
            returning
              id,
              competition,
              source_market_id,
              source_market_snapshot_id,
              canonical_match_id,
              mapping_status,
              confidence,
              resolver_version,
              reason,
              payload,
              created_at,
              updated_at
          )
          select
            upserted.id,
            upserted.competition,
            upserted.source_market_id,
            upserted.source_market_snapshot_id,
            upserted.canonical_match_id,
            cm.match_slug,
            upserted.mapping_status,
            upserted.confidence,
            upserted.resolver_version,
            upserted.reason,
            upserted.payload,
            upserted.created_at,
            upserted.updated_at
          from upserted
          left join canonical_matches cm on cm.id = upserted.canonical_match_id
        `,
        [
          mapping.sourceMarketId,
          mapping.sourceMarketSnapshotId,
          mapping.canonicalMatchId,
          mapping.mappingStatus,
          mapping.confidence,
          mapping.resolverVersion,
          mapping.reason,
          mapping.payload,
        ],
      );

      return mapMarketMatchMappingRow(result.rows[0] as MarketMatchMappingRow);
    },

    async listScorerEligibleMappings(input?: {
      minimumConfidence?: number;
    }): Promise<MarketMatchMappingRecord[]> {
      const minimumConfidence = input?.minimumConfidence ?? 0.85;
      const result = await executor.query<MarketMatchMappingRow>(
        `
          select
            m.id,
            m.competition,
            m.source_market_id,
            m.source_market_snapshot_id,
            m.canonical_match_id,
            cm.match_slug,
            m.mapping_status,
            m.confidence,
            m.resolver_version,
            m.reason,
            m.payload,
            m.created_at,
            m.updated_at
          from market_match_mappings m
          left join canonical_matches cm on cm.id = m.canonical_match_id
          where m.mapping_status = 'resolved'
            and m.competition = 'IPL'
            and m.canonical_match_id is not null
            and m.confidence is not null
            and m.confidence >= $1
          order by m.confidence desc, m.updated_at desc, m.id desc
        `,
        [minimumConfidence],
      );

      return result.rows.map((row) =>
        mapMarketMatchMappingRow(row as MarketMatchMappingRow),
      );
    },
  };
}

function mapLatestMarketSnapshotRow(
  row: LatestMarketSnapshotRow,
): MatchResolutionMarketSnapshot {
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
  };
}

function mapMatchResolutionCandidateRow(
  row: MatchResolutionCandidateRow,
): MatchResolutionCandidate {
  return {
    id: Number(row.id),
    competition: row.competition,
    matchSlug: row.match_slug,
    sourceMatchId: row.source_match_id,
    season: row.season,
    scheduledStart: row.scheduled_start.toISOString(),
    teamAName: row.team_a_name,
    teamBName: row.team_b_name,
    venueName: row.venue_name,
    status: row.status,
    tossWinnerTeamName: row.toss_winner_team_name,
    tossDecision: row.toss_decision,
    winningTeamName: row.winning_team_name,
    resultType: row.result_type,
  };
}

function mapMarketMatchMappingRow(
  row: MarketMatchMappingRow,
): MarketMatchMappingRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    sourceMarketId: row.source_market_id,
    sourceMarketSnapshotId: Number(row.source_market_snapshot_id),
    canonicalMatchId:
      row.canonical_match_id === null ? null : Number(row.canonical_match_id),
    matchSlug: row.match_slug ?? null,
    mappingStatus: row.mapping_status,
    confidence: row.confidence === null ? null : Number(row.confidence),
    resolverVersion: row.resolver_version,
    reason: row.reason,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}
