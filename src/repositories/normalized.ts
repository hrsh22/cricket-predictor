import type {
  CanonicalCheckpoint,
  CanonicalMatch,
  CheckpointState,
  CheckpointType,
  FeatureRow,
} from "../domain/checkpoint.js";
import type { JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";

export interface CanonicalMatchRecord extends CanonicalMatch {
  id: number;
  createdAt: string;
  updatedAt: string;
}

export interface PlayerRegistryRecord {
  id: number;
  cricsheetPlayerId: string;
  canonicalName: string;
  battingStyle: string | null;
  bowlingStyle: string | null;
  bowlingTypeGroup: string | null;
  playerRole: string | null;
  metadata: JsonObject;
  createdAt: string;
  updatedAt: string;
}

export interface MatchPlayerAppearanceRecord {
  id: number;
  canonicalMatchId: number;
  teamName: string;
  playerRegistryId: number | null;
  sourcePlayerName: string;
  lineupOrder: number;
  isPlayingXi: boolean;
  metadata: JsonObject;
  createdAt: string;
}

export type CheckpointStateRecord = CheckpointState & {
  id: number;
  canonicalMatchId: number;
  createdAt: string;
};

export interface FeatureRowRecord extends FeatureRow {
  id: number;
  checkpointStateId: number;
}

export interface NormalizedRepository {
  saveCanonicalMatch(match: CanonicalMatch): Promise<CanonicalMatchRecord>;
  savePlayerRegistry(player: {
    cricsheetPlayerId: string;
    canonicalName: string;
    battingStyle?: string | null;
    bowlingStyle?: string | null;
    bowlingTypeGroup?: string | null;
    playerRole?: string | null;
    metadata?: JsonObject;
  }): Promise<PlayerRegistryRecord>;
  saveMatchPlayerAppearance(input: {
    canonicalMatchId: number;
    teamName: string;
    playerRegistryId: number | null;
    sourcePlayerName: string;
    lineupOrder: number;
    isPlayingXi?: boolean;
    metadata?: JsonObject;
  }): Promise<MatchPlayerAppearanceRecord>;
  saveCheckpoint(
    checkpoint: CanonicalCheckpoint,
  ): Promise<CheckpointStateRecord>;
  saveFeatureRow(
    checkpointStateId: number,
    featureRow: FeatureRow,
  ): Promise<FeatureRowRecord>;
}

export interface CheckpointStateRow {
  id: string | number;
  canonical_match_id: string | number;
  match_slug?: string;
  checkpoint_type: CheckpointType;
  snapshot_time: Date;
  state_version: number;
  source_market_snapshot_id: string | number | null;
  source_cricket_snapshot_id: string | number | null;
  innings_number: number | null;
  batting_team_name: string | null;
  bowling_team_name: string | null;
  runs: number | null;
  wickets: number | null;
  overs: string | number | null;
  target_runs: number | null;
  current_run_rate: string | number | null;
  required_run_rate: string | number | null;
  state_payload: JsonObject;
  created_at: Date;
}

export interface CanonicalMatchRow {
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
  created_at: Date;
  updated_at: Date;
}

export interface FeatureRowRow {
  id: string | number;
  checkpoint_state_id: string | number;
  match_slug: string;
  checkpoint_type: CheckpointType;
  feature_set_version: string;
  generated_at: Date;
  features: JsonObject;
}

interface PlayerRegistryRow {
  id: string | number;
  cricsheet_player_id: string;
  canonical_name: string;
  batting_style: string | null;
  bowling_style: string | null;
  bowling_type_group: string | null;
  player_role: string | null;
  metadata: JsonObject;
  created_at: Date;
  updated_at: Date;
}

interface MatchPlayerAppearanceRow {
  id: string | number;
  canonical_match_id: string | number;
  team_name: string;
  player_registry_id: string | number | null;
  source_player_name: string;
  lineup_order: number;
  is_playing_xi: boolean;
  metadata: JsonObject;
  created_at: Date;
}

export function createNormalizedRepository(
  executor: SqlExecutor,
): NormalizedRepository {
  return {
    async saveCanonicalMatch(
      match: CanonicalMatch,
    ): Promise<CanonicalMatchRecord> {
      const result = await executor.query<CanonicalMatchRow>(
        `
          insert into canonical_matches (
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
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          on conflict (match_slug) do update set
            source_match_id = excluded.source_match_id,
            season = excluded.season,
            scheduled_start = excluded.scheduled_start,
            team_a_name = excluded.team_a_name,
            team_b_name = excluded.team_b_name,
            venue_name = excluded.venue_name,
            status = excluded.status,
            toss_winner_team_name = excluded.toss_winner_team_name,
            toss_decision = excluded.toss_decision,
            winning_team_name = excluded.winning_team_name,
            result_type = excluded.result_type,
            updated_at = now()
          returning
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
            result_type,
            created_at,
            updated_at
        `,
        [
          match.competition,
          match.matchSlug,
          match.sourceMatchId,
          match.season,
          match.scheduledStart,
          match.teamAName,
          match.teamBName,
          match.venueName,
          match.status,
          match.tossWinnerTeamName,
          match.tossDecision,
          match.winningTeamName,
          match.resultType,
        ],
      );

      return mapCanonicalMatchRow(result.rows[0] as CanonicalMatchRow);
    },

    async savePlayerRegistry(player): Promise<PlayerRegistryRecord> {
      const result = await executor.query<PlayerRegistryRow>(
        `
          insert into player_registry (
            cricsheet_player_id,
            canonical_name,
            batting_style,
            bowling_style,
            bowling_type_group,
            player_role,
            metadata
          ) values ($1, $2, $3, $4, $5, $6, $7)
          on conflict (cricsheet_player_id) do update set
            canonical_name = excluded.canonical_name,
            batting_style = coalesce(excluded.batting_style, player_registry.batting_style),
            bowling_style = coalesce(excluded.bowling_style, player_registry.bowling_style),
            bowling_type_group = coalesce(excluded.bowling_type_group, player_registry.bowling_type_group),
            player_role = coalesce(excluded.player_role, player_registry.player_role),
            metadata = excluded.metadata,
            updated_at = now()
          returning
            id,
            cricsheet_player_id,
            canonical_name,
            batting_style,
            bowling_style,
            bowling_type_group,
            player_role,
            metadata,
            created_at,
            updated_at
        `,
        [
          player.cricsheetPlayerId,
          player.canonicalName,
          player.battingStyle ?? null,
          player.bowlingStyle ?? null,
          player.bowlingTypeGroup ?? null,
          player.playerRole ?? null,
          player.metadata ?? {},
        ],
      );

      return mapPlayerRegistryRow(result.rows[0] as PlayerRegistryRow);
    },

    async saveMatchPlayerAppearance(
      input,
    ): Promise<MatchPlayerAppearanceRecord> {
      const result = await executor.query<MatchPlayerAppearanceRow>(
        `
          insert into match_player_appearances (
            canonical_match_id,
            team_name,
            player_registry_id,
            source_player_name,
            lineup_order,
            is_playing_xi,
            metadata
          ) values ($1, $2, $3, $4, $5, $6, $7)
          on conflict (canonical_match_id, team_name, lineup_order) do update set
            player_registry_id = excluded.player_registry_id,
            source_player_name = excluded.source_player_name,
            is_playing_xi = excluded.is_playing_xi,
            metadata = excluded.metadata
          returning
            id,
            canonical_match_id,
            team_name,
            player_registry_id,
            source_player_name,
            lineup_order,
            is_playing_xi,
            metadata,
            created_at
        `,
        [
          input.canonicalMatchId,
          input.teamName,
          input.playerRegistryId,
          input.sourcePlayerName,
          input.lineupOrder,
          input.isPlayingXi ?? true,
          input.metadata ?? {},
        ],
      );

      return mapMatchPlayerAppearanceRow(
        result.rows[0] as MatchPlayerAppearanceRow,
      );
    },

    async saveCheckpoint(
      checkpoint: CanonicalCheckpoint,
    ): Promise<CheckpointStateRecord> {
      const matchRecord = await this.saveCanonicalMatch(checkpoint.match);

      const result = await executor.query<CheckpointStateRow>(
        `
          insert into checkpoint_states (
            canonical_match_id,
            checkpoint_type,
            snapshot_time,
            state_version,
            source_market_snapshot_id,
            source_cricket_snapshot_id,
            innings_number,
            batting_team_name,
            bowling_team_name,
            runs,
            wickets,
            overs,
            target_runs,
            current_run_rate,
            required_run_rate,
            state_payload
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          on conflict (canonical_match_id, checkpoint_type, snapshot_time, state_version) do update set
            source_market_snapshot_id = excluded.source_market_snapshot_id,
            source_cricket_snapshot_id = excluded.source_cricket_snapshot_id,
            innings_number = excluded.innings_number,
            batting_team_name = excluded.batting_team_name,
            bowling_team_name = excluded.bowling_team_name,
            runs = excluded.runs,
            wickets = excluded.wickets,
            overs = excluded.overs,
            target_runs = excluded.target_runs,
            current_run_rate = excluded.current_run_rate,
            required_run_rate = excluded.required_run_rate,
            state_payload = excluded.state_payload
          returning
            id,
            canonical_match_id,
            checkpoint_type,
            snapshot_time,
            state_version,
            source_market_snapshot_id,
            source_cricket_snapshot_id,
            innings_number,
            batting_team_name,
            bowling_team_name,
            runs,
            wickets,
            overs,
            target_runs,
            current_run_rate,
            required_run_rate,
            state_payload,
            created_at
        `,
        [
          matchRecord.id,
          checkpoint.checkpointType,
          checkpoint.state.snapshotTime,
          checkpoint.state.stateVersion,
          checkpoint.state.sourceMarketSnapshotId,
          checkpoint.state.sourceCricketSnapshotId,
          checkpoint.state.inningsNumber,
          checkpoint.state.battingTeamName,
          checkpoint.state.bowlingTeamName,
          checkpoint.state.runs,
          checkpoint.state.wickets,
          checkpoint.state.overs,
          checkpoint.state.targetRuns,
          checkpoint.state.currentRunRate,
          checkpoint.state.requiredRunRate,
          checkpoint.state.statePayload,
        ],
      );

      return mapCheckpointStateRow({
        ...(result.rows[0] as CheckpointStateRow),
        match_slug: checkpoint.state.matchSlug,
      });
    },

    async saveFeatureRow(
      checkpointStateId: number,
      featureRow: FeatureRow,
    ): Promise<FeatureRowRecord> {
      const result = await executor.query<FeatureRowRow>(
        `
          insert into match_features (
            checkpoint_state_id,
            feature_set_version,
            generated_at,
            features
          ) values ($1, $2, $3, $4)
          on conflict (checkpoint_state_id, feature_set_version) do update set
            generated_at = excluded.generated_at,
            features = excluded.features
          returning
            id,
            checkpoint_state_id,
            feature_set_version,
            generated_at,
            features
        `,
        [
          checkpointStateId,
          featureRow.featureSetVersion,
          featureRow.generatedAt,
          featureRow.features,
        ],
      );

      return mapFeatureRowRow(result.rows[0] as FeatureRowRow);
    },
  };
}

export function mapCanonicalMatchRow(
  row: CanonicalMatchRow,
): CanonicalMatchRecord {
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
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

export function mapPlayerRegistryRow(
  row: PlayerRegistryRow,
): PlayerRegistryRecord {
  return {
    id: Number(row.id),
    cricsheetPlayerId: row.cricsheet_player_id,
    canonicalName: row.canonical_name,
    battingStyle: row.batting_style,
    bowlingStyle: row.bowling_style,
    bowlingTypeGroup: row.bowling_type_group,
    playerRole: row.player_role,
    metadata: row.metadata,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

export function mapMatchPlayerAppearanceRow(
  row: MatchPlayerAppearanceRow,
): MatchPlayerAppearanceRecord {
  return {
    id: Number(row.id),
    canonicalMatchId: Number(row.canonical_match_id),
    teamName: row.team_name,
    playerRegistryId:
      row.player_registry_id === null ? null : Number(row.player_registry_id),
    sourcePlayerName: row.source_player_name,
    lineupOrder: row.lineup_order,
    isPlayingXi: row.is_playing_xi,
    metadata: row.metadata,
    createdAt: row.created_at.toISOString(),
  };
}

export function mapCheckpointStateRow(
  row: CheckpointStateRow,
): CheckpointStateRecord {
  const base = {
    id: Number(row.id),
    canonicalMatchId: Number(row.canonical_match_id),
    checkpointType: row.checkpoint_type,
    matchSlug: row.match_slug ?? "",
    snapshotTime: row.snapshot_time.toISOString(),
    stateVersion: row.state_version,
    sourceMarketSnapshotId:
      row.source_market_snapshot_id === null
        ? null
        : Number(row.source_market_snapshot_id),
    sourceCricketSnapshotId:
      row.source_cricket_snapshot_id === null
        ? null
        : Number(row.source_cricket_snapshot_id),
    statePayload: row.state_payload,
    createdAt: row.created_at.toISOString(),
  };

  if (row.checkpoint_type === "innings_break") {
    return {
      ...base,
      checkpointType: "innings_break",
      inningsNumber: row.innings_number as 1 | 2,
      battingTeamName: row.batting_team_name as string,
      bowlingTeamName: row.bowling_team_name as string,
      runs: row.runs as number,
      wickets: row.wickets as number,
      overs: row.overs as number,
      targetRuns: row.target_runs as number,
      currentRunRate: row.current_run_rate as number,
      requiredRunRate: row.required_run_rate as number,
    };
  }

  return {
    ...base,
    checkpointType: row.checkpoint_type,
    inningsNumber: null,
    battingTeamName: null,
    bowlingTeamName: null,
    runs: null,
    wickets: null,
    overs: null,
    targetRuns: null,
    currentRunRate: null,
    requiredRunRate: null,
  };
}

export function mapFeatureRowRow(row: FeatureRowRow): FeatureRowRecord {
  return {
    id: Number(row.id),
    checkpointStateId: Number(row.checkpoint_state_id),
    matchSlug: row.match_slug,
    checkpointType: row.checkpoint_type,
    featureSetVersion: row.feature_set_version,
    generatedAt: row.generated_at.toISOString(),
    features: row.features,
  };
}
