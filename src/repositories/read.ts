import type { CheckpointType } from "../domain/checkpoint.js";
import type { CanonicalMatch } from "../domain/match.js";
import type { JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";
import type {
  CanonicalMatchRow,
  FeatureRowRow,
  CheckpointStateRow,
} from "./normalized.js";
import {
  mapCanonicalMatchRow,
  mapCheckpointStateRow,
  mapFeatureRowRow,
} from "./normalized.js";

export interface MatchReadModel {
  match: CanonicalMatch;
  checkpointStates: ReturnType<typeof mapCheckpointStateRow>[];
  featureRows: ReturnType<typeof mapFeatureRowRow>[];
}

export interface ReadModelRepository {
  getMatchReadModel(matchSlug: string): Promise<MatchReadModel | null>;
}

export function createReadModelRepository(
  executor: SqlExecutor,
): ReadModelRepository {
  return {
    async getMatchReadModel(matchSlug: string): Promise<MatchReadModel | null> {
      const matchResult = await executor.query<CanonicalMatchRow>(
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
            result_type,
            created_at,
            updated_at
          from canonical_matches
          where match_slug = $1
        `,
        [matchSlug],
      );

      const matchRow = matchResult.rows[0] as CanonicalMatchRow | undefined;
      if (matchRow === undefined) {
        return null;
      }

      const checkpointResult = await executor.query<CheckpointStateRow>(
        `
          select
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
          from checkpoint_states
          where canonical_match_id = $1
          order by snapshot_time asc, state_version asc
        `,
        [matchRow.id],
      );

      const featureResult = await executor.query<FeatureRowRow>(
        `
          select
            mf.id,
            mf.checkpoint_state_id,
            cm.match_slug,
            cs.checkpoint_type,
            mf.feature_set_version,
            mf.generated_at,
            mf.features
          from match_features mf
          join checkpoint_states cs on cs.id = mf.checkpoint_state_id
          join canonical_matches cm on cm.id = cs.canonical_match_id
          where cm.id = $1
          order by mf.generated_at asc, mf.id asc
        `,
        [matchRow.id],
      );

      return {
        match: mapCanonicalMatchRow(matchRow),
        checkpointStates: checkpointResult.rows.map((row) =>
          mapCheckpointStateRow({
            ...(row as CheckpointStateRow),
            match_slug: matchRow.match_slug,
          }),
        ),
        featureRows: featureResult.rows.map((row) =>
          mapFeatureRowRow(row as FeatureRowRow),
        ),
      };
    },
  };
}

export type { CheckpointType };
export type { JsonObject };
