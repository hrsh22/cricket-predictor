import type { CheckpointType } from "../domain/checkpoint.js";
import { isRecord, type JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "../repositories/postgres.js";
import type {
  HistoricalPredictionRow,
  HistoricalPredictionSkippedRow,
  HistoricalBacktestOptions,
  StoredScoreBacktestDataset,
} from "./types.js";
import { normalizeProbability } from "./metrics.js";

interface StoredScoreRow {
  model_score_id: string | number;
  model_key: string;
  checkpoint_type: CheckpointType;
  match_slug: string;
  season: number;
  match_status: string;
  result_type: string | null;
  winning_team_name: string | null;
  team_a_name: string;
  team_b_name: string;
  snapshot_time: Date;
  fair_win_probability: string | number;
  market_implied_probability: string | number | null;
  score_payload: JsonObject;
  yes_outcome_name: string | null;
  no_outcome_name: string | null;
}

export async function loadStoredScoreBacktestDataset(
  executor: SqlExecutor,
  options: Pick<
    HistoricalBacktestOptions,
    | "modelKey"
    | "checkpointType"
    | "evaluationSeasonFrom"
    | "evaluationSeasonTo"
  >,
): Promise<StoredScoreBacktestDataset> {
  const result = await executor.query<StoredScoreRow>(
    `
      with ranked_scores as (
        select
          ms.id as model_score_id,
          mr.model_key,
          cs.checkpoint_type,
          cm.match_slug,
          cm.season,
          cm.status as match_status,
          cm.result_type,
          cm.winning_team_name,
          cm.team_a_name,
          cm.team_b_name,
          cs.snapshot_time,
          ms.fair_win_probability,
          ms.market_implied_probability,
          ms.score_payload,
          rms.yes_outcome_name,
          rms.no_outcome_name,
          row_number() over (
            partition by ms.canonical_match_id, mr.model_key, cs.checkpoint_type
            order by ms.scored_at desc, ms.id desc
          ) as score_rank
        from model_scores ms
        join model_registry mr on mr.id = ms.model_registry_id
        join checkpoint_states cs on cs.id = ms.checkpoint_state_id
        join canonical_matches cm on cm.id = ms.canonical_match_id
        left join raw_market_snapshots rms on rms.id = cs.source_market_snapshot_id
        where mr.model_key = $1
          and cs.checkpoint_type = $2
          and cm.season <= $3
      )
      select
        model_score_id,
        model_key,
        checkpoint_type,
        match_slug,
        season,
        match_status,
        result_type,
        winning_team_name,
        team_a_name,
        team_b_name,
        snapshot_time,
        fair_win_probability,
        market_implied_probability,
        score_payload,
        yes_outcome_name,
        no_outcome_name
      from ranked_scores
      where score_rank = 1
      order by snapshot_time asc, model_score_id asc
    `,
    [options.modelKey, options.checkpointType, options.evaluationSeasonTo],
  );

  const rows: HistoricalPredictionRow[] = [];
  const skippedRows: HistoricalPredictionSkippedRow[] = [];

  for (const entry of result.rows) {
    const normalized = normalizeStoredScoreRow(entry);
    if (normalized.status === "ready") {
      rows.push(normalized.row);
      continue;
    }

    skippedRows.push(normalized.skippedRow);
  }

  return {
    rows,
    skippedRows,
    totalLoadedRows: result.rows.length,
  };
}

function normalizeStoredScoreRow(
  row: StoredScoreRow,
):
  | { status: "ready"; row: HistoricalPredictionRow }
  | { status: "skipped"; skippedRow: HistoricalPredictionSkippedRow } {
  const modelScoreId = Number(row.model_score_id);
  const snapshotTime = row.snapshot_time.toISOString();

  const baseSkippedRow = {
    modelScoreId,
    matchSlug: row.match_slug,
    season: row.season,
    checkpointType: row.checkpoint_type,
    snapshotTime,
  };

  if (row.match_status !== "completed") {
    return {
      status: "skipped",
      skippedRow: {
        ...baseSkippedRow,
        reason: "match_not_completed",
        detail: "Historical backtests only evaluate completed matches.",
      },
    };
  }

  if (row.result_type !== "win") {
    return {
      status: "skipped",
      skippedRow: {
        ...baseSkippedRow,
        reason: "unsupported_match_result",
        detail:
          "Historical backtests only evaluate binary win results; ties and non-results are excluded.",
      },
    };
  }

  if (
    typeof row.winning_team_name !== "string" ||
    row.winning_team_name.length === 0
  ) {
    return {
      status: "skipped",
      skippedRow: {
        ...baseSkippedRow,
        reason: "winner_missing",
        detail: "Completed win results require winning_team_name.",
      },
    };
  }

  const primaryProbability = normalizeProbability(
    Number(row.fair_win_probability),
  );
  const marketImpliedProbability =
    row.market_implied_probability === null
      ? null
      : normalizeProbability(Number(row.market_implied_probability));

  if (row.checkpoint_type === "pre_match") {
    return normalizePreMatchStoredScoreRow({
      row,
      modelScoreId,
      snapshotTime,
      primaryProbability,
      marketImpliedProbability,
      baseSkippedRow,
    });
  }

  return normalizeTeamABinaryStoredScoreRow({
    row,
    modelScoreId,
    snapshotTime,
    primaryProbability,
    marketImpliedProbability,
    baseSkippedRow,
  });
}

function normalizePreMatchStoredScoreRow(input: {
  row: StoredScoreRow;
  modelScoreId: number;
  snapshotTime: string;
  primaryProbability: number;
  marketImpliedProbability: number | null;
  baseSkippedRow: Omit<HistoricalPredictionSkippedRow, "reason" | "detail">;
}):
  | { status: "ready"; row: HistoricalPredictionRow }
  | { status: "skipped"; skippedRow: HistoricalPredictionSkippedRow } {
  const yesOutcomeName = input.row.yes_outcome_name;
  const noOutcomeName = input.row.no_outcome_name;

  if (
    typeof yesOutcomeName !== "string" ||
    yesOutcomeName.length === 0 ||
    typeof noOutcomeName !== "string" ||
    noOutcomeName.length === 0
  ) {
    return {
      status: "skipped",
      skippedRow: {
        ...input.baseSkippedRow,
        reason: "missing_market_outcomes",
        detail:
          "Pre-match backtests require persisted yes/no market outcomes to recover the binary class label.",
      },
    };
  }

  const winner = input.row.winning_team_name as string;
  const actualOutcome = matchesTeam(winner, yesOutcomeName)
    ? 1
    : matchesTeam(winner, noOutcomeName)
      ? 0
      : null;

  if (actualOutcome === null) {
    return {
      status: "skipped",
      skippedRow: {
        ...input.baseSkippedRow,
        reason: "winner_not_in_binary_outcomes",
        detail:
          "Winning team did not match either persisted market outcome, so the historical label is ambiguous.",
      },
    };
  }

  const structuredProbability = readNumericPath(input.row.score_payload, [
    "valuation",
    "structuredFairProbability",
  ]);

  if (structuredProbability === null) {
    return {
      status: "skipped",
      skippedRow: {
        ...input.baseSkippedRow,
        reason: "invalid_probability_payload",
        detail:
          "Pre-match score_payload must include valuation.structuredFairProbability for social-on/off comparison.",
      },
    };
  }

  return {
    status: "ready",
    row: {
      modelKey: input.row.model_key,
      checkpointType: "pre_match",
      modelScoreId: input.modelScoreId,
      matchSlug: input.row.match_slug,
      season: input.row.season,
      snapshotTime: input.snapshotTime,
      actualOutcome,
      positiveClassLabel: yesOutcomeName,
      negativeClassLabel: noOutcomeName,
      primaryProbability: input.primaryProbability,
      socialOnProbability: input.primaryProbability,
      socialOffProbability: structuredProbability,
      socialSupported: true,
      marketImpliedProbability: input.marketImpliedProbability,
      provenance: {
        source: "model_scores",
        modelScoreId: input.modelScoreId,
        positiveClass: "yes_outcome",
      },
    },
  };
}

function matchesTeam(left: string, right: string): boolean {
  return normalizeTeamName(left) === normalizeTeamName(right);
}

function normalizeTeamName(value: string): string {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, " ")
    .trim();

  if (normalized === "royal challengers bangalore") {
    return "royal challengers bengaluru";
  }

  return normalized;
}

function normalizeTeamABinaryStoredScoreRow(input: {
  row: StoredScoreRow;
  modelScoreId: number;
  snapshotTime: string;
  primaryProbability: number;
  marketImpliedProbability: number | null;
  baseSkippedRow: Omit<HistoricalPredictionSkippedRow, "reason" | "detail">;
}):
  | { status: "ready"; row: HistoricalPredictionRow }
  | { status: "skipped"; skippedRow: HistoricalPredictionSkippedRow } {
  const winner = input.row.winning_team_name as string;
  const actualOutcome =
    winner === input.row.team_a_name
      ? 1
      : winner === input.row.team_b_name
        ? 0
        : null;

  if (actualOutcome === null) {
    return {
      status: "skipped",
      skippedRow: {
        ...input.baseSkippedRow,
        reason: "winner_not_in_match_teams",
        detail:
          "Winning team did not match either canonical team name, so the historical binary label is invalid.",
      },
    };
  }

  const socialOffProbability =
    input.row.checkpoint_type === "innings_break"
      ? readNumericPath(input.row.score_payload, ["teamABaseWinProbability"])
      : input.primaryProbability;

  if (socialOffProbability === null) {
    return {
      status: "skipped",
      skippedRow: {
        ...input.baseSkippedRow,
        reason: "invalid_probability_payload",
        detail:
          "Innings-break score_payload must include teamABaseWinProbability for social-on/off comparison.",
      },
    };
  }

  return {
    status: "ready",
    row: {
      modelKey: input.row.model_key,
      checkpointType: input.row.checkpoint_type,
      modelScoreId: input.modelScoreId,
      matchSlug: input.row.match_slug,
      season: input.row.season,
      snapshotTime: input.snapshotTime,
      actualOutcome,
      positiveClassLabel: input.row.team_a_name,
      negativeClassLabel: input.row.team_b_name,
      primaryProbability: input.primaryProbability,
      socialOnProbability: input.primaryProbability,
      socialOffProbability,
      socialSupported: input.row.checkpoint_type === "innings_break",
      marketImpliedProbability: input.marketImpliedProbability,
      provenance: {
        source: "model_scores",
        modelScoreId: input.modelScoreId,
        positiveClass: "team_a",
      },
    },
  };
}

function readNumericPath(
  value: JsonObject,
  path: readonly string[],
): number | null {
  let cursor: unknown = value;

  for (const segment of path) {
    if (!isRecord(cursor)) {
      return null;
    }

    cursor = cursor[segment];
  }

  if (typeof cursor !== "number" || !Number.isFinite(cursor)) {
    return null;
  }

  return normalizeProbability(cursor);
}
