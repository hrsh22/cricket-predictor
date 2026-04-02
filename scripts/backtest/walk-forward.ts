import { loadAppConfig } from "../../src/config/index.js";
import {
  applyPlattCalibration,
  fitPlattCalibration,
  runHistoricalBacktestFromRows,
  type HistoricalPredictionRow,
} from "../../src/backtest/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/index.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import {
  DEFAULT_MODEL_WEIGHTS,
  scoreBaselineIplPreMatch,
  type ModelWeights,
} from "../../src/models/base/index.js";
import { createPgPool, closePgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
  calibrationBinCount?: number;
  minimumTrainingSamples?: number;
  minimumTestSamples?: number;
  applyPlattCalibration?: boolean;
  ensembleMode?: "none" | "dual_weight";
  applyPlattOnEnsemble?: boolean;
}

interface CompletedMatchRow {
  match_slug: string;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  winning_team_name: string;
}

const WALK_FORWARD_MODEL_KEY = "walk-forward-pre-match-v2";
const MIN_CALIBRATION_SAMPLE_SIZE = 20;

const ENSEMBLE_ALT_WEIGHTS: ModelWeights = {
  rating: 0.7,
  form: 0.5,
  venue: 0.0,
  headToHead: 0.35,
  rest: 0.2,
  congestion: 0.2,
  dewFactor: 0.0,
  homeAdvantage: 0.0,
  pitchBattingIndex: 0.0,
};

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const rows = await buildWalkForwardRows(pool, options.seasonTo, options);
    const result = runHistoricalBacktestFromRows(rows, {
      modelKey: WALK_FORWARD_MODEL_KEY,
      checkpointType: "pre_match",
      evaluationSeasonFrom: options.seasonFrom,
      evaluationSeasonTo: options.seasonTo,
      ...(options.calibrationBinCount === undefined
        ? {}
        : { calibrationBinCount: options.calibrationBinCount }),
      ...(options.minimumTrainingSamples === undefined
        ? {}
        : { minimumTrainingSamples: options.minimumTrainingSamples }),
      ...(options.minimumTestSamples === undefined
        ? {}
        : { minimumTestSamples: options.minimumTestSamples }),
    });

    process.stdout.write(
      `${JSON.stringify(
        {
          mode: "walk_forward_recompute",
          temporalGuard: "per-match-as-of-only",
          temporalGuardDetail:
            "scheduled_start_cutoff_only (strict < asOfDate); does not use result-known timestamp",
          probabilityCalibration:
            options.applyPlattCalibration === true
              ? "platt_per_fold_train_only"
              : "none",
          ensembleMode: options.ensembleMode ?? "none",
          ensembleCalibration:
            options.applyPlattOnEnsemble === true
              ? "platt_per_fold_train_only"
              : "none",
          modelKey: WALK_FORWARD_MODEL_KEY,
          totalRowsGenerated: rows.length,
          result,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function buildWalkForwardRows(
  pool: ReturnType<typeof createPgPool>,
  seasonTo: number,
  options: CliOptions = { seasonFrom: 2008, seasonTo },
): Promise<HistoricalPredictionRow[]> {
  const matches = await loadCompletedMatches(pool, seasonTo);
  const rows: HistoricalPredictionRow[] = [];
  const priorRowsBySeason = new Map<number, HistoricalPredictionRow[]>();
  const priorMemberPredictionsBySeason = new Map<
    number,
    { base: number; alt: number; outcome: 0 | 1; snapshotTime: string }[]
  >();

  for (const [index, match] of matches.entries()) {
    const asOfDate = new Date(match.scheduled_start);
    const featureContext = await buildFeatureContextFromHistory(pool, asOfDate);
    const checkpoint = parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: {
        competition: "IPL",
        matchSlug: match.match_slug,
        sourceMatchId: null,
        season: match.season,
        scheduledStart: asOfDate.toISOString(),
        teamAName: match.team_a_name,
        teamBName: match.team_b_name,
        venueName: match.venue_name,
        status: "scheduled",
        tossWinnerTeamName: null,
        tossDecision: null,
        winningTeamName: null,
        resultType: null,
      },
      state: {
        matchSlug: match.match_slug,
        checkpointType: "pre_match",
        snapshotTime: new Date(asOfDate.getTime() - 60_000).toISOString(),
        stateVersion: 1,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload: {
          source: "walk_forward_backtest",
          generated: true,
        },
      },
    });

    const featureRow = buildBaselinePreMatchFeatureRow(
      checkpoint,
      featureContext,
    );
    const baselineScore = scoreBaselineIplPreMatch(featureRow, {
      weights: DEFAULT_MODEL_WEIGHTS,
    });
    const altScore =
      options.ensembleMode === "dual_weight"
        ? scoreBaselineIplPreMatch(featureRow, {
            weights: ENSEMBLE_ALT_WEIGHTS,
          })
        : null;

    const trainRowsForSeason = collectPriorRowsForSeason(
      priorRowsBySeason,
      match.season,
    );
    const priorMemberRowsForSeason = collectPriorMemberRowsForSeason(
      priorMemberPredictionsBySeason,
      match.season,
    );

    const ensembleWeight =
      options.ensembleMode === "dual_weight"
        ? fitDualWeightEnsembleBlend(priorMemberRowsForSeason)
        : 1;

    const rawModelProbability =
      options.ensembleMode === "dual_weight" && altScore !== null
        ? blendProbabilities(
            baselineScore.teamAWinProbability,
            altScore.teamAWinProbability,
            ensembleWeight,
          )
        : baselineScore.teamAWinProbability;

    const plattTargetEnabled =
      options.ensembleMode === "dual_weight"
        ? options.applyPlattOnEnsemble === true
        : options.applyPlattCalibration === true;

    const plattModel =
      plattTargetEnabled &&
      trainRowsForSeason.length >= MIN_CALIBRATION_SAMPLE_SIZE
        ? fitPlattCalibration(
            trainRowsForSeason.map((r) => ({
              probability: readRawModelProbability(r),
              outcome: r.actualOutcome,
            })),
          )
        : null;

    const calibratedProbability =
      plattModel === null || plattModel.converged === false
        ? rawModelProbability
        : applyPlattCalibration(rawModelProbability, plattModel);

    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;

    if (actualOutcome === null) {
      continue;
    }

    const row: HistoricalPredictionRow = {
      modelKey: WALK_FORWARD_MODEL_KEY,
      checkpointType: "pre_match",
      modelScoreId: index + 1,
      matchSlug: match.match_slug,
      season: match.season,
      snapshotTime: featureRow.generatedAt,
      actualOutcome,
      positiveClassLabel: match.team_a_name,
      negativeClassLabel: match.team_b_name,
      primaryProbability: calibratedProbability,
      socialOnProbability: calibratedProbability,
      socialOffProbability: calibratedProbability,
      socialSupported: false,
      marketImpliedProbability: null,
      provenance: {
        source: "walk_forward_recompute",
        asOfDate: asOfDate.toISOString(),
        rawModelProbability,
        probabilityModel:
          plattModel === null ? "uncalibrated" : "platt_calibrated_train_only",
        plattTrainSampleSize: trainRowsForSeason.length,
        plattConverged: plattModel?.converged ?? null,
      },
    };

    rows.push(row);
    const seasonRows = priorRowsBySeason.get(match.season) ?? [];
    seasonRows.push(row);
    priorRowsBySeason.set(match.season, seasonRows);

    if (options.ensembleMode === "dual_weight" && altScore !== null) {
      const memberRows = priorMemberPredictionsBySeason.get(match.season) ?? [];
      memberRows.push({
        base: baselineScore.teamAWinProbability,
        alt: altScore.teamAWinProbability,
        outcome: actualOutcome,
        snapshotTime: featureRow.generatedAt,
      });
      priorMemberPredictionsBySeason.set(match.season, memberRows);
    }
  }

  return rows;
}

function collectPriorRowsForSeason(
  rowsBySeason: Map<number, HistoricalPredictionRow[]>,
  season: number,
): HistoricalPredictionRow[] {
  const prior: HistoricalPredictionRow[] = [];

  for (const [rowSeason, seasonRows] of rowsBySeason.entries()) {
    if (rowSeason < season) {
      prior.push(...seasonRows);
    }
  }

  prior.sort((a, b) =>
    a.snapshotTime === b.snapshotTime
      ? a.modelScoreId - b.modelScoreId
      : a.snapshotTime.localeCompare(b.snapshotTime),
  );

  return prior;
}

function collectPriorMemberRowsForSeason(
  rowsBySeason: Map<
    number,
    { base: number; alt: number; outcome: 0 | 1; snapshotTime: string }[]
  >,
  season: number,
): { base: number; alt: number; outcome: 0 | 1; snapshotTime: string }[] {
  const prior: {
    base: number;
    alt: number;
    outcome: 0 | 1;
    snapshotTime: string;
  }[] = [];

  for (const [rowSeason, seasonRows] of rowsBySeason.entries()) {
    if (rowSeason < season) {
      prior.push(...seasonRows);
    }
  }

  prior.sort((a, b) => a.snapshotTime.localeCompare(b.snapshotTime));

  return prior;
}

function fitDualWeightEnsembleBlend(
  rows: { base: number; alt: number; outcome: 0 | 1; snapshotTime: string }[],
): number {
  if (rows.length < MIN_CALIBRATION_SAMPLE_SIZE) {
    return 0.65;
  }

  let bestWeight = 0.65;
  let bestLogLoss = Number.POSITIVE_INFINITY;

  for (let step = 0; step <= 10; step += 1) {
    const baseWeight = step / 10;
    const loss = averageLogLoss(
      rows.map((row) => ({
        probability: blendProbabilities(row.base, row.alt, baseWeight),
        outcome: row.outcome,
      })),
    );

    if (loss < bestLogLoss) {
      bestLogLoss = loss;
      bestWeight = baseWeight;
    }
  }

  return bestWeight;
}

function blendProbabilities(
  baseProbability: number,
  altProbability: number,
  baseWeight: number,
): number {
  const clippedWeight = Math.max(0, Math.min(1, baseWeight));
  const blended =
    clippedWeight * baseProbability + (1 - clippedWeight) * altProbability;
  return Math.max(1e-6, Math.min(1 - 1e-6, Number(blended.toFixed(6))));
}

function averageLogLoss(
  rows: { probability: number; outcome: 0 | 1 }[],
): number {
  if (rows.length === 0) {
    return Number.POSITIVE_INFINITY;
  }

  let total = 0;
  for (const row of rows) {
    const p = Math.max(1e-6, Math.min(1 - 1e-6, row.probability));
    total += row.outcome === 1 ? -Math.log(p) : -Math.log(1 - p);
  }

  return total / rows.length;
}

function readRawModelProbability(row: HistoricalPredictionRow): number {
  const provenance = row.provenance;
  if (
    typeof provenance === "object" &&
    provenance !== null &&
    typeof provenance["rawModelProbability"] === "number" &&
    Number.isFinite(provenance["rawModelProbability"])
  ) {
    return provenance["rawModelProbability"];
  }

  return row.primaryProbability;
}

async function loadCompletedMatches(
  pool: ReturnType<typeof createPgPool>,
  seasonTo: number,
): Promise<CompletedMatchRow[]> {
  const result = await pool.query<CompletedMatchRow>(
    `
      select
        match_slug,
        season,
        scheduled_start,
        team_a_name,
        team_b_name,
        venue_name,
        winning_team_name
      from canonical_matches
      where competition = 'IPL'
        and season <= $1
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
      order by scheduled_start asc, id asc
    `,
    [seasonTo],
  );

  return result.rows;
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom: number | null = null;
  let seasonTo: number | null = null;
  let calibrationBinCount: number | undefined;
  let minimumTrainingSamples: number | undefined;
  let minimumTestSamples: number | undefined;
  let applyPlattCalibration = false;
  let ensembleMode: "none" | "dual_weight" = "none";
  let applyPlattOnEnsemble = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--season-from") {
      seasonFrom = parseIntegerArg("--season-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--season-to") {
      seasonTo = parseIntegerArg("--season-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--bin-count") {
      calibrationBinCount = parseIntegerArg("--bin-count", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--min-training-samples") {
      minimumTrainingSamples = parseIntegerArg(
        "--min-training-samples",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    if (argument === "--min-test-samples") {
      minimumTestSamples = parseIntegerArg(
        "--min-test-samples",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    if (argument === "--apply-platt-calibration") {
      applyPlattCalibration = true;
      continue;
    }

    if (argument === "--ensemble-mode") {
      const value = argv[index + 1];
      if (value !== "none" && value !== "dual_weight") {
        throw new Error(
          '--ensemble-mode must be either "none" or "dual_weight".',
        );
      }
      ensembleMode = value;
      index += 1;
      continue;
    }

    if (argument === "--apply-platt-on-ensemble") {
      applyPlattOnEnsemble = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from, --season-to, optional --bin-count, --min-training-samples, --min-test-samples, --apply-platt-calibration, --ensemble-mode, --apply-platt-on-ensemble.`,
    );
  }

  if (seasonFrom === null || seasonTo === null) {
    throw new Error(
      "Missing required --season-from <year> and --season-to <year> arguments.",
    );
  }

  return {
    seasonFrom,
    seasonTo,
    ...(calibrationBinCount === undefined ? {} : { calibrationBinCount }),
    ...(minimumTrainingSamples === undefined ? {} : { minimumTrainingSamples }),
    ...(minimumTestSamples === undefined ? {} : { minimumTestSamples }),
    applyPlattCalibration,
    ensembleMode,
    applyPlattOnEnsemble,
  };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Walk-forward backtest failed: ${message}`);
  process.exitCode = 1;
});
