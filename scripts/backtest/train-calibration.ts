import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/index.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import { scoreBaselineIplPreMatch } from "../../src/models/base/index.js";
import { createPgPool, closePgPool } from "../../src/repositories/postgres.js";
import { calculateProbabilityMetrics } from "../../src/backtest/metrics.js";
import {
  fitIsotonicCalibration,
  applyIsotonicCalibration,
  fitPlattCalibration,
  applyPlattCalibration,
  type IsotonicCalibrationModel,
  type PlattCalibrationModel,
} from "../../src/backtest/calibration.js";
import type { HistoricalPredictionRow } from "../../src/backtest/types.js";

interface CliOptions {
  trainSeasonFrom: number;
  trainSeasonTo: number;
  testSeasonFrom: number;
  testSeasonTo: number;
  persistModelKey?: string;
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

interface MatchWithPrediction {
  match: CompletedMatchRow;
  season: number;
  rawProbability: number;
  actualOutcome: 0 | 1;
}

interface CalibrationResult {
  trainSeasons: { from: number; to: number };
  testSeasons: { from: number; to: number };
  trainSampleSize: number;
  testSampleSize: number;
  uncalibrated: {
    train: MetricsSummary;
    test: MetricsSummary;
  };
  plattCalibrated: {
    model: PlattCalibrationModel;
    train: MetricsSummary;
    test: MetricsSummary;
  };
  isotonicCalibrated: {
    model: IsotonicCalibrationModel;
    train: MetricsSummary;
    test: MetricsSummary;
  };
  improvement: {
    isotonic: {
      calibrationError: number;
      brierScore: number;
      logLoss: number;
    };
    platt: {
      calibrationError: number;
      brierScore: number;
      logLoss: number;
    };
  };
  recommendation: string;
}

interface MetricsSummary {
  logLoss: number;
  brierScore: number;
  calibrationError: number;
  accuracy: number;
  sampleSize: number;
}

const CALIBRATION_MODEL_KEY = "calibration-training";

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    console.error(
      `Loading matches for seasons ${options.trainSeasonFrom}-${options.testSeasonTo}...`,
    );
    const allMatches = await loadCompletedMatches(pool, options.testSeasonTo);
    console.error(`Loaded ${allMatches.length} completed matches.`);

    console.error("Generating predictions for all matches...");
    const predictions = await generatePredictions(pool, allMatches);
    console.error(`Generated predictions for ${predictions.length} matches.`);

    const trainPredictions = predictions.filter(
      (p) =>
        p.season >= options.trainSeasonFrom &&
        p.season <= options.trainSeasonTo,
    );
    const testPredictions = predictions.filter(
      (p) =>
        p.season >= options.testSeasonFrom && p.season <= options.testSeasonTo,
    );

    console.error(
      `Train set: ${trainPredictions.length} matches (${options.trainSeasonFrom}-${options.trainSeasonTo})`,
    );
    console.error(
      `Test set: ${testPredictions.length} matches (${options.testSeasonFrom}-${options.testSeasonTo})`,
    );

    console.error("Fitting calibration models on training data...");
    const trainingRows = trainPredictions.map((p) => ({
      probability: p.rawProbability,
      outcome: p.actualOutcome,
    }));

    const isotonicModel = fitIsotonicCalibration(trainingRows);
    const plattModel = fitPlattCalibration(trainingRows);

    console.error(
      `Isotonic model: ${isotonicModel.thresholds.length} thresholds`,
    );
    console.error(
      `Platt model: intercept=${plattModel.intercept}, slope=${plattModel.slope}, converged=${plattModel.converged}`,
    );

    const uncalibratedTrainMetrics = evaluateMetrics(
      trainPredictions,
      (p) => p.rawProbability,
    );
    const uncalibratedTestMetrics = evaluateMetrics(
      testPredictions,
      (p) => p.rawProbability,
    );

    const isotonicTrainMetrics = evaluateMetrics(trainPredictions, (p) =>
      applyIsotonicCalibration(p.rawProbability, isotonicModel),
    );
    const isotonicTestMetrics = evaluateMetrics(testPredictions, (p) =>
      applyIsotonicCalibration(p.rawProbability, isotonicModel),
    );

    const plattTrainMetrics = evaluateMetrics(trainPredictions, (p) =>
      applyPlattCalibration(p.rawProbability, plattModel),
    );
    const plattTestMetrics = evaluateMetrics(testPredictions, (p) =>
      applyPlattCalibration(p.rawProbability, plattModel),
    );

    const isotonicImprovement = {
      calibrationError: roundTo(
        uncalibratedTestMetrics.calibrationError -
          isotonicTestMetrics.calibrationError,
        6,
      ),
      brierScore: roundTo(
        uncalibratedTestMetrics.brierScore - isotonicTestMetrics.brierScore,
        6,
      ),
      logLoss: roundTo(
        uncalibratedTestMetrics.logLoss - isotonicTestMetrics.logLoss,
        6,
      ),
    };

    const plattImprovement = {
      calibrationError: roundTo(
        uncalibratedTestMetrics.calibrationError -
          plattTestMetrics.calibrationError,
        6,
      ),
      brierScore: roundTo(
        uncalibratedTestMetrics.brierScore - plattTestMetrics.brierScore,
        6,
      ),
      logLoss: roundTo(
        uncalibratedTestMetrics.logLoss - plattTestMetrics.logLoss,
        6,
      ),
    };

    const recommendation = selectCalibrationRecommendation({
      plattImprovement,
      isotonicImprovement,
      plattConverged: plattModel.converged,
    });
    const shouldEnableRuntimeCalibration = recommendation === "platt";

    if (options.persistModelKey !== undefined) {
      const now = new Date().toISOString();
      const existing = await loadExistingModelRegistryByKey(
        pool,
        options.persistModelKey,
        "pre_match",
      );
      const metadata = {
        ...existing.metadata,
        source: "train-calibration-script",
        preMatchModelOptions: {
          enabled: shouldEnableRuntimeCalibration,
          calibrationMethod: recommendation,
          ...(recommendation === "platt"
            ? {
                plattCalibration: {
                  intercept: plattModel.intercept,
                  slope: plattModel.slope,
                  trainSampleSize: plattModel.trainSampleSize,
                  converged: plattModel.converged,
                },
              }
            : {}),
        },
        calibrationTraining: {
          trainSeasonFrom: options.trainSeasonFrom,
          trainSeasonTo: options.trainSeasonTo,
          testSeasonFrom: options.testSeasonFrom,
          testSeasonTo: options.testSeasonTo,
          recommendation,
          shouldEnableRuntimeCalibration,
        },
      };

      await pool.query(
        `
          update model_registry
          set
            training_window = $3,
            is_active = true,
            metadata = $4,
            created_at = $5
          where id = $1
            and checkpoint_type = $2
        `,
        [
          existing.id,
          "pre_match",
          `${options.trainSeasonFrom}-${options.trainSeasonTo}`,
          metadata,
          now,
        ],
      );

      console.error(
        `Persisted calibration metadata to model key ${options.persistModelKey}`,
      );
    }

    const result: CalibrationResult = {
      trainSeasons: {
        from: options.trainSeasonFrom,
        to: options.trainSeasonTo,
      },
      testSeasons: { from: options.testSeasonFrom, to: options.testSeasonTo },
      trainSampleSize: trainPredictions.length,
      testSampleSize: testPredictions.length,
      uncalibrated: {
        train: uncalibratedTrainMetrics,
        test: uncalibratedTestMetrics,
      },
      plattCalibrated: {
        model: plattModel,
        train: plattTrainMetrics,
        test: plattTestMetrics,
      },
      isotonicCalibrated: {
        model: isotonicModel,
        train: isotonicTrainMetrics,
        test: isotonicTestMetrics,
      },
      improvement: {
        isotonic: isotonicImprovement,
        platt: plattImprovement,
      },
      recommendation: describeCalibrationRecommendation(
        recommendation,
        plattImprovement,
        isotonicImprovement,
      ),
    };

    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function selectCalibrationRecommendation(input: {
  plattImprovement: {
    calibrationError: number;
    brierScore: number;
    logLoss: number;
  };
  isotonicImprovement: {
    calibrationError: number;
    brierScore: number;
    logLoss: number;
  };
  plattConverged: boolean;
}): "platt" | "none" {
  if (
    input.plattConverged &&
    input.plattImprovement.calibrationError > 0 &&
    input.plattImprovement.brierScore > 0 &&
    input.plattImprovement.logLoss > 0
  ) {
    return "platt";
  }

  return "none";
}

function describeCalibrationRecommendation(
  recommendation: "platt" | "none",
  plattImprovement: {
    calibrationError: number;
    brierScore: number;
    logLoss: number;
  },
  isotonicImprovement: {
    calibrationError: number;
    brierScore: number;
    logLoss: number;
  },
): string {
  if (recommendation === "platt") {
    return `Use platt calibration - improves test logLoss by ${plattImprovement.logLoss}, brier by ${plattImprovement.brierScore}, calibration error by ${plattImprovement.calibrationError}`;
  }

  return `Do not enable runtime calibration - isotonic overfits and platt does not clear all quality gates (platt: logLoss ${plattImprovement.logLoss}, brier ${plattImprovement.brierScore}, calibration ${plattImprovement.calibrationError}; isotonic: logLoss ${isotonicImprovement.logLoss}, brier ${isotonicImprovement.brierScore}, calibration ${isotonicImprovement.calibrationError})`;
}

async function generatePredictions(
  pool: ReturnType<typeof createPgPool>,
  matches: CompletedMatchRow[],
): Promise<MatchWithPrediction[]> {
  const results: MatchWithPrediction[] = [];

  for (const match of matches) {
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
          source: "calibration_training",
          generated: true,
        },
      },
    });

    const featureRow = buildBaselinePreMatchFeatureRow(
      checkpoint,
      featureContext,
    );
    const score = scoreBaselineIplPreMatch(featureRow);

    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;

    if (actualOutcome === null) {
      continue;
    }

    results.push({
      match,
      season: match.season,
      rawProbability: score.teamAWinProbability,
      actualOutcome,
    });
  }

  return results;
}

function evaluateMetrics(
  predictions: MatchWithPrediction[],
  probabilitySelector: (p: MatchWithPrediction) => number,
): MetricsSummary {
  const rows: HistoricalPredictionRow[] = predictions.map((p, index) => ({
    modelKey: CALIBRATION_MODEL_KEY,
    checkpointType: "pre_match" as const,
    modelScoreId: index + 1,
    matchSlug: p.match.match_slug,
    season: p.season,
    snapshotTime: p.match.scheduled_start.toISOString(),
    actualOutcome: p.actualOutcome,
    positiveClassLabel: p.match.team_a_name,
    negativeClassLabel: p.match.team_b_name,
    primaryProbability: probabilitySelector(p),
    socialOnProbability: probabilitySelector(p),
    socialOffProbability: probabilitySelector(p),
    socialSupported: false,
    marketImpliedProbability: null,
    provenance: { source: "calibration_training" },
  }));

  const metrics = calculateProbabilityMetrics(
    rows,
    (r) => r.primaryProbability,
    10,
  );

  return {
    logLoss: metrics.logLoss,
    brierScore: metrics.brierScore,
    calibrationError: metrics.calibrationError,
    accuracy: metrics.accuracy,
    sampleSize: metrics.sampleSize,
  };
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

async function loadExistingModelRegistryByKey(
  pool: ReturnType<typeof createPgPool>,
  modelKey: string,
  checkpointType: "pre_match",
): Promise<{ id: number; metadata: Record<string, unknown> }> {
  const result = await pool.query<{
    id: number;
    metadata: Record<string, unknown> | null;
  }>(
    `
      select id, metadata
      from model_registry
      where model_key = $1
        and checkpoint_type = $2
      order by created_at desc, id desc
      limit 1
    `,
    [modelKey, checkpointType],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new Error(
      `Cannot persist calibration: model key ${modelKey} (${checkpointType}) not found.`,
    );
  }

  return {
    id: Number(row.id),
    metadata: row.metadata ?? {},
  };
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let trainSeasonFrom: number | null = null;
  let trainSeasonTo: number | null = null;
  let testSeasonFrom: number | null = null;
  let testSeasonTo: number | null = null;
  let persistModelKey: string | undefined;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--train-from") {
      trainSeasonFrom = parseIntegerArg("--train-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--train-to") {
      trainSeasonTo = parseIntegerArg("--train-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--test-from") {
      testSeasonFrom = parseIntegerArg("--test-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--test-to") {
      testSeasonTo = parseIntegerArg("--test-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--persist-model-key") {
      const value = argv[index + 1];
      if (typeof value !== "string" || value.trim().length === 0) {
        throw new Error("--persist-model-key requires a non-empty value.");
      }
      persistModelKey = value.trim();
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --train-from, --train-to, --test-from, --test-to, --persist-model-key.`,
    );
  }

  if (
    trainSeasonFrom === null ||
    trainSeasonTo === null ||
    testSeasonFrom === null ||
    testSeasonTo === null
  ) {
    throw new Error(
      "Missing required arguments: --train-from, --train-to, --test-from, --test-to",
    );
  }

  if (trainSeasonTo >= testSeasonFrom) {
    throw new Error(
      "Training seasons must end before test seasons start to prevent leakage.",
    );
  }

  return {
    trainSeasonFrom,
    trainSeasonTo,
    testSeasonFrom,
    testSeasonTo,
    ...(persistModelKey === undefined ? {} : { persistModelKey }),
  };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }
  return parsed;
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Calibration training failed: ${message}`);
  process.exitCode = 1;
});
