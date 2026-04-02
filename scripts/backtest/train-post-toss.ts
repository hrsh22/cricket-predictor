import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import { createPgPool, closePgPool } from "../../src/repositories/postgres.js";
import {
  applyPlattCalibration,
  fitPlattCalibration,
} from "../../src/backtest/calibration.js";
import { calculateProbabilityMetrics } from "../../src/backtest/metrics.js";
import type { HistoricalPredictionRow } from "../../src/backtest/types.js";
import {
  scorePostTossValuation,
  type PostTossScoringResult,
} from "../../src/scoring/post-toss.js";

interface CliOptions {
  trainSeasonFrom: number;
  trainSeasonTo: number;
  testSeasonFrom: number;
  testSeasonTo: number;
  modelKey?: string;
}

interface CompletedMatchRow {
  match_slug: string;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  toss_winner_team_name: string;
  toss_decision: "bat" | "bowl";
  winning_team_name: string;
}

interface MatchPrediction {
  season: number;
  matchSlug: string;
  snapshotTime: string;
  teamAName: string;
  teamBName: string;
  tossWinnerTeamName: string;
  tossDecision: "bat" | "bowl";
  venueTossDecisionWinRate: number;
  actualOutcome: 0 | 1;
  baseProbability: number;
  marketImpliedProbability: number;
}

interface PostTossTrainingResult {
  trainSeasons: { from: number; to: number };
  testSeasons: { from: number; to: number };
  bestTossStrengths: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  };
  defaultTossStrengths: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  };
  uncalibrated: {
    train: MetricsSummary;
    test: MetricsSummary;
  };
  calibrated: {
    model: {
      intercept: number;
      slope: number;
      converged: boolean;
      trainSampleSize: number;
    };
    train: MetricsSummary;
    test: MetricsSummary;
  };
  improvement: {
    logLoss: number;
    brierScore: number;
    calibrationError: number;
  };
  shouldEnableRuntimeCalibration: boolean;
}

interface MetricsSummary {
  logLoss: number;
  brierScore: number;
  calibrationError: number;
  accuracy: number;
  sampleSize: number;
}

const CANDIDATE_STRENGTHS = [0.005, 0.01, 0.015, 0.02, 0.025];
const CANDIDATE_VENUE_TOSS_STRENGTHS = [0, 0.025, 0.05, 0.075];

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const allMatches = await loadCompletedPostTossMatches(
      pool,
      options.testSeasonTo,
    );
    const predictions = await generatePredictions(pool, allMatches);

    const train = predictions.filter(
      (row) =>
        row.season >= options.trainSeasonFrom &&
        row.season <= options.trainSeasonTo,
    );
    const test = predictions.filter(
      (row) =>
        row.season >= options.testSeasonFrom &&
        row.season <= options.testSeasonTo,
    );

    const defaultStrengths = {
      bowlDecisionStrength: 0.015,
      batDecisionStrength: 0.01,
      venueTossStrength: 0.05,
    };

    const bestStrengths = fitTossStrengths(train);
    const trainRowsForBest = toHistoricalRows(train, bestStrengths, null);
    const testRowsForBest = toHistoricalRows(test, bestStrengths, null);

    const plattModel = fitPlattCalibration(
      trainRowsForBest.map((row) => ({
        probability: row.primaryProbability,
        outcome: row.actualOutcome,
      })),
    );

    const calibratedTrainRows = toHistoricalRows(
      train,
      bestStrengths,
      plattModel,
    );
    const calibratedTestRows = toHistoricalRows(
      test,
      bestStrengths,
      plattModel,
    );

    const uncalibratedTrain = calculateProbabilityMetrics(
      trainRowsForBest,
      (r) => r.primaryProbability,
      10,
    );
    const uncalibratedTest = calculateProbabilityMetrics(
      testRowsForBest,
      (r) => r.primaryProbability,
      10,
    );

    const calibratedTrain = calculateProbabilityMetrics(
      calibratedTrainRows,
      (r) => r.primaryProbability,
      10,
    );
    const calibratedTest = calculateProbabilityMetrics(
      calibratedTestRows,
      (r) => r.primaryProbability,
      10,
    );

    const improvement = {
      logLoss: roundTo(uncalibratedTest.logLoss - calibratedTest.logLoss, 6),
      brierScore: roundTo(
        uncalibratedTest.brierScore - calibratedTest.brierScore,
        6,
      ),
      calibrationError: roundTo(
        uncalibratedTest.calibrationError - calibratedTest.calibrationError,
        6,
      ),
    };
    const shouldEnableRuntimeCalibration =
      improvement.logLoss > 0 &&
      improvement.brierScore > 0 &&
      improvement.calibrationError > 0 &&
      plattModel.converged;

    if (options.modelKey !== undefined) {
      await persistPostTossModelOptions(pool, {
        modelKey: options.modelKey,
        trainSeasonFrom: options.trainSeasonFrom,
        trainSeasonTo: options.trainSeasonTo,
        testSeasonFrom: options.testSeasonFrom,
        testSeasonTo: options.testSeasonTo,
        tossStrengths: bestStrengths,
        plattModel,
        shouldEnableRuntimeCalibration,
      });
    }

    const result: PostTossTrainingResult = {
      trainSeasons: {
        from: options.trainSeasonFrom,
        to: options.trainSeasonTo,
      },
      testSeasons: {
        from: options.testSeasonFrom,
        to: options.testSeasonTo,
      },
      bestTossStrengths: bestStrengths,
      defaultTossStrengths: defaultStrengths,
      uncalibrated: {
        train: summarizeMetrics(uncalibratedTrain),
        test: summarizeMetrics(uncalibratedTest),
      },
      calibrated: {
        model: {
          intercept: plattModel.intercept,
          slope: plattModel.slope,
          converged: plattModel.converged,
          trainSampleSize: plattModel.trainSampleSize,
        },
        train: summarizeMetrics(calibratedTrain),
        test: summarizeMetrics(calibratedTest),
      },
      improvement,
      shouldEnableRuntimeCalibration,
    };

    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function fitTossStrengths(trainRows: MatchPrediction[]): {
  bowlDecisionStrength: number;
  batDecisionStrength: number;
  venueTossStrength: number;
} {
  let best = {
    bowlDecisionStrength: 0.015,
    batDecisionStrength: 0.01,
    venueTossStrength: 0.05,
  };
  let bestLoss = Number.POSITIVE_INFINITY;

  for (const bowl of CANDIDATE_STRENGTHS) {
    for (const bat of CANDIDATE_STRENGTHS) {
      for (const venueToss of CANDIDATE_VENUE_TOSS_STRENGTHS) {
        const rows = toHistoricalRows(
          trainRows,
          {
            bowlDecisionStrength: bowl,
            batDecisionStrength: bat,
            venueTossStrength: venueToss,
          },
          null,
        );
        const metrics = calculateProbabilityMetrics(
          rows,
          (row) => row.primaryProbability,
          10,
        );
        if (metrics.logLoss < bestLoss) {
          bestLoss = metrics.logLoss;
          best = {
            bowlDecisionStrength: bowl,
            batDecisionStrength: bat,
            venueTossStrength: venueToss,
          };
        }
      }
    }
  }

  return best;
}

function toHistoricalRows(
  rows: MatchPrediction[],
  tossStrengths: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  },
  plattModel: ReturnType<typeof fitPlattCalibration> | null,
): HistoricalPredictionRow[] {
  return rows.map((row, index) => {
    const adjusted = applyTossStrength(row.baseProbability, row, tossStrengths);
    const probability =
      plattModel === null
        ? adjusted
        : applyPlattCalibration(adjusted, plattModel);

    return {
      modelKey: "post-toss-training",
      checkpointType: "post_toss",
      modelScoreId: index + 1,
      matchSlug: row.matchSlug,
      season: row.season,
      snapshotTime: row.snapshotTime,
      actualOutcome: row.actualOutcome,
      positiveClassLabel: row.teamAName,
      negativeClassLabel: row.teamBName,
      primaryProbability: probability,
      socialOnProbability: probability,
      socialOffProbability: probability,
      socialSupported: false,
      marketImpliedProbability: row.marketImpliedProbability,
      provenance: {
        source: "train-post-toss",
      },
    };
  });
}

function applyTossStrength(
  baseProbability: number,
  prediction: MatchPrediction,
  strengths: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  },
): number {
  const tossWinnerIsTeamA =
    prediction.tossWinnerTeamName === prediction.teamAName;
  const tossDecisionIsBowl = prediction.tossDecision === "bowl";
  const side = tossWinnerIsTeamA ? 1 : -1;
  const decisionStrength = tossDecisionIsBowl
    ? strengths.bowlDecisionStrength
    : strengths.batDecisionStrength;
  const venueCentered = prediction.venueTossDecisionWinRate - 0.5;
  const adjusted =
    baseProbability +
    side * decisionStrength +
    side * venueCentered * strengths.venueTossStrength;
  return Math.max(0, Math.min(1, Number(adjusted.toFixed(6))));
}

async function generatePredictions(
  pool: ReturnType<typeof createPgPool>,
  matches: CompletedMatchRow[],
): Promise<MatchPrediction[]> {
  const rows: MatchPrediction[] = [];

  for (const match of matches) {
    const scheduledStart = match.scheduled_start;
    const snapshotTime = new Date(
      scheduledStart.getTime() - 5 * 60_000,
    ).toISOString();
    const checkpoint = parseCanonicalCheckpoint({
      checkpointType: "post_toss",
      match: {
        competition: "IPL",
        matchSlug: match.match_slug,
        sourceMatchId: null,
        season: match.season,
        scheduledStart: scheduledStart.toISOString(),
        teamAName: match.team_a_name,
        teamBName: match.team_b_name,
        venueName: match.venue_name,
        status: "in_progress",
        tossWinnerTeamName: match.toss_winner_team_name,
        tossDecision: match.toss_decision,
        winningTeamName: null,
        resultType: null,
      },
      state: {
        matchSlug: match.match_slug,
        checkpointType: "post_toss",
        snapshotTime,
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
          source: "train-post-toss",
        },
      },
    });

    const context = await buildFeatureContextFromHistory(pool, scheduledStart);
    const scored: PostTossScoringResult = scorePostTossValuation({
      checkpoint,
      featureContext: context,
      marketImpliedProbability: 0.5,
      evaluatedAt: snapshotTime,
    });

    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;

    if (actualOutcome === null) {
      continue;
    }

    rows.push({
      season: match.season,
      matchSlug: match.match_slug,
      snapshotTime,
      teamAName: match.team_a_name,
      teamBName: match.team_b_name,
      tossWinnerTeamName: match.toss_winner_team_name,
      tossDecision: match.toss_decision,
      venueTossDecisionWinRate: readVenueTossDecisionWinRate(
        scored.featureRow.features,
      ),
      actualOutcome,
      baseProbability: scored.baseFairWinProbability,
      marketImpliedProbability: 0.5,
    });
  }

  return rows;
}

async function loadCompletedPostTossMatches(
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
        toss_winner_team_name,
        toss_decision,
        winning_team_name
      from canonical_matches
      where competition = 'IPL'
        and season <= $1
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
        and toss_winner_team_name is not null
        and toss_decision is not null
      order by scheduled_start asc, id asc
    `,
    [seasonTo],
  );

  return result.rows;
}

async function persistPostTossModelOptions(
  pool: ReturnType<typeof createPgPool>,
  input: {
    modelKey: string;
    trainSeasonFrom: number;
    trainSeasonTo: number;
    testSeasonFrom: number;
    testSeasonTo: number;
    tossStrengths: {
      bowlDecisionStrength: number;
      batDecisionStrength: number;
      venueTossStrength: number;
    };
    plattModel: ReturnType<typeof fitPlattCalibration>;
    shouldEnableRuntimeCalibration: boolean;
  },
): Promise<void> {
  const result = await pool.query<{
    id: number;
    metadata: Record<string, unknown> | null;
  }>(
    `
      select id, metadata
      from model_registry
      where model_key = $1
        and checkpoint_type = 'post_toss'
      order by created_at desc, id desc
      limit 1
    `,
    [input.modelKey],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new Error(
      `Post-toss model ${input.modelKey} not found in model_registry.`,
    );
  }

  const metadata = {
    ...(row.metadata ?? {}),
    source: "train-post-toss-script",
    postTossModelOptions: {
      enabled: true,
      bowlDecisionStrength: input.tossStrengths.bowlDecisionStrength,
      batDecisionStrength: input.tossStrengths.batDecisionStrength,
      venueTossStrength: input.tossStrengths.venueTossStrength,
      calibrationMethod: "platt",
      plattCalibration: {
        intercept: input.plattModel.intercept,
        slope: input.plattModel.slope,
        converged: input.plattModel.converged,
        trainSampleSize: input.plattModel.trainSampleSize,
      },
      runtimeCalibrationEnabled: input.shouldEnableRuntimeCalibration,
    },
    postTossTraining: {
      trainSeasonFrom: input.trainSeasonFrom,
      trainSeasonTo: input.trainSeasonTo,
      testSeasonFrom: input.testSeasonFrom,
      testSeasonTo: input.testSeasonTo,
    },
  };

  await pool.query(
    `
      update model_registry
      set
        is_active = true,
        training_window = $2,
        metadata = $3,
        created_at = $4
      where id = $1
    `,
    [
      row.id,
      `${input.trainSeasonFrom}-${input.trainSeasonTo}`,
      metadata,
      new Date().toISOString(),
    ],
  );
}

function readVenueTossDecisionWinRate(
  features: Record<string, unknown>,
): number {
  const value = features["venueTossDecisionWinRate"];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0.5;
  }

  if (value < 0) {
    return 0;
  }

  if (value > 1) {
    return 1;
  }

  return Number(value.toFixed(6));
}

function summarizeMetrics(
  input: ReturnType<typeof calculateProbabilityMetrics>,
): MetricsSummary {
  return {
    logLoss: input.logLoss,
    brierScore: input.brierScore,
    calibrationError: input.calibrationError,
    accuracy: input.accuracy,
    sampleSize: input.sampleSize,
  };
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let trainSeasonFrom: number | null = null;
  let trainSeasonTo: number | null = null;
  let testSeasonFrom: number | null = null;
  let testSeasonTo: number | null = null;
  let modelKey: string | undefined;

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

    if (argument === "--model-key") {
      const value = argv[index + 1];
      if (typeof value !== "string" || value.trim().length === 0) {
        throw new Error("--model-key requires a non-empty value.");
      }
      modelKey = value.trim();
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --train-from, --train-to, --test-from, --test-to, optional --model-key.`,
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
    ...(modelKey === undefined ? {} : { modelKey }),
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
  console.error(`Post-toss training failed: ${message}`);
  process.exitCode = 1;
});
