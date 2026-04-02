import type { ModelingRepository } from "../repositories/modeling.js";
import type { JsonObject } from "../domain/primitives.js";
import type { SqlExecutor } from "../repositories/postgres.js";
import {
  calculateCalibrationSummary,
  calculateProbabilityMetrics,
} from "./metrics.js";
import { loadStoredScoreBacktestDataset } from "./repository.js";
import type {
  FoldBacktestSummary,
  HistoricalBacktestOptions,
  HistoricalBacktestResult,
  HistoricalPredictionRow,
  SocialComparisonSummary,
  TimeSplitFold,
  TimeSplitSkippedFold,
} from "./types.js";

const DEFAULT_CALIBRATION_BIN_COUNT = 10;
const DEFAULT_MINIMUM_TRAINING_SAMPLES = 1;
const DEFAULT_MINIMUM_TEST_SAMPLES = 1;
const METRIC_TOLERANCE = 1e-9;

export function createWalkForwardSeasonTimeSplits(
  rows: readonly HistoricalPredictionRow[],
  options: Pick<
    HistoricalBacktestOptions,
    "evaluationSeasonFrom" | "evaluationSeasonTo"
  > & {
    minimumTrainingSamples?: number;
    minimumTestSamples?: number;
  },
): {
  folds: TimeSplitFold[];
  skippedFolds: TimeSplitSkippedFold[];
} {
  const minimumTrainingSamples =
    options.minimumTrainingSamples ?? DEFAULT_MINIMUM_TRAINING_SAMPLES;
  const minimumTestSamples =
    options.minimumTestSamples ?? DEFAULT_MINIMUM_TEST_SAMPLES;

  validateSeasonRange(options.evaluationSeasonFrom, options.evaluationSeasonTo);

  const sortedRows = [...rows].sort(comparePredictionRows);
  const evaluationSeasons = Array.from(
    new Set(
      sortedRows
        .filter(
          (row) =>
            row.season >= options.evaluationSeasonFrom &&
            row.season <= options.evaluationSeasonTo,
        )
        .map((row) => row.season),
    ),
  ).sort((left, right) => left - right);

  const folds: TimeSplitFold[] = [];
  const skippedFolds: TimeSplitSkippedFold[] = [];

  for (const season of evaluationSeasons) {
    const testRows = sortedRows.filter((row) => row.season === season);
    if (testRows.length < minimumTestSamples) {
      skippedFolds.push({
        testSeason: season,
        trainSampleSize: sortedRows.filter(
          (row) =>
            row.season < season &&
            row.snapshotTime <
              (testRows[0] as HistoricalPredictionRow).snapshotTime,
        ).length,
        testSampleSize: testRows.length,
        reason: "insufficient_test_samples",
      });
      continue;
    }

    const earliestTestSnapshot = (testRows[0] as HistoricalPredictionRow)
      .snapshotTime;
    const trainingRows = sortedRows.filter(
      (row) => row.season < season && row.snapshotTime < earliestTestSnapshot,
    );

    if (trainingRows.length < minimumTrainingSamples) {
      skippedFolds.push({
        testSeason: season,
        trainSampleSize: trainingRows.length,
        testSampleSize: testRows.length,
        reason: "insufficient_training_history",
      });
      continue;
    }

    const trainThrough =
      trainingRows[trainingRows.length - 1]?.snapshotTime ?? null;
    const testTo = testRows[testRows.length - 1]?.snapshotTime;
    if (testTo === undefined) {
      skippedFolds.push({
        testSeason: season,
        trainSampleSize: trainingRows.length,
        testSampleSize: testRows.length,
        reason: "insufficient_test_samples",
      });
      continue;
    }

    folds.push({
      testSeason: season,
      trainSampleSize: trainingRows.length,
      testSampleSize: testRows.length,
      trainThrough,
      testFrom: earliestTestSnapshot,
      testTo,
      strictChronology: true,
      trainingRows,
      testRows,
    });
  }

  return {
    folds,
    skippedFolds,
  };
}

export function runHistoricalBacktestFromRows(
  rows: readonly HistoricalPredictionRow[],
  options: HistoricalBacktestOptions,
): HistoricalBacktestResult {
  validateSeasonRange(options.evaluationSeasonFrom, options.evaluationSeasonTo);

  const calibrationBinCount =
    options.calibrationBinCount ?? DEFAULT_CALIBRATION_BIN_COUNT;
  const sortedRows = [...rows].sort(comparePredictionRows);
  const evaluationRows = sortedRows.filter(
    (row) =>
      row.season >= options.evaluationSeasonFrom &&
      row.season <= options.evaluationSeasonTo,
  );

  if (evaluationRows.length === 0) {
    throw new Error(
      "No eligible historical prediction rows were found in the requested evaluation season range.",
    );
  }

  const splitResult = createWalkForwardSeasonTimeSplits(sortedRows, {
    evaluationSeasonFrom: options.evaluationSeasonFrom,
    evaluationSeasonTo: options.evaluationSeasonTo,
    ...(options.minimumTrainingSamples === undefined
      ? {}
      : { minimumTrainingSamples: options.minimumTrainingSamples }),
    ...(options.minimumTestSamples === undefined
      ? {}
      : { minimumTestSamples: options.minimumTestSamples }),
  });

  if (splitResult.folds.length === 0) {
    throw new Error(
      "No chronology-safe walk-forward folds were available for the requested season range.",
    );
  }

  const foldSummaries = splitResult.folds.map((fold) =>
    summarizeFold(fold, calibrationBinCount),
  );
  const foldRows = splitResult.folds.flatMap((fold) => fold.testRows);

  return {
    modelKey: options.modelKey,
    checkpointType: options.checkpointType,
    evaluationSeasonFrom: options.evaluationSeasonFrom,
    evaluationSeasonTo: options.evaluationSeasonTo,
    splitStrategy: "walk_forward_by_season",
    totalLoadedRows: sortedRows.length,
    eligibleRowCount: evaluationRows.length,
    skippedRowCount: 0,
    overallMetrics: calculateProbabilityMetrics(
      foldRows,
      (row) => row.primaryProbability,
      calibrationBinCount,
    ),
    calibration: calculateCalibrationSummary(
      foldRows,
      (row) => row.primaryProbability,
      calibrationBinCount,
    ),
    folds: foldSummaries,
    skippedFolds: splitResult.skippedFolds,
    socialComparison: summarizeSocialComparison(foldRows, calibrationBinCount),
    skippedRows: [],
  };
}

export async function runStoredScoreBacktest(input: {
  executor: SqlExecutor;
  modelingRepository: ModelingRepository;
  options: HistoricalBacktestOptions;
  runKey: string;
  triggeredBy: string;
  startedAt?: string;
  persist?: boolean;
}): Promise<HistoricalBacktestResult> {
  const startedAt = input.startedAt ?? new Date().toISOString();
  const dataset = await loadStoredScoreBacktestDataset(
    input.executor,
    input.options,
  );
  const result = runHistoricalBacktestFromRows(dataset.rows, input.options);
  result.totalLoadedRows = dataset.totalLoadedRows;
  result.skippedRowCount = dataset.skippedRows.length;
  result.skippedRows = dataset.skippedRows;

  if (input.persist === false) {
    return result;
  }

  await input.modelingRepository.saveBacktest({
    runKey: input.runKey,
    modelKey: input.options.modelKey,
    checkpointType: input.options.checkpointType,
    runStatus: "succeeded",
    seasonFrom: input.options.evaluationSeasonFrom,
    seasonTo: input.options.evaluationSeasonTo,
    sampleSize: result.overallMetrics.sampleSize,
    logLoss: result.overallMetrics.logLoss,
    brierScore: result.overallMetrics.brierScore,
    calibrationError: result.calibration.calibrationError,
    startedAt,
    completedAt: new Date().toISOString(),
    summary: buildBacktestSummaryJson(result),
    metadata: {
      source: "task-17-backtest-engine",
      triggeredBy: input.triggeredBy,
      splitStrategy: result.splitStrategy,
      regressionDetected: result.socialComparison.regressionDetected,
      socialRecommendation: result.socialComparison.recommendation,
      skippedRowCount: result.skippedRows.length,
    },
  });

  return result;
}

function summarizeFold(
  fold: TimeSplitFold,
  calibrationBinCount: number,
): FoldBacktestSummary {
  return {
    testSeason: fold.testSeason,
    trainSampleSize: fold.trainSampleSize,
    testSampleSize: fold.testSampleSize,
    trainThrough: fold.trainThrough,
    testFrom: fold.testFrom,
    testTo: fold.testTo,
    strictChronology: true,
    primaryMetrics: calculateProbabilityMetrics(
      fold.testRows,
      (row) => row.primaryProbability,
      calibrationBinCount,
    ),
    calibration: calculateCalibrationSummary(
      fold.testRows,
      (row) => row.primaryProbability,
      calibrationBinCount,
    ),
    socialComparison: summarizeSocialComparison(
      fold.testRows,
      calibrationBinCount,
    ),
  };
}

function summarizeSocialComparison(
  rows: readonly HistoricalPredictionRow[],
  calibrationBinCount: number,
): SocialComparisonSummary {
  const socialOn = calculateProbabilityMetrics(
    rows,
    (row) => row.socialOnProbability,
    calibrationBinCount,
  );
  const socialOff = calculateProbabilityMetrics(
    rows,
    (row) => row.socialOffProbability,
    calibrationBinCount,
  );
  const socialOnCalibration = calculateCalibrationSummary(
    rows,
    (row) => row.socialOnProbability,
    calibrationBinCount,
  );
  const socialOffCalibration = calculateCalibrationSummary(
    rows,
    (row) => row.socialOffProbability,
    calibrationBinCount,
  );
  const socialSupportedSampleSize = rows.filter(
    (row) => row.socialSupported,
  ).length;
  const socialChangedSampleSize = rows.filter(
    (row) => row.socialOnProbability !== row.socialOffProbability,
  ).length;

  const delta = {
    logLoss: roundTo(socialOn.logLoss - socialOff.logLoss, 6),
    brierScore: roundTo(socialOn.brierScore - socialOff.brierScore, 6),
    calibrationError: roundTo(
      socialOnCalibration.calibrationError -
        socialOffCalibration.calibrationError,
      6,
    ),
  };

  const regressionMetrics: Array<
    "logLoss" | "brierScore" | "calibrationError"
  > = [];
  if (delta.logLoss > METRIC_TOLERANCE) {
    regressionMetrics.push("logLoss");
  }
  if (delta.brierScore > METRIC_TOLERANCE) {
    regressionMetrics.push("brierScore");
  }
  if (delta.calibrationError > METRIC_TOLERANCE) {
    regressionMetrics.push("calibrationError");
  }

  const regressionDetected = regressionMetrics.length > 0;
  const effectDirection = regressionDetected
    ? "regressed"
    : delta.logLoss < -METRIC_TOLERANCE ||
        delta.brierScore < -METRIC_TOLERANCE ||
        delta.calibrationError < -METRIC_TOLERANCE
      ? "improved"
      : "neutral";

  return {
    sampleSize: rows.length,
    socialSupportedSampleSize,
    socialChangedSampleSize,
    socialOn: {
      ...socialOn,
      calibrationError: socialOnCalibration.calibrationError,
    },
    socialOff: {
      ...socialOff,
      calibrationError: socialOffCalibration.calibrationError,
    },
    delta,
    regressionDetected,
    regressionMetrics,
    effectDirection,
    recommendation: regressionDetected
      ? "disable_social_by_default"
      : "keep_social_enabled",
  };
}

function buildBacktestSummaryJson(
  result: HistoricalBacktestResult,
): JsonObject {
  return JSON.parse(
    JSON.stringify({
      modelKey: result.modelKey,
      checkpointType: result.checkpointType,
      evaluationSeasonFrom: result.evaluationSeasonFrom,
      evaluationSeasonTo: result.evaluationSeasonTo,
      splitStrategy: result.splitStrategy,
      totalLoadedRows: result.totalLoadedRows,
      eligibleRowCount: result.eligibleRowCount,
      skippedRowCount: result.skippedRowCount,
      overallMetrics: result.overallMetrics,
      calibration: result.calibration,
      folds: result.folds,
      skippedFolds: result.skippedFolds,
      socialComparison: result.socialComparison,
      skippedRows: result.skippedRows.map((row) => ({
        modelScoreId: row.modelScoreId,
        matchSlug: row.matchSlug,
        season: row.season,
        checkpointType: row.checkpointType,
        snapshotTime: row.snapshotTime,
        reason: row.reason,
        detail: row.detail,
      })),
    }),
  ) as JsonObject;
}

function validateSeasonRange(seasonFrom: number, seasonTo: number): void {
  if (!Number.isInteger(seasonFrom) || !Number.isInteger(seasonTo)) {
    throw new Error("Backtest season bounds must be integers.");
  }

  if (seasonTo < seasonFrom) {
    throw new Error(
      "Backtest seasonTo must be greater than or equal to seasonFrom.",
    );
  }
}

function comparePredictionRows(
  left: HistoricalPredictionRow,
  right: HistoricalPredictionRow,
): number {
  const bySnapshot = left.snapshotTime.localeCompare(right.snapshotTime);
  if (bySnapshot !== 0) {
    return bySnapshot;
  }

  const bySeason = left.season - right.season;
  if (bySeason !== 0) {
    return bySeason;
  }

  return left.modelScoreId - right.modelScoreId;
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
