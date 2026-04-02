import { describe, expect, it } from "vitest";

import {
  createWalkForwardSeasonTimeSplits,
  runHistoricalBacktestFromRows,
  type HistoricalPredictionRow,
} from "../../src/backtest/index.js";

describe("historical backtest engine", () => {
  it("creates strict walk-forward season folds without future leakage", () => {
    const rows = createSyntheticRows();

    const result = createWalkForwardSeasonTimeSplits(rows, {
      evaluationSeasonFrom: 2024,
      evaluationSeasonTo: 2025,
      minimumTrainingSamples: 1,
      minimumTestSamples: 1,
    });

    expect(result.skippedFolds).toEqual([
      {
        testSeason: 2024,
        trainSampleSize: 0,
        testSampleSize: 2,
        reason: "insufficient_training_history",
      },
    ]);

    expect(result.folds).toHaveLength(1);
    expect(result.folds[0]?.testSeason).toBe(2025);
    expect(result.folds[0]?.strictChronology).toBe(true);
    expect(result.folds[0]?.trainThrough).toBe("2024-04-03T12:00:00.000Z");
    expect(result.folds[0]?.testFrom).toBe("2025-04-02T12:00:00.000Z");
    expect(
      result.folds[0]?.trainingRows.every((row) => row.season === 2024),
    ).toBe(true);
  });

  it("does not include same-season earlier rows in training folds", () => {
    const rows = [
      createRow({
        modelScoreId: 10,
        season: 2025,
        snapshotTime: "2025-03-01T12:00:00.000Z",
        actualOutcome: 1,
        socialOnProbability: 0.61,
        socialOffProbability: 0.61,
      }),
      ...createSyntheticRows(),
    ];

    const result = createWalkForwardSeasonTimeSplits(rows, {
      evaluationSeasonFrom: 2024,
      evaluationSeasonTo: 2025,
      minimumTrainingSamples: 1,
      minimumTestSamples: 1,
    });

    const fold2025 = result.folds.find((fold) => fold.testSeason === 2025);
    expect(fold2025).toBeDefined();
    expect(fold2025?.trainingRows.every((row) => row.season < 2025)).toBe(true);
  });

  it("computes calibration metrics and flags social regressions explicitly", () => {
    const result = runHistoricalBacktestFromRows(createSyntheticRows(), {
      modelKey: "baseline-pre-match-v1",
      checkpointType: "pre_match",
      evaluationSeasonFrom: 2024,
      evaluationSeasonTo: 2025,
      calibrationBinCount: 4,
      minimumTrainingSamples: 1,
      minimumTestSamples: 1,
    });

    expect(result.folds).toHaveLength(1);
    expect(result.overallMetrics.sampleSize).toBe(2);
    expect(result.calibration.binCount).toBe(4);
    expect(result.socialComparison.regressionDetected).toBe(true);
    expect(result.socialComparison.regressionMetrics).toContain("logLoss");
    expect(result.socialComparison.regressionMetrics).toContain("brierScore");
    expect(result.socialComparison.recommendation).toBe(
      "disable_social_by_default",
    );
    expect(result.socialComparison.socialOff.logLoss).toBeLessThan(
      result.socialComparison.socialOn.logLoss,
    );
    expect(result.socialComparison.socialOff.brierScore).toBeLessThan(
      result.socialComparison.socialOn.brierScore,
    );
  });
});

function createSyntheticRows(): HistoricalPredictionRow[] {
  return [
    createRow({
      modelScoreId: 1,
      season: 2024,
      snapshotTime: "2024-04-01T12:00:00.000Z",
      actualOutcome: 1,
      socialOnProbability: 0.74,
      socialOffProbability: 0.7,
    }),
    createRow({
      modelScoreId: 2,
      season: 2024,
      snapshotTime: "2024-04-03T12:00:00.000Z",
      actualOutcome: 0,
      socialOnProbability: 0.34,
      socialOffProbability: 0.3,
    }),
    createRow({
      modelScoreId: 3,
      season: 2025,
      snapshotTime: "2025-04-02T12:00:00.000Z",
      actualOutcome: 1,
      socialOnProbability: 0.56,
      socialOffProbability: 0.74,
    }),
    createRow({
      modelScoreId: 4,
      season: 2025,
      snapshotTime: "2025-04-04T12:00:00.000Z",
      actualOutcome: 0,
      socialOnProbability: 0.64,
      socialOffProbability: 0.28,
    }),
  ];
}

function createRow(input: {
  modelScoreId: number;
  season: number;
  snapshotTime: string;
  actualOutcome: 0 | 1;
  socialOnProbability: number;
  socialOffProbability: number;
}): HistoricalPredictionRow {
  return {
    modelKey: "baseline-pre-match-v1",
    checkpointType: "pre_match",
    modelScoreId: input.modelScoreId,
    matchSlug: `ipl-${input.season}-match-${input.modelScoreId}`,
    season: input.season,
    snapshotTime: input.snapshotTime,
    actualOutcome: input.actualOutcome,
    positiveClassLabel: "Team A",
    negativeClassLabel: "Team B",
    primaryProbability: input.socialOnProbability,
    socialOnProbability: input.socialOnProbability,
    socialOffProbability: input.socialOffProbability,
    socialSupported: true,
    marketImpliedProbability: null,
    provenance: {
      source: "unit-test",
      modelScoreId: input.modelScoreId,
    },
  };
}
