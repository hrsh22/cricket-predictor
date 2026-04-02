import type { CheckpointType } from "../domain/checkpoint.js";
import type { JsonObject } from "../domain/primitives.js";

export interface HistoricalPredictionRow {
  modelKey: string;
  checkpointType: CheckpointType;
  modelScoreId: number;
  matchSlug: string;
  season: number;
  snapshotTime: string;
  actualOutcome: 0 | 1;
  positiveClassLabel: string;
  negativeClassLabel: string;
  primaryProbability: number;
  socialOnProbability: number;
  socialOffProbability: number;
  socialSupported: boolean;
  marketImpliedProbability: number | null;
  provenance: JsonObject;
}

export type HistoricalPredictionSkipReason =
  | "match_not_completed"
  | "unsupported_match_result"
  | "winner_missing"
  | "winner_not_in_binary_outcomes"
  | "winner_not_in_match_teams"
  | "missing_market_outcomes"
  | "invalid_probability_payload";

export interface HistoricalPredictionSkippedRow {
  modelScoreId: number;
  matchSlug: string;
  season: number;
  checkpointType: CheckpointType;
  snapshotTime: string;
  reason: HistoricalPredictionSkipReason;
  detail: string;
}

export interface ProbabilityMetrics {
  sampleSize: number;
  logLoss: number;
  brierScore: number;
  calibrationError: number;
  accuracy: number;
  meanPredictedProbability: number;
  positiveRate: number;
}

export interface CalibrationBinSummary {
  index: number;
  lowerBound: number;
  upperBound: number;
  sampleSize: number;
  averagePredictedProbability: number | null;
  empiricalPositiveRate: number | null;
  absoluteGap: number | null;
}

export interface CalibrationSummary {
  sampleSize: number;
  binCount: number;
  calibrationError: number;
  bins: CalibrationBinSummary[];
}

export interface TimeSplitFold {
  testSeason: number;
  trainSampleSize: number;
  testSampleSize: number;
  trainThrough: string | null;
  testFrom: string;
  testTo: string;
  strictChronology: true;
  trainingRows: HistoricalPredictionRow[];
  testRows: HistoricalPredictionRow[];
}

export interface TimeSplitSkippedFold {
  testSeason: number;
  trainSampleSize: number;
  testSampleSize: number;
  reason: "insufficient_training_history" | "insufficient_test_samples";
}

export interface FoldBacktestSummary {
  testSeason: number;
  trainSampleSize: number;
  testSampleSize: number;
  trainThrough: string | null;
  testFrom: string;
  testTo: string;
  strictChronology: true;
  primaryMetrics: ProbabilityMetrics;
  calibration: CalibrationSummary;
  socialComparison: SocialComparisonSummary;
}

export interface SocialComparisonSummary {
  sampleSize: number;
  socialSupportedSampleSize: number;
  socialChangedSampleSize: number;
  socialOn: ProbabilityMetrics;
  socialOff: ProbabilityMetrics;
  delta: {
    logLoss: number;
    brierScore: number;
    calibrationError: number;
  };
  regressionDetected: boolean;
  regressionMetrics: Array<"logLoss" | "brierScore" | "calibrationError">;
  effectDirection: "improved" | "neutral" | "regressed";
  recommendation: "keep_social_enabled" | "disable_social_by_default";
}

export interface StoredScoreBacktestDataset {
  rows: HistoricalPredictionRow[];
  skippedRows: HistoricalPredictionSkippedRow[];
  totalLoadedRows: number;
}

export interface HistoricalBacktestResult {
  modelKey: string;
  checkpointType: CheckpointType;
  evaluationSeasonFrom: number;
  evaluationSeasonTo: number;
  splitStrategy: "walk_forward_by_season";
  totalLoadedRows: number;
  eligibleRowCount: number;
  skippedRowCount: number;
  overallMetrics: ProbabilityMetrics;
  calibration: CalibrationSummary;
  folds: FoldBacktestSummary[];
  skippedFolds: TimeSplitSkippedFold[];
  socialComparison: SocialComparisonSummary;
  skippedRows: HistoricalPredictionSkippedRow[];
}

export interface HistoricalBacktestOptions {
  modelKey: string;
  checkpointType: CheckpointType;
  evaluationSeasonFrom: number;
  evaluationSeasonTo: number;
  calibrationBinCount?: number;
  minimumTrainingSamples?: number;
  minimumTestSamples?: number;
}
