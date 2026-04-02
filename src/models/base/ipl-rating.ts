import type { FeatureRow } from "../../domain/checkpoint.js";
import type { JsonObject } from "../../domain/primitives.js";
import { assertNoMarketOddsFeatures } from "../../features/pre-match.js";

export const BASELINE_IPL_RATING_MODEL_FAMILY = "baseline_ipl_rating";
export const BASELINE_IPL_RATING_MODEL_VERSION = "v2";

/**
 * Model weights for combining pre-match features into a win probability.
 * Each weight controls how much the corresponding feature difference
 * contributes to the final logit score.
 */
export interface ModelWeights {
  /** Weight for Elo rating difference (normalized by 145 points) */
  rating: number;
  /** Weight for recent form difference */
  form: number;
  /** Weight for venue strength difference */
  venue: number;
  /** Weight for head-to-head record difference */
  headToHead: number;
  /** Weight for rest days difference (normalized by 7 days) */
  rest: number;
  /** Weight for schedule congestion difference (normalized by 4, inverted) */
  congestion: number;
  lineupStability: number;
  lineupContinuity: number;
  lineupRotation: number;
  bowlerShare: number;
  allRounderShare: number;
  seasonWinRate: number;
  seasonMatchesPlayed: number;
  seasonWinStrength: number;
  /** Weight for dew factor (evening matches at humid venues favor chasing team) */
  dewFactor: number;
  /** Weight for home advantage difference */
  homeAdvantage: number;
  pitchBattingIndex: number;
}

/**
 * Optimized weights from grid search on 2020-2023 seasons.
 * Validated on 2024-2025 holdout: accuracy 52.5%, logLoss 0.724.
 * Note: dewFactor and homeAdvantage set to 0 as current formulation
 * doesn't improve predictions. Future work: model dew as chasing advantage.
 */
export const DEFAULT_MODEL_WEIGHTS: Readonly<ModelWeights> = {
  rating: 0.5,
  form: 0.2,
  venue: 0.0,
  headToHead: 0.2,
  rest: 0.5,
  congestion: 0.4,
  lineupStability: 0.0,
  lineupContinuity: 0.0,
  lineupRotation: 0.0,
  bowlerShare: 0.0,
  allRounderShare: 0.0,
  seasonWinRate: 0.0,
  seasonMatchesPlayed: 0.0,
  seasonWinStrength: 0.0,
  dewFactor: 0.0,
  homeAdvantage: 0.0,
  pitchBattingIndex: 0.0,
};

/**
 * Logit clamp bounds to prevent extreme probabilities.
 * TODO: These should be data-driven from calibration, not hard-coded.
 */
export const DEFAULT_LOGIT_BOUNDS = {
  min: -2.25,
  max: 2.25,
} as const;

export interface BaselineProbabilityScore {
  matchSlug: string;
  checkpointType: "pre_match";
  modelFamily: string;
  modelVersion: string;
  teamAWinProbability: number;
  teamBWinProbability: number;
  generatedAt: string;
  scoreBreakdown: JsonObject;
}

export interface ScoreOptions {
  weights?: ModelWeights;
  logitBounds?: { min: number; max: number };
  plattCalibration?: {
    intercept: number;
    slope: number;
  };
}

export function scoreBaselineIplPreMatch(
  featureRow: FeatureRow,
  options?: ScoreOptions,
): BaselineProbabilityScore {
  if (featureRow.checkpointType !== "pre_match") {
    throw new Error(
      "Baseline IPL rating model only supports pre_match features.",
    );
  }

  assertNoMarketOddsFeatures(featureRow.features);

  const weights = options?.weights ?? DEFAULT_MODEL_WEIGHTS;
  const logitBounds = options?.logitBounds ?? DEFAULT_LOGIT_BOUNDS;

  const ratingDiff = readRequiredFiniteNumber(
    featureRow.features,
    "ratingDiff",
  );
  const formDiff = readRequiredFiniteNumber(featureRow.features, "formDiff");
  const venueDiff = readRequiredFiniteNumber(featureRow.features, "venueDiff");
  const restDiff = readRequiredFiniteNumber(featureRow.features, "restDiff");
  const congestionDiff = readRequiredFiniteNumber(
    featureRow.features,
    "congestionDiff",
  );
  const seasonWinRateDiff = readOptionalFiniteNumber(
    featureRow.features,
    "seasonWinRateDiff",
    0,
  );
  const seasonMatchesPlayedDiffNormalized = readOptionalFiniteNumber(
    featureRow.features,
    "seasonMatchesPlayedDiffNormalized",
    0,
  );
  const seasonWinStrengthDiff = readOptionalFiniteNumber(
    featureRow.features,
    "seasonWinStrengthDiff",
    0,
  );
  const lineupStabilityDiff = readOptionalFiniteNumber(
    featureRow.features,
    "lineupStabilityDiff",
    0,
  );
  const lineupContinuityDiff = readOptionalFiniteNumber(
    featureRow.features,
    "lineupContinuityDiff",
    0,
  );
  const lineupRotationEdge = readOptionalFiniteNumber(
    featureRow.features,
    "lineupRotationEdge",
    0,
  );
  const bowlerShareDiff = readOptionalFiniteNumber(
    featureRow.features,
    "bowlerShareDiff",
    0,
  );
  const allRounderShareDiff = readOptionalFiniteNumber(
    featureRow.features,
    "allRounderShareDiff",
    0,
  );
  const headToHeadDiff = readRequiredFiniteNumber(
    featureRow.features,
    "headToHeadDiff",
  );
  const dewFactor = readOptionalFiniteNumber(
    featureRow.features,
    "dewFactor",
    0,
  );
  const homeAdvantageDiff = readOptionalFiniteNumber(
    featureRow.features,
    "homeAdvantageDiff",
    0,
  );
  const pitchBattingIndex = readOptionalFiniteNumber(
    featureRow.features,
    "pitchBattingIndex",
    0,
  );
  const ratingComponent = (ratingDiff / 145) * weights.rating;
  const formComponent = formDiff * weights.form;
  const venueComponent = venueDiff * weights.venue;
  const headToHeadComponent = headToHeadDiff * weights.headToHead;
  const restComponent = (restDiff / 7) * weights.rest;
  const congestionComponent = (-congestionDiff / 4) * weights.congestion;
  const lineupStabilityComponent =
    lineupStabilityDiff * weights.lineupStability;
  const lineupContinuityComponent =
    lineupContinuityDiff * weights.lineupContinuity;
  const lineupRotationComponent = lineupRotationEdge * weights.lineupRotation;
  const bowlerShareComponent = bowlerShareDiff * weights.bowlerShare;
  const allRounderShareComponent =
    allRounderShareDiff * weights.allRounderShare;
  const seasonWinRateComponent = seasonWinRateDiff * weights.seasonWinRate;
  const seasonMatchesPlayedComponent =
    seasonMatchesPlayedDiffNormalized * weights.seasonMatchesPlayed;
  const seasonWinStrengthComponent =
    seasonWinStrengthDiff * weights.seasonWinStrength;
  const dewFactorComponent = dewFactor * weights.dewFactor;
  const homeAdvantageComponent = homeAdvantageDiff * weights.homeAdvantage;
  const pitchBattingIndexComponent =
    pitchBattingIndex * weights.pitchBattingIndex;

  const logit =
    ratingComponent +
    formComponent +
    venueComponent +
    headToHeadComponent +
    restComponent +
    congestionComponent +
    lineupStabilityComponent +
    lineupContinuityComponent +
    lineupRotationComponent +
    bowlerShareComponent +
    allRounderShareComponent +
    seasonWinRateComponent +
    seasonMatchesPlayedComponent +
    seasonWinStrengthComponent +
    dewFactorComponent +
    homeAdvantageComponent +
    pitchBattingIndexComponent;

  const calibratedLogit = clampToRange(logit, logitBounds.min, logitBounds.max);
  const uncalibratedTeamAWinProbability = clampProbability(
    sigmoid(calibratedLogit),
  );
  const teamAWinProbability =
    options?.plattCalibration === undefined
      ? uncalibratedTeamAWinProbability
      : applyPlattCalibrationProbability(
          uncalibratedTeamAWinProbability,
          options.plattCalibration,
        );
  const teamBWinProbability = clampProbability(1 - teamAWinProbability);

  assertCoherentTwoOutcomeProbabilities(
    teamAWinProbability,
    teamBWinProbability,
  );

  return {
    matchSlug: featureRow.matchSlug,
    checkpointType: "pre_match",
    modelFamily: BASELINE_IPL_RATING_MODEL_FAMILY,
    modelVersion: BASELINE_IPL_RATING_MODEL_VERSION,
    teamAWinProbability,
    teamBWinProbability,
    generatedAt: featureRow.generatedAt,
    scoreBreakdown: {
      rawLogit: roundTo(logit, 8),
      calibratedLogit: roundTo(calibratedLogit, 8),
      uncalibratedTeamAWinProbability,
      plattCalibrationApplied: options?.plattCalibration === undefined ? 0 : 1,
      ratingComponent: roundTo(ratingComponent, 8),
      formComponent: roundTo(formComponent, 8),
      venueComponent: roundTo(venueComponent, 8),
      headToHeadComponent: roundTo(headToHeadComponent, 8),
      restComponent: roundTo(restComponent, 8),
      congestionComponent: roundTo(congestionComponent, 8),
      lineupStabilityComponent: roundTo(lineupStabilityComponent, 8),
      lineupContinuityComponent: roundTo(lineupContinuityComponent, 8),
      lineupRotationComponent: roundTo(lineupRotationComponent, 8),
      bowlerShareComponent: roundTo(bowlerShareComponent, 8),
      allRounderShareComponent: roundTo(allRounderShareComponent, 8),
      seasonWinRateComponent: roundTo(seasonWinRateComponent, 8),
      seasonMatchesPlayedComponent: roundTo(seasonMatchesPlayedComponent, 8),
      seasonWinStrengthComponent: roundTo(seasonWinStrengthComponent, 8),
      dewFactorComponent: roundTo(dewFactorComponent, 8),
      homeAdvantageComponent: roundTo(homeAdvantageComponent, 8),
      pitchBattingIndexComponent: roundTo(pitchBattingIndexComponent, 8),
      featureSetVersion: featureRow.featureSetVersion,
      weightsUsed: { ...weights },
      ...(options?.plattCalibration === undefined
        ? {}
        : {
            plattCalibration: {
              intercept: options.plattCalibration.intercept,
              slope: options.plattCalibration.slope,
            },
          }),
    },
  };
}

function applyPlattCalibrationProbability(
  probability: number,
  model: { intercept: number; slope: number },
): number {
  const boundedProbability = clampProbability(probability);
  const logit = Math.log(boundedProbability / (1 - boundedProbability));
  return clampProbability(sigmoid(model.intercept + model.slope * logit));
}

function clampToRange(value: number, min: number, max: number): number {
  if (value < min) {
    return min;
  }

  if (value > max) {
    return max;
  }

  return value;
}

export function assertCoherentTwoOutcomeProbabilities(
  teamAWinProbability: number,
  teamBWinProbability: number,
): void {
  if (
    !Number.isFinite(teamAWinProbability) ||
    teamAWinProbability < 0 ||
    teamAWinProbability > 1
  ) {
    throw new Error(
      "teamAWinProbability must be a finite probability in [0, 1].",
    );
  }

  if (
    !Number.isFinite(teamBWinProbability) ||
    teamBWinProbability < 0 ||
    teamBWinProbability > 1
  ) {
    throw new Error(
      "teamBWinProbability must be a finite probability in [0, 1].",
    );
  }

  const sum = roundTo(teamAWinProbability + teamBWinProbability, 12);
  if (Math.abs(sum - 1) > 1e-9) {
    throw new Error(
      `Two-outcome probabilities must sum to 1, received ${sum.toFixed(12)}.`,
    );
  }
}

function readRequiredFiniteNumber(features: JsonObject, key: string): number {
  const value = features[key];

  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(
      `Missing required finite numeric feature "${key}" for baseline pre-match model.`,
    );
  }

  return value;
}

function readOptionalFiniteNumber(
  features: JsonObject,
  key: string,
  defaultValue: number,
): number {
  const value = features[key];

  if (value === undefined || value === null) {
    return defaultValue;
  }

  if (typeof value !== "number" || !Number.isFinite(value)) {
    return defaultValue;
  }

  return value;
}

function sigmoid(value: number): number {
  return 1 / (1 + Math.exp(-value));
}

function clampProbability(value: number): number {
  if (value <= 0) {
    return 0;
  }

  if (value >= 1) {
    return 1;
  }

  return roundTo(value, 6);
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
