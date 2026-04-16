import type { FeatureRow } from "../../domain/checkpoint.js";
import type { JsonObject } from "../../domain/primitives.js";

import { assertCoherentTwoOutcomeProbabilities } from "./ipl-rating.js";

export const PROFIT_FIRST_MARKET_MODEL_FAMILY = "profit_first_market";
export const PROFIT_FIRST_MARKET_MODEL_VERSION = "v1";

export interface ProfitFirstMarketModelConfig {
  marketWeight: number;
  liquidityBlendBoost: number;
  maxDeviationFromMarket: number;
  midFavoriteFade: number;
  probabilityFloor: number;
  probabilityCeiling: number;
  structuredLogitMin: number;
  structuredLogitMax: number;
  rating: number;
  form: number;
  venue: number;
  headToHead: number;
  rest: number;
  congestion: number;
  seasonWinStrength: number;
  lineupStability: number;
  lineupContinuity: number;
  lineupRotation: number;
  homeAdvantage: number;
}

export const DEFAULT_PROFIT_FIRST_MARKET_MODEL_CONFIG: Readonly<ProfitFirstMarketModelConfig> =
  {
    marketWeight: 0.7,
    liquidityBlendBoost: 0.16,
    maxDeviationFromMarket: 0.08,
    midFavoriteFade: 0.022,
    probabilityFloor: 0.14,
    probabilityCeiling: 0.86,
    structuredLogitMin: -1.1,
    structuredLogitMax: 1.1,
    rating: 0.24,
    form: 0.05,
    venue: 0.06,
    headToHead: 0.04,
    rest: 0.14,
    congestion: 0.09,
    seasonWinStrength: 0.12,
    lineupStability: 0.05,
    lineupContinuity: 0.03,
    lineupRotation: 0.04,
    homeAdvantage: 0.08,
  };

export interface ProfitFirstMarketScore {
  matchSlug: string;
  checkpointType: "pre_match";
  modelFamily: string;
  modelVersion: string;
  teamAWinProbability: number;
  teamBWinProbability: number;
  generatedAt: string;
  scoreBreakdown: JsonObject;
}

export interface ProfitFirstMarketScoreOptions {
  marketTeamAProbability: number;
  marketLiquidity?: number | null;
  config?: ProfitFirstMarketModelConfig;
  plattCalibration?: {
    intercept: number;
    slope: number;
  };
}

export function scoreProfitFirstPreMatch(
  featureRow: FeatureRow,
  options: ProfitFirstMarketScoreOptions,
): ProfitFirstMarketScore {
  if (featureRow.checkpointType !== "pre_match") {
    throw new Error(
      "Profit-first pre-match model only supports pre_match features.",
    );
  }

  const config = options.config ?? DEFAULT_PROFIT_FIRST_MARKET_MODEL_CONFIG;
  const marketTeamAProbability = normalizeProbability(
    options.marketTeamAProbability,
    "marketTeamAProbability",
  );
  const liquidityScore = normalizeLiquidityScore(
    options.marketLiquidity ?? null,
  );

  const ratingDiff = readRequiredFiniteNumber(
    featureRow.features,
    "ratingDiff",
  );
  const formDiff = readRequiredFiniteNumber(featureRow.features, "formDiff");
  const venueDiff = readRequiredFiniteNumber(featureRow.features, "venueDiff");
  const headToHeadDiff = readRequiredFiniteNumber(
    featureRow.features,
    "headToHeadDiff",
  );
  const restDiff = readRequiredFiniteNumber(featureRow.features, "restDiff");
  const congestionDiff = readRequiredFiniteNumber(
    featureRow.features,
    "congestionDiff",
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
  const homeAdvantageDiff = readOptionalFiniteNumber(
    featureRow.features,
    "homeAdvantageDiff",
    0,
  );

  const ratingComponent = (ratingDiff / 145) * config.rating;
  const formComponent = formDiff * config.form;
  const venueComponent = venueDiff * config.venue;
  const headToHeadComponent = headToHeadDiff * config.headToHead;
  const restComponent = (restDiff / 7) * config.rest;
  const congestionComponent = (-congestionDiff / 4) * config.congestion;
  const seasonWinStrengthComponent =
    seasonWinStrengthDiff * config.seasonWinStrength;
  const lineupStabilityComponent = lineupStabilityDiff * config.lineupStability;
  const lineupContinuityComponent =
    lineupContinuityDiff * config.lineupContinuity;
  const lineupRotationComponent = lineupRotationEdge * config.lineupRotation;
  const homeAdvantageComponent = homeAdvantageDiff * config.homeAdvantage;

  const rawStructuredLogit =
    ratingComponent +
    formComponent +
    venueComponent +
    headToHeadComponent +
    restComponent +
    congestionComponent +
    seasonWinStrengthComponent +
    lineupStabilityComponent +
    lineupContinuityComponent +
    lineupRotationComponent +
    homeAdvantageComponent;

  const boundedStructuredLogit = clampToRange(
    rawStructuredLogit,
    config.structuredLogitMin,
    config.structuredLogitMax,
  );
  const structuredTeamAProbability = clampProbability(
    sigmoid(boundedStructuredLogit),
  );
  const marketAnchorWeight = clampToRange(
    config.marketWeight + config.liquidityBlendBoost * liquidityScore,
    0.55,
    0.92,
  );
  const blendedTeamAProbability =
    marketTeamAProbability * marketAnchorWeight +
    structuredTeamAProbability * (1 - marketAnchorWeight);

  const favoriteFadeApplied = computeFavoriteFade({
    marketTeamAProbability,
    structuredTeamAProbability,
    liquidityScore,
    favoriteFade: config.midFavoriteFade,
  });
  const fadedTeamAProbability =
    marketTeamAProbability >= 0.5
      ? blendedTeamAProbability - favoriteFadeApplied
      : blendedTeamAProbability + favoriteFadeApplied;

  const maxDeviationFromMarket =
    config.maxDeviationFromMarket * (1 - 0.25 * liquidityScore);
  const deviationFromMarket = clampToRange(
    fadedTeamAProbability - marketTeamAProbability,
    -maxDeviationFromMarket,
    maxDeviationFromMarket,
  );
  const boundedTeamAProbability = clampProbabilityToBounds(
    marketTeamAProbability + deviationFromMarket,
    config.probabilityFloor,
    config.probabilityCeiling,
  );
  const teamAWinProbability =
    options.plattCalibration === undefined
      ? boundedTeamAProbability
      : applyPlattCalibrationProbability(
          boundedTeamAProbability,
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
    modelFamily: PROFIT_FIRST_MARKET_MODEL_FAMILY,
    modelVersion: PROFIT_FIRST_MARKET_MODEL_VERSION,
    teamAWinProbability,
    teamBWinProbability,
    generatedAt: featureRow.generatedAt,
    scoreBreakdown: {
      marketTeamAProbability,
      marketLiquidity: options.marketLiquidity ?? null,
      liquidityScore: roundTo(liquidityScore, 6),
      marketAnchorWeight: roundTo(marketAnchorWeight, 6),
      rawStructuredLogit: roundTo(rawStructuredLogit, 8),
      boundedStructuredLogit: roundTo(boundedStructuredLogit, 8),
      structuredTeamAProbability,
      blendedTeamAProbability: roundTo(blendedTeamAProbability, 6),
      favoriteFadeApplied: roundTo(favoriteFadeApplied, 6),
      maxDeviationFromMarket: roundTo(maxDeviationFromMarket, 6),
      boundedTeamAProbability,
      uncalibratedTeamAWinProbability: boundedTeamAProbability,
      plattCalibrationApplied: options.plattCalibration === undefined ? 0 : 1,
      componentWeights: { ...config },
      components: {
        ratingComponent: roundTo(ratingComponent, 8),
        formComponent: roundTo(formComponent, 8),
        venueComponent: roundTo(venueComponent, 8),
        headToHeadComponent: roundTo(headToHeadComponent, 8),
        restComponent: roundTo(restComponent, 8),
        congestionComponent: roundTo(congestionComponent, 8),
        seasonWinStrengthComponent: roundTo(seasonWinStrengthComponent, 8),
        lineupStabilityComponent: roundTo(lineupStabilityComponent, 8),
        lineupContinuityComponent: roundTo(lineupContinuityComponent, 8),
        lineupRotationComponent: roundTo(lineupRotationComponent, 8),
        homeAdvantageComponent: roundTo(homeAdvantageComponent, 8),
      },
      ...(options.plattCalibration === undefined
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

function computeFavoriteFade(input: {
  marketTeamAProbability: number;
  structuredTeamAProbability: number;
  liquidityScore: number;
  favoriteFade: number;
}): number {
  const marketFavoriteProbability = Math.max(
    input.marketTeamAProbability,
    1 - input.marketTeamAProbability,
  );
  const bandStrength = computeTriangularBandStrength(
    marketFavoriteProbability,
    0.54,
    0.63,
    0.72,
  );

  if (bandStrength <= 0) {
    return 0;
  }

  const favoriteSupport =
    input.marketTeamAProbability >= 0.5
      ? input.structuredTeamAProbability - 0.5
      : 0.5 - input.structuredTeamAProbability;
  const alignmentRelief = clampToRange(favoriteSupport / 0.12, 0, 1);
  const liquidityAmplifier = 1 - 0.25 * input.liquidityScore;

  return roundTo(
    input.favoriteFade *
      bandStrength *
      liquidityAmplifier *
      (1 - 0.65 * alignmentRelief),
    6,
  );
}

function computeTriangularBandStrength(
  value: number,
  lower: number,
  peak: number,
  upper: number,
): number {
  if (value <= lower || value >= upper) {
    return 0;
  }

  if (value === peak) {
    return 1;
  }

  if (value < peak) {
    return (value - lower) / (peak - lower);
  }

  return (upper - value) / (upper - peak);
}

function normalizeLiquidityScore(value: number | null): number {
  if (value === null || !Number.isFinite(value) || value <= 0) {
    return 0;
  }

  return clampToRange(Math.log10(value + 1) / 4.7, 0, 1);
}

function applyPlattCalibrationProbability(
  probability: number,
  model: { intercept: number; slope: number },
): number {
  const boundedProbability = clampProbability(probability);
  const logit = Math.log(boundedProbability / (1 - boundedProbability));
  return clampProbability(sigmoid(model.intercept + model.slope * logit));
}

function readRequiredFiniteNumber(features: JsonObject, key: string): number {
  const value = features[key];

  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(
      `Missing required finite numeric feature "${key}" for profit-first pre-match model.`,
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

function normalizeProbability(value: number, field: string): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error(`${field} must be a finite probability in [0, 1].`);
  }

  return roundTo(value, 6);
}

function sigmoid(value: number): number {
  return 1 / (1 + Math.exp(-value));
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

function clampProbability(value: number): number {
  return clampProbabilityToBounds(value, 0, 1);
}

function clampProbabilityToBounds(
  value: number,
  min: number,
  max: number,
): number {
  if (value <= min) {
    return roundTo(min, 6);
  }

  if (value >= max) {
    return roundTo(max, 6);
  }

  return roundTo(value, 6);
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
