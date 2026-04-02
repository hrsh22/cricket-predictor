import {
  parseFeatureRow,
  parseValuationResult,
  type CanonicalCheckpoint,
  type ValuationResult,
} from "../domain/checkpoint.js";
import type { JsonObject, JsonValue } from "../domain/primitives.js";
import {
  DEFAULT_MODEL_WEIGHTS,
  BASELINE_IPL_RATING_MODEL_FAMILY,
  BASELINE_IPL_RATING_MODEL_VERSION,
  scoreBaselineIplPreMatch,
  type ModelWeights,
} from "../models/base/index.js";
import {
  assertNoInningsLeakageInFeatures,
  assertNoInningsLeakageInPostTossCheckpoint,
  buildBaselinePostTossFeatureRow,
  type PreMatchFeatureContext,
} from "../features/post-toss.js";

export const BASELINE_POST_TOSS_SCORER_VERSION = "baseline_post_toss_v1";

export interface PostTossScoringInput {
  checkpoint: CanonicalCheckpoint;
  featureContext: PreMatchFeatureContext;
  marketImpliedProbability: number | null;
  evaluatedAt?: string;
  modelMetadata?: JsonObject;
}

export interface PostTossScoringResult {
  featureRow: ReturnType<typeof buildBaselinePostTossFeatureRow>;
  valuation: ValuationResult;
  baseFairWinProbability: number;
  tossAdjustment: number;
}

export function scorePostTossValuation(
  input: PostTossScoringInput,
): PostTossScoringResult {
  const checkpoint = input.checkpoint;
  if (checkpoint.checkpointType !== "post_toss") {
    throw new Error("Post-toss scorer requires a post_toss checkpoint.");
  }

  assertNoInningsLeakageInPostTossCheckpoint(checkpoint);

  const featureRow = buildBaselinePostTossFeatureRow(
    checkpoint,
    input.featureContext,
  );
  assertNoInningsLeakageInFeatures(featureRow.features);

  const options = derivePostTossOptionsFromMetadata(input.modelMetadata);
  const baseScore = scoreBaselineIplPreMatch(
    parseFeatureRow({
      matchSlug: featureRow.matchSlug,
      checkpointType: "pre_match",
      featureSetVersion: featureRow.featureSetVersion,
      generatedAt: featureRow.generatedAt,
      features: featureRow.features,
    }),
    {
      ...(options.weights === undefined ? {} : { weights: options.weights }),
      ...(options.plattCalibration === undefined
        ? {}
        : { plattCalibration: options.plattCalibration }),
    },
  );

  const tossAdjustment = computeTossAdjustment(featureRow.features, options);
  const venueTossAdjustment = computeVenueTossAdjustment(
    featureRow.features,
    options,
  );
  const fairWinProbability = clampProbability(
    baseScore.teamAWinProbability + tossAdjustment + venueTossAdjustment,
  );
  const marketImpliedProbability = input.marketImpliedProbability;

  if (
    marketImpliedProbability !== null &&
    (marketImpliedProbability < 0 || marketImpliedProbability > 1)
  ) {
    throw new Error(
      "marketImpliedProbability must be in [0, 1] when provided.",
    );
  }

  const edge =
    marketImpliedProbability === null
      ? null
      : roundTo(fairWinProbability - marketImpliedProbability, 6);

  const evaluatedAt = input.evaluatedAt ?? checkpoint.state.snapshotTime;

  const valuationPayload: JsonObject = {
    modelFamily: BASELINE_IPL_RATING_MODEL_FAMILY,
    modelVersion: BASELINE_IPL_RATING_MODEL_VERSION,
    scorerVersion: BASELINE_POST_TOSS_SCORER_VERSION,
    checkpoint: "post_toss",
    baseFairWinProbability: baseScore.teamAWinProbability,
    finalFairWinProbability: fairWinProbability,
    tossAdjustment,
    venueTossAdjustment,
    tossWinnerTeamName: checkpoint.match.tossWinnerTeamName,
    tossDecision: checkpoint.match.tossDecision,
    socialAdjustmentApplied: false,
    signals: {
      tossOnly: true,
      inningsStateUsed: false,
      leakageGuard: "post_toss_innings_fields_rejected",
    },
    scoreBreakdown: baseScore.scoreBreakdown,
    tossAdjustmentOptions: {
      bowlDecisionStrength: options.bowlDecisionStrength,
      batDecisionStrength: options.batDecisionStrength,
      venueTossStrength: options.venueTossStrength,
      weightsOverridden: options.weights === undefined ? 0 : 1,
      plattCalibrationApplied: options.plattCalibration === undefined ? 0 : 1,
    },
  };

  rejectInningsFieldsInPayload(valuationPayload, "valuationPayload");

  const valuation = parseValuationResult({
    matchSlug: checkpoint.match.matchSlug,
    checkpointType: "post_toss",
    fairWinProbability,
    marketImpliedProbability,
    edge,
    evaluatedAt,
    valuationPayload,
  });

  return {
    featureRow,
    valuation,
    baseFairWinProbability: baseScore.teamAWinProbability,
    tossAdjustment,
  };
}

function computeTossAdjustment(
  features: JsonObject,
  options: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  },
): number {
  const tossWinnerIsTeamA = readBinaryFlag(features, "tossWinnerIsTeamA");
  const tossWinnerIsTeamB = readBinaryFlag(features, "tossWinnerIsTeamB");
  const tossDecisionIsBat = readBinaryFlag(features, "tossDecisionIsBat");
  const tossDecisionIsBowl = readBinaryFlag(features, "tossDecisionIsBowl");

  if (tossWinnerIsTeamA + tossWinnerIsTeamB !== 1) {
    throw new Error(
      "Post-toss features must encode exactly one toss winner side.",
    );
  }
  if (tossDecisionIsBat + tossDecisionIsBowl !== 1) {
    throw new Error(
      "Post-toss features must encode exactly one toss decision.",
    );
  }

  const decisionStrength =
    tossDecisionIsBowl === 1
      ? options.bowlDecisionStrength
      : options.batDecisionStrength;
  const side = tossWinnerIsTeamA === 1 ? 1 : -1;

  return roundTo(side * decisionStrength, 6);
}

function computeVenueTossAdjustment(
  features: JsonObject,
  options: {
    bowlDecisionStrength: number;
    batDecisionStrength: number;
    venueTossStrength: number;
  },
): number {
  const tossWinnerIsTeamA = readBinaryFlag(features, "tossWinnerIsTeamA");
  const tossWinnerIsTeamB = readBinaryFlag(features, "tossWinnerIsTeamB");
  const venueTossDecisionWinRate = readProbabilityFeature(
    features,
    "venueTossDecisionWinRate",
  );

  if (tossWinnerIsTeamA + tossWinnerIsTeamB !== 1) {
    throw new Error(
      "Post-toss features must encode exactly one toss winner side.",
    );
  }

  const centered = venueTossDecisionWinRate - 0.5;
  const side = tossWinnerIsTeamA === 1 ? 1 : -1;
  return roundTo(side * centered * options.venueTossStrength, 6);
}

function readBinaryFlag(features: JsonObject, key: string): 0 | 1 {
  const value = features[key];
  if (value !== 0 && value !== 1) {
    throw new Error(
      `Missing required binary post-toss feature "${key}" (expected 0 or 1).`,
    );
  }

  return value;
}

function readProbabilityFeature(features: JsonObject, key: string): number {
  const value = features[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Missing required probability feature \"${key}\".`);
  }

  if (value < 0 || value > 1) {
    throw new Error(`Feature \"${key}\" must be in [0, 1].`);
  }

  return value;
}

function rejectInningsFieldsInPayload(value: JsonValue, path: string): void {
  const inningsPattern =
    /(inningsnumber|battingteamname|bowlingteamname|runs|wickets|overs|targetruns|currentrunrate|requiredrunrate)/i;

  if (Array.isArray(value)) {
    value.forEach((entry, index) => {
      rejectInningsFieldsInPayload(entry, `${path}[${index}]`);
    });
    return;
  }

  if (value === null || typeof value !== "object") {
    return;
  }

  for (const [key, entry] of Object.entries(value)) {
    if (inningsPattern.test(key)) {
      throw new Error(
        `Innings-state leakage is not allowed in post_toss valuation payload (${path}.${key}).`,
      );
    }

    rejectInningsFieldsInPayload(entry, `${path}.${key}`);
  }
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

function derivePostTossOptionsFromMetadata(metadata: JsonObject | undefined): {
  bowlDecisionStrength: number;
  batDecisionStrength: number;
  venueTossStrength: number;
  weights?: ModelWeights;
  plattCalibration?: { intercept: number; slope: number };
} {
  const postTossOptions = readObject(metadata?.["postTossModelOptions"]);
  if (postTossOptions === null || postTossOptions["enabled"] !== true) {
    return {
      bowlDecisionStrength: 0.015,
      batDecisionStrength: 0.01,
      venueTossStrength: 0.05,
    };
  }

  const bowlDecisionStrength = readFinite(
    postTossOptions["bowlDecisionStrength"],
  );
  const batDecisionStrength = readFinite(
    postTossOptions["batDecisionStrength"],
  );
  const venueTossStrength = readFinite(postTossOptions["venueTossStrength"]);
  const weightsInput = readObject(postTossOptions["weights"]);
  const plattInput = readObject(postTossOptions["plattCalibration"]);
  const runtimeCalibrationEnabled =
    postTossOptions["runtimeCalibrationEnabled"] === true;
  const weights =
    weightsInput === null
      ? undefined
      : readModelWeightsOverride(weightsInput, DEFAULT_MODEL_WEIGHTS);
  const plattCalibration =
    plattInput === null || runtimeCalibrationEnabled !== true
      ? undefined
      : readPlattCalibrationModel({
          interceptValue: plattInput["intercept"],
          slopeValue: plattInput["slope"],
          convergedValue: plattInput["converged"],
          trainSampleSizeValue: plattInput["trainSampleSize"],
          methodValue: postTossOptions["calibrationMethod"],
        });

  return {
    bowlDecisionStrength:
      bowlDecisionStrength === null
        ? 0.015
        : clampTossStrength(bowlDecisionStrength),
    batDecisionStrength:
      batDecisionStrength === null
        ? 0.01
        : clampTossStrength(batDecisionStrength),
    venueTossStrength:
      venueTossStrength === null ? 0.05 : clampTossStrength(venueTossStrength),
    ...(weights === undefined ? {} : { weights }),
    ...(plattCalibration === undefined ? {} : { plattCalibration }),
  };
}

function readObject(value: unknown): Record<string, unknown> | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

function readFinite(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }

  return value;
}

function clampTossStrength(value: number): number {
  if (value < 0) {
    return 0;
  }

  if (value > 0.1) {
    return 0.1;
  }

  return Number(value.toFixed(6));
}

function readPlattCalibrationModel(input: {
  interceptValue: unknown;
  slopeValue: unknown;
  convergedValue: unknown;
  trainSampleSizeValue: unknown;
  methodValue: unknown;
}): { intercept: number; slope: number } | undefined {
  const intercept = readFinite(input.interceptValue);
  const slope = readFinite(input.slopeValue);
  const converged = input.convergedValue;
  const trainSampleSize = input.trainSampleSizeValue;
  const method = input.methodValue;

  if (intercept === null || slope === null) {
    return undefined;
  }

  if (method !== "platt") {
    return undefined;
  }

  if (converged !== true) {
    return undefined;
  }

  if (
    typeof trainSampleSize !== "number" ||
    !Number.isInteger(trainSampleSize) ||
    trainSampleSize < 50
  ) {
    return undefined;
  }

  return {
    intercept,
    slope,
  };
}

function readModelWeightsOverride(
  value: Record<string, unknown>,
  fallback: Readonly<ModelWeights>,
): ModelWeights {
  return {
    rating: readFinite(value["rating"]) ?? fallback.rating,
    form: readFinite(value["form"]) ?? fallback.form,
    venue: readFinite(value["venue"]) ?? fallback.venue,
    headToHead: readFinite(value["headToHead"]) ?? fallback.headToHead,
    rest: readFinite(value["rest"]) ?? fallback.rest,
    congestion: readFinite(value["congestion"]) ?? fallback.congestion,
    dewFactor: readFinite(value["dewFactor"]) ?? fallback.dewFactor,
    homeAdvantage: readFinite(value["homeAdvantage"]) ?? fallback.homeAdvantage,
    pitchBattingIndex:
      readFinite(value["pitchBattingIndex"]) ?? fallback.pitchBattingIndex,
  };
}
