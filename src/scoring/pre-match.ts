import type { FeatureRow } from "../domain/checkpoint.js";
import type { MarketSnapshot } from "../domain/market.js";
import type { JsonObject } from "../domain/primitives.js";
import {
  BASELINE_IPL_RATING_MODEL_FAMILY,
  BASELINE_IPL_RATING_MODEL_VERSION,
  DEFAULT_MODEL_WEIGHTS,
  scoreBaselineIplPreMatch,
  type ModelWeights,
} from "../models/base/index.js";
import type {
  ModelScoreRecord,
  ModelingRepository,
} from "../repositories/modeling.js";
import type { MarketMatchMappingRecord } from "../repositories/matching.js";
import {
  resolveSocialSignal,
  type SocialSignalCandidate,
  type SocialSignalPolicy,
} from "../social/index.js";

const PRE_MATCH_VALUATION_VERSION = "task14-v1";

export interface PreMatchValuationInput {
  mapping: MarketMatchMappingRecord;
  marketSnapshot: MarketSnapshot;
  featureRow: FeatureRow;
  checkpointStateId: number;
  scoringRunKey: string;
  modelKey: string;
  scoredAt?: string;
  socialCandidate?: SocialSignalCandidate | null;
  socialPolicyOverrides?: Partial<SocialSignalPolicy>;
  modelMetadata?: JsonObject;
}

export interface PreMatchValuationResult {
  matchSlug: string;
  checkpointType: "pre_match";
  checkpointTag: "pre_match";
  scoringRunKey: string;
  modelKey: string;
  modelFamily: string;
  modelVersion: string;
  checkpointStateId: number;
  sourceMarketId: string;
  sourceMarketSnapshotId: number;
  structuredFairProbability: number;
  fairWinProbability: number;
  marketImpliedProbability: number;
  edge: number;
  spread: number;
  socialMode: "enabled" | "disabled";
  socialAdjustmentNote: string;
  scoredAt: string;
  scorePayload: JsonObject;
}

export interface PreMatchPersistedScoreResult {
  valuation: PreMatchValuationResult;
  persistedScore: ModelScoreRecord;
}

export async function scoreAndPersistPreMatchValuation(
  input: PreMatchValuationInput & {
    modelingRepository: ModelingRepository;
  },
): Promise<PreMatchPersistedScoreResult> {
  const valuation = scorePreMatchValuation(input);
  const persistedScore = await input.modelingRepository.saveModelScore({
    checkpointStateId: valuation.checkpointStateId,
    checkpointType: "pre_match",
    scoringRunKey: valuation.scoringRunKey,
    modelKey: valuation.modelKey,
    fairWinProbability: valuation.fairWinProbability,
    marketImpliedProbability: valuation.marketImpliedProbability,
    edge: valuation.edge,
    scoredAt: valuation.scoredAt,
    scorePayload: valuation.scorePayload,
  });

  return {
    valuation,
    persistedScore,
  };
}

export function scorePreMatchValuation(
  input: PreMatchValuationInput,
): PreMatchValuationResult {
  assertPreMatchScorerEligibleMapping(input.mapping);
  assertPreMatchFeatureRow(input.featureRow);

  if (input.mapping.matchSlug !== input.featureRow.matchSlug) {
    throw new Error(
      "Pre-match scoring requires mapping.matchSlug to match featureRow.matchSlug.",
    );
  }

  if (input.mapping.sourceMarketId !== input.marketSnapshot.sourceMarketId) {
    throw new Error(
      "Pre-match scoring requires marketSnapshot.sourceMarketId to match mapping.sourceMarketId.",
    );
  }

  const modelOptions = deriveModelScoreOptionsFromMetadata(input.modelMetadata);
  const baseline = scoreBaselineIplPreMatch(input.featureRow, modelOptions);
  const yesOutcomeName = requireString(input.marketSnapshot.yesOutcomeName, {
    field: "marketSnapshot.yesOutcomeName",
  });
  const noOutcomeName = requireString(input.marketSnapshot.noOutcomeName, {
    field: "marketSnapshot.noOutcomeName",
  });

  const teamAName = readRequiredStringFeature(
    input.featureRow.features,
    "teamAName",
  );
  const teamBName = readRequiredStringFeature(
    input.featureRow.features,
    "teamBName",
  );
  const yesOutcomeSide = resolveOutcomeSide({
    outcomeTeamName: yesOutcomeName,
    teamAName,
    teamBName,
    field: "marketSnapshot.yesOutcomeName",
  });

  const structuredFairProbability =
    yesOutcomeSide === "team_a"
      ? baseline.teamAWinProbability
      : baseline.teamBWinProbability;

  const socialSignal = resolveSocialSignal(
    {
      competition: "IPL",
      matchSlug: input.featureRow.matchSlug,
      checkpointType: "pre_match",
    },
    input.socialCandidate ?? null,
    input.socialPolicyOverrides,
  );

  const socialAdjustment = deriveYesOutcomeAdjustment({
    socialSignal,
    yesOutcomeName,
    noOutcomeName,
  });

  const fairWinProbability = clampProbability(
    structuredFairProbability + socialAdjustment.adjustmentToYesOutcome,
  );
  const marketImpliedProbability = readMarketImpliedProbability(
    input.marketSnapshot.outcomeProbabilities,
  );
  const spread = roundTo(fairWinProbability - marketImpliedProbability, 6);
  const scoredAt = input.scoredAt ?? baseline.generatedAt;
  const socialMode =
    socialSignal.status === "disabled" ? "disabled" : "enabled";

  return {
    matchSlug: input.featureRow.matchSlug,
    checkpointType: "pre_match",
    checkpointTag: "pre_match",
    scoringRunKey: input.scoringRunKey,
    modelKey: input.modelKey,
    modelFamily: BASELINE_IPL_RATING_MODEL_FAMILY,
    modelVersion: BASELINE_IPL_RATING_MODEL_VERSION,
    checkpointStateId: input.checkpointStateId,
    sourceMarketId: input.mapping.sourceMarketId,
    sourceMarketSnapshotId: input.mapping.sourceMarketSnapshotId,
    structuredFairProbability,
    fairWinProbability,
    marketImpliedProbability,
    edge: spread,
    spread,
    socialMode,
    socialAdjustmentNote: socialAdjustment.note,
    scoredAt,
    scorePayload: {
      valuationVersion: PRE_MATCH_VALUATION_VERSION,
      checkpointTag: "pre_match",
      lineage: {
        runKey: input.scoringRunKey,
        modelKey: input.modelKey,
        modelFamily: BASELINE_IPL_RATING_MODEL_FAMILY,
        modelVersion: BASELINE_IPL_RATING_MODEL_VERSION,
        checkpointStateId: input.checkpointStateId,
        sourceMarketId: input.mapping.sourceMarketId,
        sourceMarketSnapshotId: input.mapping.sourceMarketSnapshotId,
        resolverVersion: input.mapping.resolverVersion,
      },
      valuation: {
        structuredFairProbability,
        fairWinProbability,
        marketImpliedProbability,
        edge: spread,
        spread,
      },
      market: {
        yesOutcomeName,
        noOutcomeName,
      },
      socialAdjustment: {
        mode: socialMode,
        status: socialSignal.status,
        reason: socialSignal.reason,
        requestedAdjustment: socialSignal.requestedAdjustment,
        boundedAdjustment: socialSignal.boundedAdjustment,
        appliedToYesOutcome: socialAdjustment.adjustmentToYesOutcome,
        targetTeamName: socialSignal.targetTeamName,
        targetSide: socialAdjustment.targetSide,
        adjustmentCap: socialSignal.adjustmentCap,
        note: socialAdjustment.note,
        sourceProviderKey: socialSignal.source?.providerKey ?? null,
        sourceId: socialSignal.source?.sourceId ?? null,
        summary: socialSignal.summary,
      },
      baseline: {
        teamAWinProbability: baseline.teamAWinProbability,
        teamBWinProbability: baseline.teamBWinProbability,
        scoreBreakdown: baseline.scoreBreakdown,
        modelOptionsApplied: {
          weightsOverridden: modelOptions.weights !== undefined,
          plattCalibrationApplied:
            modelOptions.plattCalibration !== undefined ? 1 : 0,
        },
      },
    },
  };
}

function deriveModelScoreOptionsFromMetadata(
  metadata: JsonObject | undefined,
): {
  weights?: ModelWeights;
  plattCalibration?: { intercept: number; slope: number };
} {
  const preMatchOptions = readObject(metadata?.["preMatchModelOptions"]);
  if (preMatchOptions === null) {
    return {};
  }

  const enabled = preMatchOptions["enabled"];
  if (enabled !== true) {
    return {};
  }

  const platt = readObject(preMatchOptions["plattCalibration"]);
  const plattCalibration =
    platt === null
      ? undefined
      : readPlattCalibrationModel({
          interceptValue: platt["intercept"],
          slopeValue: platt["slope"],
          convergedValue: platt["converged"],
          trainSampleSizeValue: platt["trainSampleSize"],
          methodValue: preMatchOptions["calibrationMethod"],
        });

  const weightsInput = readObject(preMatchOptions["weights"]);
  const weights =
    weightsInput === null
      ? undefined
      : readModelWeightsOverride(weightsInput, DEFAULT_MODEL_WEIGHTS);

  return {
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

function deriveYesOutcomeAdjustment(input: {
  socialSignal: ReturnType<typeof resolveSocialSignal>;
  yesOutcomeName: string;
  noOutcomeName: string;
}): {
  adjustmentToYesOutcome: number;
  targetSide: "yes" | "no" | "unknown";
  note: string;
} {
  const socialSignal = input.socialSignal;

  if (socialSignal.status !== "applied") {
    if (socialSignal.status === "disabled") {
      return {
        adjustmentToYesOutcome: 0,
        targetSide: "unknown",
        note: "Social adjustment disabled by policy.",
      };
    }

    return {
      adjustmentToYesOutcome: 0,
      targetSide: "unknown",
      note: `Social adjustment not applied (${socialSignal.reason}).`,
    };
  }

  const targetTeamName = socialSignal.targetTeamName;
  if (targetTeamName === null) {
    return {
      adjustmentToYesOutcome: 0,
      targetSide: "unknown",
      note: "Social signal applied status had no target team; adjustment ignored.",
    };
  }

  const targetSide = resolveOutcomeTargetSide({
    targetTeamName,
    yesOutcomeName: input.yesOutcomeName,
    noOutcomeName: input.noOutcomeName,
  });

  if (targetSide === "unknown") {
    return {
      adjustmentToYesOutcome: 0,
      targetSide,
      note: "Social target team did not match either market outcome; adjustment ignored.",
    };
  }

  const signedAdjustment =
    targetSide === "yes"
      ? socialSignal.boundedAdjustment
      : -socialSignal.boundedAdjustment;

  return {
    adjustmentToYesOutcome: signedAdjustment,
    targetSide,
    note:
      targetSide === "yes"
        ? "Social adjustment applied toward yes outcome."
        : "Social adjustment applied toward no outcome.",
  };
}

function readMarketImpliedProbability(
  probabilities: Record<string, number>,
): number {
  const yesProbability = probabilities["yes"];

  if (
    typeof yesProbability !== "number" ||
    !Number.isFinite(yesProbability) ||
    yesProbability < 0 ||
    yesProbability > 1
  ) {
    throw new Error(
      'Pre-match scoring requires marketSnapshot.outcomeProbabilities["yes"] in [0, 1].',
    );
  }

  return roundTo(yesProbability, 6);
}

function assertPreMatchScorerEligibleMapping(
  mapping: MarketMatchMappingRecord,
): void {
  if (mapping.mappingStatus !== "resolved") {
    throw new Error("Pre-match scoring requires a resolved market mapping.");
  }

  if (mapping.canonicalMatchId === null || mapping.matchSlug === null) {
    throw new Error(
      "Pre-match scoring requires a mapping with canonicalMatchId and matchSlug.",
    );
  }

  if (mapping.confidence === null) {
    throw new Error(
      "Pre-match scoring requires confidence metadata for resolved mapping.",
    );
  }
}

function assertPreMatchFeatureRow(featureRow: FeatureRow): void {
  if (featureRow.checkpointType !== "pre_match") {
    throw new Error("Pre-match scorer only supports pre_match feature rows.");
  }
}

function resolveOutcomeSide(input: {
  outcomeTeamName: string;
  teamAName: string;
  teamBName: string;
  field: string;
}): "team_a" | "team_b" {
  if (matchesTeam(input.outcomeTeamName, input.teamAName)) {
    return "team_a";
  }

  if (matchesTeam(input.outcomeTeamName, input.teamBName)) {
    return "team_b";
  }

  throw new Error(
    `${input.field} does not match either team in the pre-match feature row.`,
  );
}

function resolveOutcomeTargetSide(input: {
  targetTeamName: string;
  yesOutcomeName: string;
  noOutcomeName: string;
}): "yes" | "no" | "unknown" {
  if (matchesTeam(input.targetTeamName, input.yesOutcomeName)) {
    return "yes";
  }

  if (matchesTeam(input.targetTeamName, input.noOutcomeName)) {
    return "no";
  }

  return "unknown";
}

function readRequiredStringFeature(features: JsonObject, key: string): string {
  const value = features[key];

  if (typeof value !== "string") {
    throw new Error(
      `Missing required string feature "${key}" for pre-match scoring.`,
    );
  }

  return value;
}

function requireString(
  value: string | null,
  context: {
    field: string;
  },
): string {
  if (value === null) {
    throw new Error(`${context.field} is required for pre-match scoring.`);
  }

  return value;
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
