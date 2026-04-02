import {
  parseValuationResult,
  type CanonicalCheckpoint,
  type ValuationResult,
} from "../domain/checkpoint.js";
import {
  buildInningsBreakFeatureRow,
  type InningsBreakFeatureBuildResult,
  type InningsBreakSkipReason,
} from "../features/innings-break.js";
import type { SocialSignal } from "../social/policy.js";

export const INNINGS_BREAK_SCORER_MODEL_FAMILY = "innings_break_baseline";
export const INNINGS_BREAK_SCORER_MODEL_VERSION = "v1";

export interface InningsBreakScoringInput {
  checkpoint: CanonicalCheckpoint;
  marketImpliedProbability: number | null;
  evaluatedAt?: string;
  socialSignal?: SocialSignal | null;
}

export interface InningsBreakScoringSuccess {
  status: "scored";
  valuation: ValuationResult;
}

export interface InningsBreakScoringSkipped {
  status: "skipped";
  reason: InningsBreakSkipReason;
  detail: string;
}

export type InningsBreakScoringResult =
  | InningsBreakScoringSuccess
  | InningsBreakScoringSkipped;

export function scoreInningsBreakCheckpoint(
  input: InningsBreakScoringInput,
): InningsBreakScoringResult {
  const featureResult = buildInningsBreakFeatureRow(input.checkpoint);
  if (featureResult.status === "skipped") {
    return featureResult;
  }

  const checkpoint = input.checkpoint;
  const state = checkpoint.state;

  if (state.checkpointType !== "innings_break") {
    return {
      status: "skipped",
      reason: "checkpoint_mismatch",
      detail: "innings-break scorer requires innings_break checkpoint state",
    };
  }

  const defendingWinProbability =
    scoreDefendingTeamWinProbability(featureResult);
  const teamABaseWinProbability =
    checkpoint.match.teamAName === state.battingTeamName
      ? defendingWinProbability
      : 1 - defendingWinProbability;

  const socialAdjustment = resolveTeamAAdjustment(
    checkpoint,
    input.socialSignal,
  );
  const fairWinProbability = clampProbability(
    roundTo(teamABaseWinProbability + socialAdjustment, 6),
  );
  const marketImpliedProbability = normalizeMarketProbability(
    input.marketImpliedProbability,
  );
  const edge =
    marketImpliedProbability === null
      ? null
      : roundTo(fairWinProbability - marketImpliedProbability, 6);

  const evaluatedAt = input.evaluatedAt ?? new Date().toISOString();

  return {
    status: "scored",
    valuation: parseValuationResult({
      matchSlug: checkpoint.match.matchSlug,
      checkpointType: "innings_break",
      fairWinProbability,
      marketImpliedProbability,
      edge,
      evaluatedAt,
      valuationPayload: {
        modelFamily: INNINGS_BREAK_SCORER_MODEL_FAMILY,
        modelVersion: INNINGS_BREAK_SCORER_MODEL_VERSION,
        generatedFromFeatureSetVersion:
          featureResult.featureRow.featureSetVersion,
        defendingTeamName: state.battingTeamName,
        battingFirstTeamName: state.battingTeamName,
        teamABaseWinProbability: roundTo(teamABaseWinProbability, 6),
        defendingTeamWinProbability: defendingWinProbability,
        socialAdjustment,
        socialApplied:
          input.socialSignal !== null &&
          input.socialSignal !== undefined &&
          input.socialSignal.boundedAdjustment !== 0,
      },
    }),
  };
}

function scoreDefendingTeamWinProbability(
  featureResult: Extract<InningsBreakFeatureBuildResult, { status: "ready" }>,
): number {
  const features = featureResult.featureRow.features;

  const firstInningsRuns = readRequiredFeature(features, "firstInningsRuns");
  const firstInningsWickets = readRequiredFeature(
    features,
    "firstInningsWickets",
  );
  const firstInningsOvers = readRequiredFeature(features, "firstInningsOvers");
  const runRatePressure = readRequiredFeature(features, "runRatePressure");

  const runsComponent = (firstInningsRuns - 165) / 20;
  const wicketsComponent = (firstInningsWickets - 5) / 3;
  const oversComponent = (firstInningsOvers - 20) / 3;
  const pressureComponent = runRatePressure / 2;

  const defendingLogit =
    runsComponent * 0.85 +
    pressureComponent * 0.75 -
    wicketsComponent * 0.35 +
    oversComponent * 0.15;

  return clampProbability(roundTo(sigmoid(defendingLogit), 6));
}

function resolveTeamAAdjustment(
  checkpoint: CanonicalCheckpoint,
  socialSignal: SocialSignal | null | undefined,
): number {
  if (socialSignal === null || socialSignal === undefined) {
    return 0;
  }

  if (
    socialSignal.matchSlug !== checkpoint.match.matchSlug ||
    socialSignal.checkpointType !== "innings_break"
  ) {
    throw new Error(
      "social signal context must match innings-break scoring checkpoint",
    );
  }

  if (socialSignal.targetTeamName === checkpoint.match.teamAName) {
    return socialSignal.boundedAdjustment;
  }

  if (socialSignal.targetTeamName === checkpoint.match.teamBName) {
    return -socialSignal.boundedAdjustment;
  }

  return 0;
}

function readRequiredFeature(
  features: Record<string, unknown>,
  key: string,
): number {
  const value = features[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Missing required numeric innings-break feature: ${key}`);
  }

  return value;
}

function normalizeMarketProbability(value: number | null): number | null {
  if (value === null) {
    return null;
  }

  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error("marketImpliedProbability must be within [0, 1]");
  }

  return roundTo(value, 6);
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

  return value;
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
