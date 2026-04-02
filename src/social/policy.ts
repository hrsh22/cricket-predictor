import type { CheckpointType } from "../domain/checkpoint.js";

import type { SocialSignalCandidate, SocialSignalSource, SocialSourceQuality } from "./contract.js";

export interface SocialSignalContext {
  competition: "IPL";
  matchSlug: string;
  checkpointType: CheckpointType;
}

export interface SocialSignalPolicy {
  enabled: boolean;
  adjustmentCap: number;
  minimumConfidence: number;
  acceptedSourceQualities: readonly SocialSourceQuality[];
}

export type SocialSignalStatus = "applied" | "disabled" | "fallback";

export type SocialSignalReason =
  | "applied"
  | "clamped_to_cap"
  | "disabled_by_policy"
  | "missing_signal"
  | "empty_summary"
  | "low_confidence"
  | "source_quality_rejected";

export interface SocialSignal {
  competition: "IPL";
  matchSlug: string;
  checkpointType: CheckpointType;
  targetTeamName: string | null;
  source: SocialSignalSource | null;
  summary: string;
  confidence: number;
  requestedAdjustment: number;
  boundedAdjustment: number;
  adjustmentCap: number;
  status: SocialSignalStatus;
  reason: SocialSignalReason;
}

export const defaultSocialSignalPolicy: SocialSignalPolicy = {
  enabled: true,
  adjustmentCap: 0.05,
  minimumConfidence: 0.5,
  acceptedSourceQualities: ["trusted", "mixed"],
};

export function clampSocialAdjustment(requestedAdjustment: number, adjustmentCap: number): number {
  if (!Number.isFinite(requestedAdjustment)) {
    throw new RangeError("requestedAdjustment must be finite");
  }

  if (!Number.isFinite(adjustmentCap) || adjustmentCap < 0 || adjustmentCap > 1) {
    throw new RangeError("adjustmentCap must be between 0 and 1");
  }

  if (requestedAdjustment > adjustmentCap) {
    return adjustmentCap;
  }

  if (requestedAdjustment < -adjustmentCap) {
    return -adjustmentCap;
  }

  return requestedAdjustment;
}

export function resolveSocialSignal(
  context: SocialSignalContext,
  candidate: SocialSignalCandidate | null | undefined,
  policyOverrides?: Partial<SocialSignalPolicy>,
): SocialSignal {
  const policy = resolvePolicy(policyOverrides);

  if (candidate !== null && candidate !== undefined) {
    assertMatchingContext(context, candidate);
  }

  if (!policy.enabled) {
    return createNeutralSocialSignal(context, policy, {
      targetTeamName: candidate?.targetTeamName ?? null,
      source: candidate?.source ?? null,
      summary: normalizeSummary(candidate?.summary, "Social layer disabled by policy."),
      confidence: candidate?.confidence ?? 0,
      requestedAdjustment: candidate?.requestedAdjustment ?? 0,
      status: "disabled",
      reason: "disabled_by_policy",
    });
  }

  if (candidate === null || candidate === undefined) {
    return createNeutralSocialSignal(context, policy, {
      targetTeamName: null,
      source: null,
      summary: "No social signal available.",
      confidence: 0,
      requestedAdjustment: 0,
      status: "fallback",
      reason: "missing_signal",
    });
  }

  const summary = candidate.summary.trim();

  if (summary.length === 0) {
    return createNeutralSocialSignal(context, policy, {
      targetTeamName: candidate.targetTeamName,
      source: candidate.source,
      summary: "Social signal summary was empty.",
      confidence: candidate.confidence,
      requestedAdjustment: candidate.requestedAdjustment,
      status: "fallback",
      reason: "empty_summary",
    });
  }

  if (!policy.acceptedSourceQualities.includes(candidate.source.sourceQuality)) {
    return createNeutralSocialSignal(context, policy, {
      targetTeamName: candidate.targetTeamName,
      source: candidate.source,
      summary,
      confidence: candidate.confidence,
      requestedAdjustment: candidate.requestedAdjustment,
      status: "fallback",
      reason: "source_quality_rejected",
    });
  }

  if (candidate.confidence < policy.minimumConfidence) {
    return createNeutralSocialSignal(context, policy, {
      targetTeamName: candidate.targetTeamName,
      source: candidate.source,
      summary,
      confidence: candidate.confidence,
      requestedAdjustment: candidate.requestedAdjustment,
      status: "fallback",
      reason: "low_confidence",
    });
  }

  const boundedAdjustment = clampSocialAdjustment(candidate.requestedAdjustment, policy.adjustmentCap);

  return {
    competition: "IPL",
    matchSlug: context.matchSlug,
    checkpointType: context.checkpointType,
    targetTeamName: candidate.targetTeamName,
    source: candidate.source,
    summary,
    confidence: candidate.confidence,
    requestedAdjustment: candidate.requestedAdjustment,
    boundedAdjustment,
    adjustmentCap: policy.adjustmentCap,
    status: "applied",
    reason: boundedAdjustment === candidate.requestedAdjustment ? "applied" : "clamped_to_cap",
  };
}

function createNeutralSocialSignal(
  context: SocialSignalContext,
  policy: SocialSignalPolicy,
  overrides: {
    targetTeamName: string | null;
    source: SocialSignalSource | null;
    summary: string;
    confidence: number;
    requestedAdjustment: number;
    status: SocialSignalStatus;
    reason: SocialSignalReason;
  },
): SocialSignal {
  return {
    competition: "IPL",
    matchSlug: context.matchSlug,
    checkpointType: context.checkpointType,
    targetTeamName: overrides.targetTeamName,
    source: overrides.source,
    summary: overrides.summary,
    confidence: overrides.confidence,
    requestedAdjustment: overrides.requestedAdjustment,
    boundedAdjustment: 0,
    adjustmentCap: policy.adjustmentCap,
    status: overrides.status,
    reason: overrides.reason,
  };
}

function resolvePolicy(policyOverrides?: Partial<SocialSignalPolicy>): SocialSignalPolicy {
  const adjustmentCap = policyOverrides?.adjustmentCap ?? defaultSocialSignalPolicy.adjustmentCap;
  const minimumConfidence = policyOverrides?.minimumConfidence ?? defaultSocialSignalPolicy.minimumConfidence;
  const acceptedSourceQualities =
    policyOverrides?.acceptedSourceQualities ?? defaultSocialSignalPolicy.acceptedSourceQualities;

  if (!Number.isFinite(adjustmentCap) || adjustmentCap < 0 || adjustmentCap > 1) {
    throw new RangeError("adjustmentCap must be between 0 and 1");
  }

  if (!Number.isFinite(minimumConfidence) || minimumConfidence < 0 || minimumConfidence > 1) {
    throw new RangeError("minimumConfidence must be between 0 and 1");
  }

  return {
    enabled: policyOverrides?.enabled ?? defaultSocialSignalPolicy.enabled,
    adjustmentCap,
    minimumConfidence,
    acceptedSourceQualities,
  };
}

function normalizeSummary(summary: string | undefined, fallbackSummary: string): string {
  const normalized = summary?.trim();

  return normalized === undefined || normalized.length === 0 ? fallbackSummary : normalized;
}

function assertMatchingContext(context: SocialSignalContext, candidate: SocialSignalCandidate): void {
  if (context.matchSlug !== candidate.matchSlug) {
    throw new RangeError("social signal candidate matchSlug must match the scoring context");
  }

  if (context.checkpointType !== candidate.checkpointType) {
    throw new RangeError("social signal candidate checkpointType must match the scoring context");
  }
}
