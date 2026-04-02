import {
  parseFeatureRow,
  type CanonicalCheckpoint,
  type FeatureRow,
} from "../domain/checkpoint.js";
import { isRecord, type JsonObject } from "../domain/primitives.js";

export const INNINGS_BREAK_FEATURE_SET_VERSION = "innings_break_v1";

export type InningsBreakSkipReason =
  | "checkpoint_mismatch"
  | "unsupported_innings_number"
  | "unsupported_provider_coverage"
  | "incomplete_state"
  | "abbreviated_innings_not_supported";

export interface InningsBreakFeatureReady {
  status: "ready";
  featureRow: FeatureRow;
}

export interface InningsBreakFeatureSkipped {
  status: "skipped";
  reason: InningsBreakSkipReason;
  detail: string;
}

export type InningsBreakFeatureBuildResult =
  | InningsBreakFeatureReady
  | InningsBreakFeatureSkipped;

interface CoverageFlags {
  dlsApplied: boolean;
  reducedOvers: boolean;
  noResult: boolean;
  superOver: boolean;
  incomplete: boolean;
}

export function buildInningsBreakFeatureRow(
  checkpoint: CanonicalCheckpoint,
): InningsBreakFeatureBuildResult {
  if (
    checkpoint.checkpointType !== "innings_break" ||
    checkpoint.state.checkpointType !== "innings_break"
  ) {
    return {
      status: "skipped",
      reason: "checkpoint_mismatch",
      detail:
        "innings-break feature builder only supports innings_break checkpoints",
    };
  }

  if (checkpoint.state.inningsNumber !== 1) {
    return {
      status: "skipped",
      reason: "unsupported_innings_number",
      detail:
        "innings-break scoring is supported only for completed first-innings state",
    };
  }

  const statePayload = checkpoint.state.statePayload;
  const coverage = readCoverageFlags(statePayload);

  if (
    coverage.dlsApplied ||
    coverage.reducedOvers ||
    coverage.noResult ||
    coverage.superOver
  ) {
    return {
      status: "skipped",
      reason: "unsupported_provider_coverage",
      detail:
        "DLS/reduced-over/no-result/super-over innings-break states are skipped",
    };
  }

  if (
    coverage.incomplete ||
    hasNonAvailableStatePayload(statePayload, "innings") ||
    hasNonAvailableStatePayload(statePayload, "toss")
  ) {
    return {
      status: "skipped",
      reason: "incomplete_state",
      detail:
        "innings-break scoring requires complete toss and innings payload state",
    };
  }

  if (checkpoint.state.overs < 20 && checkpoint.state.wickets < 10) {
    return {
      status: "skipped",
      reason: "abbreviated_innings_not_supported",
      detail:
        "abbreviated first-innings states without all-out completion are skipped",
    };
  }

  const wicketsRemaining = Math.max(0, 10 - checkpoint.state.wickets);
  const remainingRunsFromPar170 = checkpoint.state.targetRuns - 1 - 170;
  const runRatePressure =
    checkpoint.state.requiredRunRate - checkpoint.state.currentRunRate;

  const features: JsonObject = {
    battingTeamName: checkpoint.state.battingTeamName,
    bowlingTeamName: checkpoint.state.bowlingTeamName,
    firstInningsRuns: checkpoint.state.runs,
    firstInningsWickets: checkpoint.state.wickets,
    firstInningsOvers: checkpoint.state.overs,
    targetRuns: checkpoint.state.targetRuns,
    firstInningsCurrentRunRate: checkpoint.state.currentRunRate,
    secondInningsRequiredRunRateAtStart: checkpoint.state.requiredRunRate,
    wicketsRemaining,
    remainingRunsFromPar170,
    runRatePressure,
    source: {
      featureScope: "innings_break_structured",
      cricketOnlyInputs: true,
      requiresCompletedFirstInnings: true,
    },
  };

  return {
    status: "ready",
    featureRow: parseFeatureRow({
      matchSlug: checkpoint.match.matchSlug,
      checkpointType: "innings_break",
      featureSetVersion: INNINGS_BREAK_FEATURE_SET_VERSION,
      generatedAt: checkpoint.state.snapshotTime,
      features,
    }),
  };
}

function readCoverageFlags(payload: JsonObject): CoverageFlags {
  const rawCoverage = payload["coverage"];
  if (!isRecord(rawCoverage)) {
    return {
      dlsApplied: false,
      reducedOvers: false,
      noResult: false,
      superOver: false,
      incomplete: false,
    };
  }

  return {
    dlsApplied: rawCoverage["dlsApplied"] === true,
    reducedOvers: rawCoverage["reducedOvers"] === true,
    noResult: rawCoverage["noResult"] === true,
    superOver: rawCoverage["superOver"] === true,
    incomplete: rawCoverage["incomplete"] === true,
  };
}

function hasNonAvailableStatePayload(
  payload: JsonObject,
  key: "innings" | "toss",
): boolean {
  const block = payload[key];
  if (!isRecord(block)) {
    return true;
  }

  return block["status"] !== "available";
}
