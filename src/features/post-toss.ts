import {
  parseFeatureRow,
  type CanonicalCheckpoint,
  type FeatureRow,
} from "../domain/checkpoint.js";
import type { JsonObject, JsonValue } from "../domain/primitives.js";
import {
  assertNoMarketOddsFeatures,
  buildBaselinePreMatchFeatureRow,
  createVenueTossDecisionKey,
  type PreMatchFeatureContext,
} from "./pre-match.js";

export const BASELINE_POST_TOSS_FEATURE_SET_VERSION = "baseline_post_toss_v1";

const INNINGS_LEAKAGE_PATTERN =
  /(inningsnumber|battingteamname|bowlingteamname|runs|wickets|overs|targetruns|currentrunrate|requiredrunrate)/i;

export function buildBaselinePostTossFeatureRow(
  checkpoint: CanonicalCheckpoint,
  context: PreMatchFeatureContext,
): FeatureRow {
  if (checkpoint.checkpointType !== "post_toss") {
    throw new Error(
      "Baseline post-toss feature generation requires a post_toss checkpoint.",
    );
  }

  assertNoInningsLeakageInPostTossCheckpoint(checkpoint);

  const baseFeatureRow = buildBaselinePreMatchFeatureRow(
    toSyntheticPreMatchCheckpoint(checkpoint),
    context,
  );

  const tossWinnerTeamName = checkpoint.match.tossWinnerTeamName;
  const tossDecision = checkpoint.match.tossDecision;

  if (tossWinnerTeamName === null || tossDecision === null) {
    throw new Error(
      "post_toss feature generation requires toss winner and toss decision.",
    );
  }

  const tossWinnerIsTeamA =
    tossWinnerTeamName === checkpoint.match.teamAName ? 1 : 0;
  const tossWinnerIsTeamB =
    tossWinnerTeamName === checkpoint.match.teamBName ? 1 : 0;
  const tossDecisionIsBat = tossDecision === "bat" ? 1 : 0;
  const tossDecisionIsBowl = tossDecision === "bowl" ? 1 : 0;
  const venueName = checkpoint.match.venueName ?? "unknown";
  const venueTossDecisionWinRate = readVenueTossDecisionWinRate(
    context.venueTossDecisionWinRate,
    venueName,
    tossDecision,
  );

  const features: JsonObject = {
    ...baseFeatureRow.features,
    tossWinnerTeamName,
    tossDecision,
    tossWinnerIsTeamA,
    tossWinnerIsTeamB,
    tossDecisionIsBat,
    tossDecisionIsBowl,
    venueTossDecisionWinRate,
    tossWinnerDecisionPair: `${
      tossWinnerIsTeamA === 1 ? "team_a" : "team_b"
    }_${tossDecision}`,
    source: {
      baseFeatureSetVersion: baseFeatureRow.featureSetVersion,
      checkpoint: "post_toss",
      tossSignalIncluded: true,
      tossSignalScope: "winner_and_decision_only",
      pointInTimeSnapshotTime: checkpoint.state.snapshotTime,
    },
  };

  assertNoInningsLeakageInFeatures(features);
  assertNoMarketOddsFeatures(features);

  return parseFeatureRow({
    matchSlug: checkpoint.match.matchSlug,
    checkpointType: "post_toss",
    featureSetVersion: BASELINE_POST_TOSS_FEATURE_SET_VERSION,
    generatedAt: checkpoint.state.snapshotTime,
    features,
  });
}

export function assertNoInningsLeakageInPostTossCheckpoint(
  checkpoint: CanonicalCheckpoint,
): void {
  if (checkpoint.checkpointType !== "post_toss") {
    return;
  }

  const state = checkpoint.state;
  if (
    state.inningsNumber !== null ||
    state.battingTeamName !== null ||
    state.bowlingTeamName !== null ||
    state.runs !== null ||
    state.wickets !== null ||
    state.overs !== null ||
    state.targetRuns !== null ||
    state.currentRunRate !== null ||
    state.requiredRunRate !== null
  ) {
    throw new Error(
      "Innings-state leakage is not allowed in post_toss checkpoint scoring.",
    );
  }

  const inningsPayload = checkpoint.state.statePayload["innings"];
  if (
    inningsPayload !== undefined &&
    isRecord(inningsPayload) &&
    inningsPayload["status"] === "available"
  ) {
    throw new Error(
      "Innings-state leakage is not allowed in post_toss checkpoint scoring.",
    );
  }
}

export function assertNoInningsLeakageInFeatures(
  features: JsonObject,
  path = "features",
): void {
  scanForInningsLeakage(features, path);
}

function scanForInningsLeakage(value: JsonValue, path: string): void {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => {
      scanForInningsLeakage(entry, `${path}[${index}]`);
    });
    return;
  }

  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return;
  }

  for (const [key, entry] of Object.entries(value)) {
    if (
      INNINGS_LEAKAGE_PATTERN.test(key) &&
      !isBenignInningsMetadata(path, key, entry)
    ) {
      throw new Error(
        `Innings-state leakage is not allowed in post_toss features (${path}.${key}).`,
      );
    }

    scanForInningsLeakage(entry, `${path}.${key}`);
  }
}

function isBenignInningsMetadata(
  parentPath: string,
  key: string,
  entry: JsonValue,
): boolean {
  if (
    parentPath === "features" &&
    key === "source" &&
    entry !== null &&
    typeof entry === "object"
  ) {
    return true;
  }

  return false;
}

function toSyntheticPreMatchCheckpoint(
  checkpoint: CanonicalCheckpoint,
): CanonicalCheckpoint {
  return {
    checkpointType: "pre_match",
    match: {
      ...checkpoint.match,
      tossWinnerTeamName: null,
      tossDecision: null,
    },
    state: {
      ...checkpoint.state,
      checkpointType: "pre_match",
      inningsNumber: null,
      battingTeamName: null,
      bowlingTeamName: null,
      runs: null,
      wickets: null,
      overs: null,
      targetRuns: null,
      currentRunRate: null,
      requiredRunRate: null,
    },
  };
}

function readVenueTossDecisionWinRate(
  rates: Record<string, number>,
  venueName: string,
  tossDecision: "bat" | "bowl",
): number {
  const key = createVenueTossDecisionKey(venueName, tossDecision);
  const value = rates[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0.5;
  }

  if (value < 0) {
    return 0;
  }

  if (value > 1) {
    return 1;
  }

  return Number(value.toFixed(6));
}

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export type { PreMatchFeatureContext };
