import {
  collectUnknownKeys,
  DomainValidationError,
  parseEnumValue,
  parseJsonObject,
  parseNullableFiniteNumber,
  parseNullableNonNegativeInteger,
  parseNullablePositiveInteger,
  parseNullableProbability,
  parseNullableString,
  parsePositiveInteger,
  parseProbability,
  parseRecord,
  parseString,
  parseTimestamptzString,
  rejectStateLeakageFlags,
  type JsonObject,
  type ValidationIssue,
} from "./primitives.js";
import { assertPredictableMatch, parseCanonicalMatch, type CanonicalMatch, type MatchStatus } from "./match.js";

export type CheckpointType = "pre_match" | "post_toss" | "innings_break";

export interface CheckpointStateBase {
  matchSlug: string;
  checkpointType: CheckpointType;
  snapshotTime: string;
  stateVersion: number;
  sourceMarketSnapshotId: number | null;
  sourceCricketSnapshotId: number | null;
  inningsNumber: number | null;
  battingTeamName: string | null;
  bowlingTeamName: string | null;
  runs: number | null;
  wickets: number | null;
  overs: number | null;
  targetRuns: number | null;
  currentRunRate: number | null;
  requiredRunRate: number | null;
  statePayload: JsonObject;
}

export interface PreMatchCheckpointState extends CheckpointStateBase {
  checkpointType: "pre_match";
  inningsNumber: null;
  battingTeamName: null;
  bowlingTeamName: null;
  runs: null;
  wickets: null;
  overs: null;
  targetRuns: null;
  currentRunRate: null;
  requiredRunRate: null;
}

export interface PostTossCheckpointState extends CheckpointStateBase {
  checkpointType: "post_toss";
  inningsNumber: null;
  battingTeamName: null;
  bowlingTeamName: null;
  runs: null;
  wickets: null;
  overs: null;
  targetRuns: null;
  currentRunRate: null;
  requiredRunRate: null;
}

export interface InningsBreakCheckpointState extends CheckpointStateBase {
  checkpointType: "innings_break";
  inningsNumber: 1 | 2;
  battingTeamName: string;
  bowlingTeamName: string;
  runs: number;
  wickets: number;
  overs: number;
  targetRuns: number;
  currentRunRate: number;
  requiredRunRate: number;
}

export type CheckpointState = PreMatchCheckpointState | PostTossCheckpointState | InningsBreakCheckpointState;

export interface CanonicalCheckpoint {
  checkpointType: CheckpointType;
  match: CanonicalMatch;
  state: CheckpointState;
}

export interface FeatureRow {
  matchSlug: string;
  checkpointType: CheckpointType;
  featureSetVersion: string;
  generatedAt: string;
  features: JsonObject;
}

export interface ModelScore {
  matchSlug: string;
  checkpointType: CheckpointType;
  scoringRunKey: string;
  modelKey: string;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  edge: number | null;
  scoredAt: string;
  scorePayload: JsonObject;
}

export interface ValuationResult {
  matchSlug: string;
  checkpointType: CheckpointType;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  edge: number | null;
  evaluatedAt: string;
  valuationPayload: JsonObject;
}

const checkpointTypes = ["pre_match", "post_toss", "innings_break"] as const;
const allowedMatchStatusesByCheckpoint: Record<CheckpointType, readonly MatchStatus[]> = {
  pre_match: ["scheduled"],
  post_toss: ["scheduled", "in_progress"],
  innings_break: ["in_progress"],
};

function parseCheckpointState(value: unknown): CheckpointState {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "state", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "matchSlug",
      "checkpointType",
      "snapshotTime",
      "stateVersion",
      "sourceMarketSnapshotId",
      "sourceCricketSnapshotId",
      "inningsNumber",
      "battingTeamName",
      "bowlingTeamName",
      "runs",
      "wickets",
      "overs",
      "targetRuns",
      "currentRunRate",
      "requiredRunRate",
      "statePayload",
    ]),
  );

  const matchSlug = parseString(record["matchSlug"], "state.matchSlug", issues);
  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "state.checkpointType", issues);
  const snapshotTime = parseTimestamptzString(record["snapshotTime"], "state.snapshotTime", issues);
  const stateVersion = parsePositiveInteger(record["stateVersion"], "state.stateVersion", issues);
  const sourceMarketSnapshotId = parseNullableSourceSnapshotId(
    record["sourceMarketSnapshotId"],
    "state.sourceMarketSnapshotId",
    issues,
  );
  const sourceCricketSnapshotId = parseNullableSourceSnapshotId(
    record["sourceCricketSnapshotId"],
    "state.sourceCricketSnapshotId",
    issues,
  );
  const inningsNumber = parseNullablePositiveInteger(record["inningsNumber"], "state.inningsNumber", issues);
  const battingTeamName = parseNullableString(record["battingTeamName"], "state.battingTeamName", issues);
  const bowlingTeamName = parseNullableString(record["bowlingTeamName"], "state.bowlingTeamName", issues);
  const runs = parseNullableNonNegativeInteger(record["runs"], "state.runs", issues);
  const wickets = parseNullableNonNegativeInteger(record["wickets"], "state.wickets", issues);
  const overs = parseNullableFiniteNumber(record["overs"], "state.overs", issues);
  const targetRuns = parseNullableNonNegativeInteger(record["targetRuns"], "state.targetRuns", issues);
  const currentRunRate = parseNullableFiniteNumber(record["currentRunRate"], "state.currentRunRate", issues);
  const requiredRunRate = parseNullableFiniteNumber(record["requiredRunRate"], "state.requiredRunRate", issues);
  const statePayload = parseJsonObject(record["statePayload"], "state.statePayload", issues);

  if (statePayload === null) {
    throw new DomainValidationError(issues);
  }

  rejectStateLeakageFlags(statePayload, "state.statePayload", issues);

  if (
    issues.length > 0 ||
    matchSlug === null ||
    checkpointType === null ||
    snapshotTime === null ||
    stateVersion === null
  ) {
    throw new DomainValidationError(issues);
  }

  if (checkpointType === "pre_match" || checkpointType === "post_toss") {
    if (inningsNumber !== null || battingTeamName !== null || bowlingTeamName !== null || runs !== null || wickets !== null || overs !== null || targetRuns !== null || currentRunRate !== null || requiredRunRate !== null) {
      issues.push({ path: "state", message: `${checkpointType} state cannot contain innings data` });
    }

    if (issues.length > 0) {
      throw new DomainValidationError(issues);
    }

    return {
      matchSlug,
      checkpointType,
      snapshotTime,
      stateVersion,
      sourceMarketSnapshotId,
      sourceCricketSnapshotId,
      inningsNumber: null,
      battingTeamName: null,
      bowlingTeamName: null,
      runs: null,
      wickets: null,
      overs: null,
      targetRuns: null,
      currentRunRate: null,
      requiredRunRate: null,
      statePayload,
    };
  }

  if (inningsNumber === null || battingTeamName === null || bowlingTeamName === null || runs === null || wickets === null || overs === null || targetRuns === null || currentRunRate === null || requiredRunRate === null) {
    issues.push({ path: "state", message: "innings_break state requires complete innings data" });
    throw new DomainValidationError(issues);
  }

  if (inningsNumber !== 1 && inningsNumber !== 2) {
    issues.push({ path: "state.inningsNumber", message: "must be 1 or 2" });
  }

  if (issues.length > 0) {
    throw new DomainValidationError(issues);
  }

  return {
    matchSlug,
    checkpointType,
    snapshotTime,
    stateVersion,
    sourceMarketSnapshotId,
    sourceCricketSnapshotId,
    inningsNumber: inningsNumber as 1 | 2,
    battingTeamName,
    bowlingTeamName,
    runs,
    wickets,
    overs,
    targetRuns,
    currentRunRate,
    requiredRunRate,
    statePayload,
  };
}

function parseNullableSourceSnapshotId(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): number | null {
  if (value === null) {
    return null;
  }

  return parsePositiveInteger(value, path, issues);
}

export function parseCanonicalCheckpoint(value: unknown): CanonicalCheckpoint {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "checkpoint", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(...collectUnknownKeys(record, ["checkpointType", "match", "state"]));

  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "checkpoint.checkpointType", issues);
  const match = parseCanonicalMatch(record["match"]);
  const state = parseCheckpointState(record["state"]);

  if (checkpointType === null) {
    throw new DomainValidationError(issues);
  }

  if (state.checkpointType !== checkpointType) {
    issues.push({ path: "checkpoint.state.checkpointType", message: "must match checkpointType" });
  }

  if (state.matchSlug !== match.matchSlug) {
    issues.push({ path: "checkpoint.state.matchSlug", message: "must match match.matchSlug" });
  }

  assertPredictableMatch(match);

  const allowedStatuses = allowedMatchStatusesByCheckpoint[checkpointType];
  if (!allowedStatuses.includes(match.status)) {
    issues.push({ path: "checkpoint.match.status", message: `must be one of: ${allowedStatuses.join(", ")}` });
  }

  if (checkpointType === "pre_match") {
    if (match.tossWinnerTeamName !== null || match.tossDecision !== null || match.winningTeamName !== null || match.resultType !== null) {
      issues.push({ path: "checkpoint.match", message: "pre_match cannot include toss or result data" });
    }
  }

  if (checkpointType === "post_toss" || checkpointType === "innings_break") {
    if (match.tossWinnerTeamName === null || match.tossDecision === null) {
      issues.push({ path: "checkpoint.match", message: "toss information is required after the toss" });
    }
  }

  if (checkpointType === "innings_break") {
    if (state.inningsNumber === null || state.battingTeamName === null || state.bowlingTeamName === null || state.runs === null || state.wickets === null || state.overs === null || state.targetRuns === null || state.currentRunRate === null || state.requiredRunRate === null) {
      issues.push({ path: "checkpoint.state", message: "innings_break must include complete innings state" });
    }
  }

  const stateFlags = state.statePayload;
  if (stateFlags["dlsApplied"] === true || stateFlags["noResult"] === true || stateFlags["superOver"] === true || stateFlags["incomplete"] === true) {
    issues.push({ path: "checkpoint.state.statePayload", message: "future-only or incomplete state is not allowed" });
  }

  if (issues.length > 0) {
    throw new DomainValidationError(issues);
  }

  return { checkpointType, match, state };
}

export function parseFeatureRow(value: unknown): FeatureRow {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "featureRow", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, ["matchSlug", "checkpointType", "featureSetVersion", "generatedAt", "features"]),
  );

  const matchSlug = parseString(record["matchSlug"], "featureRow.matchSlug", issues);
  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "featureRow.checkpointType", issues);
  const featureSetVersion = parseString(record["featureSetVersion"], "featureRow.featureSetVersion", issues);
  const generatedAt = parseTimestamptzString(record["generatedAt"], "featureRow.generatedAt", issues);
  const features = parseJsonObject(record["features"], "featureRow.features", issues);

  if (issues.length > 0 || matchSlug === null || checkpointType === null || featureSetVersion === null || generatedAt === null || features === null) {
    throw new DomainValidationError(issues);
  }

  return { matchSlug, checkpointType, featureSetVersion, generatedAt, features };
}

export function parseModelScore(value: unknown): ModelScore {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "modelScore", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "matchSlug",
      "checkpointType",
      "scoringRunKey",
      "modelKey",
      "fairWinProbability",
      "marketImpliedProbability",
      "edge",
      "scoredAt",
      "scorePayload",
    ]),
  );

  const matchSlug = parseString(record["matchSlug"], "modelScore.matchSlug", issues);
  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "modelScore.checkpointType", issues);
  const scoringRunKey = parseString(record["scoringRunKey"], "modelScore.scoringRunKey", issues);
  const modelKey = parseString(record["modelKey"], "modelScore.modelKey", issues);
  const fairWinProbability = parseProbability(record["fairWinProbability"], "modelScore.fairWinProbability", issues);
  const marketImpliedProbability = parseNullableProbability(
    record["marketImpliedProbability"],
    "modelScore.marketImpliedProbability",
    issues,
  );
  const edge = parseNullableFiniteNumber(record["edge"], "modelScore.edge", issues);
  const scoredAt = parseTimestamptzString(record["scoredAt"], "modelScore.scoredAt", issues);
  const scorePayload = parseJsonObject(record["scorePayload"], "modelScore.scorePayload", issues);

  if (issues.length > 0 || matchSlug === null || checkpointType === null || scoringRunKey === null || modelKey === null || fairWinProbability === null || scoredAt === null || scorePayload === null) {
    throw new DomainValidationError(issues);
  }

  return { matchSlug, checkpointType, scoringRunKey, modelKey, fairWinProbability, marketImpliedProbability, edge, scoredAt, scorePayload };
}

export function parseValuationResult(value: unknown): ValuationResult {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "valuationResult", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "matchSlug",
      "checkpointType",
      "fairWinProbability",
      "marketImpliedProbability",
      "edge",
      "evaluatedAt",
      "valuationPayload",
    ]),
  );

  const matchSlug = parseString(record["matchSlug"], "valuationResult.matchSlug", issues);
  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "valuationResult.checkpointType", issues);
  const fairWinProbability = parseProbability(record["fairWinProbability"], "valuationResult.fairWinProbability", issues);
  const marketImpliedProbability = parseNullableProbability(
    record["marketImpliedProbability"],
    "valuationResult.marketImpliedProbability",
    issues,
  );
  const edge = parseNullableFiniteNumber(record["edge"], "valuationResult.edge", issues);
  const evaluatedAt = parseTimestamptzString(record["evaluatedAt"], "valuationResult.evaluatedAt", issues);
  const valuationPayload = parseJsonObject(record["valuationPayload"], "valuationResult.valuationPayload", issues);

  if (issues.length > 0 || matchSlug === null || checkpointType === null || fairWinProbability === null || evaluatedAt === null || valuationPayload === null) {
    throw new DomainValidationError(issues);
  }

  return { matchSlug, checkpointType, fairWinProbability, marketImpliedProbability, edge, evaluatedAt, valuationPayload };
}

export type { CanonicalMatch } from "./match.js";
