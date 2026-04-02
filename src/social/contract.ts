import type { CheckpointType } from "../domain/checkpoint.js";
import {
  collectUnknownKeys,
  DomainValidationError,
  parseEnumValue,
  parseFiniteNumber,
  parseNullableString,
  parseProbability,
  parseRecord,
  parseString,
  parseTimestamptzString,
  type ValidationIssue,
} from "../domain/primitives.js";

export type SocialSourceType = "analyst_note" | "news_report" | "team_update" | "market_chatter";

export type SocialSourceQuality = "trusted" | "mixed" | "noisy";

export interface SocialSignalSource {
  providerKey: string;
  sourceType: SocialSourceType;
  sourceId: string;
  sourceLabel: string;
  sourceQuality: SocialSourceQuality;
  capturedAt: string;
  publishedAt: string | null;
  provenanceUrl: string | null;
}

export interface SocialSignalCandidate {
  competition: "IPL";
  matchSlug: string;
  checkpointType: CheckpointType;
  targetTeamName: string;
  source: SocialSignalSource;
  summary: string;
  confidence: number;
  requestedAdjustment: number;
}

const checkpointTypes = ["pre_match", "post_toss", "innings_break"] as const;
const socialSourceTypes = ["analyst_note", "news_report", "team_update", "market_chatter"] as const;
const socialSourceQualities = ["trusted", "mixed", "noisy"] as const;

export function parseSocialSignalSource(value: unknown): SocialSignalSource {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "socialSignalSource", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "providerKey",
      "sourceType",
      "sourceId",
      "sourceLabel",
      "sourceQuality",
      "capturedAt",
      "publishedAt",
      "provenanceUrl",
    ]),
  );

  const providerKey = parseString(record["providerKey"], "socialSignalSource.providerKey", issues);
  const sourceType = parseEnumValue(record["sourceType"], socialSourceTypes, "socialSignalSource.sourceType", issues);
  const sourceId = parseString(record["sourceId"], "socialSignalSource.sourceId", issues);
  const sourceLabel = parseString(record["sourceLabel"], "socialSignalSource.sourceLabel", issues);
  const sourceQuality = parseEnumValue(
    record["sourceQuality"],
    socialSourceQualities,
    "socialSignalSource.sourceQuality",
    issues,
  );
  const capturedAt = parseTimestamptzString(record["capturedAt"], "socialSignalSource.capturedAt", issues);
  const publishedAt = parseNullableString(record["publishedAt"], "socialSignalSource.publishedAt", issues);
  const provenanceUrl = parseNullableString(record["provenanceUrl"], "socialSignalSource.provenanceUrl", issues);

  if (publishedAt !== null && Number.isNaN(Date.parse(publishedAt))) {
    issues.push({ path: "socialSignalSource.publishedAt", message: "must be a valid timestamp string when present" });
  }

  if (
    issues.length > 0 ||
    providerKey === null ||
    sourceType === null ||
    sourceId === null ||
    sourceLabel === null ||
    sourceQuality === null ||
    capturedAt === null
  ) {
    throw new DomainValidationError(issues);
  }

  return {
    providerKey,
    sourceType,
    sourceId,
    sourceLabel,
    sourceQuality,
    capturedAt,
    publishedAt,
    provenanceUrl,
  };
}

export function parseSocialSignalCandidate(value: unknown): SocialSignalCandidate {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "socialSignal", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "competition",
      "matchSlug",
      "checkpointType",
      "targetTeamName",
      "source",
      "summary",
      "confidence",
      "requestedAdjustment",
    ]),
  );

  const competition = parseString(record["competition"], "socialSignal.competition", issues);
  const matchSlug = parseString(record["matchSlug"], "socialSignal.matchSlug", issues);
  const checkpointType = parseEnumValue(record["checkpointType"], checkpointTypes, "socialSignal.checkpointType", issues);
  const targetTeamName = parseString(record["targetTeamName"], "socialSignal.targetTeamName", issues);
  const summary = parseString(record["summary"], "socialSignal.summary", issues);
  const confidence = parseProbability(record["confidence"], "socialSignal.confidence", issues);
  const requestedAdjustment = parseSignedAdjustment(
    record["requestedAdjustment"],
    "socialSignal.requestedAdjustment",
    issues,
  );

  let source: SocialSignalSource | null = null;

  try {
    source = parseSocialSignalSource(record["source"]);
  } catch (error) {
    if (error instanceof DomainValidationError) {
      issues.push(
        ...error.issues.map((issue) => ({
          path: issue.path.replace(/^socialSignalSource/, "socialSignal.source"),
          message: issue.message,
        })),
      );
    } else {
      throw error;
    }
  }

  if (competition !== "IPL") {
    issues.push({ path: "socialSignal.competition", message: "must be IPL" });
  }

  if (
    issues.length > 0 ||
    matchSlug === null ||
    checkpointType === null ||
    targetTeamName === null ||
    source === null ||
    summary === null ||
    confidence === null ||
    requestedAdjustment === null
  ) {
    throw new DomainValidationError(issues);
  }

  return {
    competition: "IPL",
    matchSlug,
    checkpointType,
    targetTeamName,
    source,
    summary,
    confidence,
    requestedAdjustment,
  };
}

function parseSignedAdjustment(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  const parsed = parseFiniteNumber(value, path, issues);

  if (parsed === null) {
    return null;
  }

  if (parsed < -1 || parsed > 1) {
    issues.push({ path, message: "must be between -1 and 1" });
    return null;
  }

  return parsed;
}
