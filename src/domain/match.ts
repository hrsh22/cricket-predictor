import {
  collectUnknownKeys,
  DomainValidationError,
  parseEnumValue,
  parseNullableEnumValue,
  parseNullableString,
  parsePositiveInteger,
  parseRecord,
  parseString,
  parseTimestamptzString,
  type ValidationIssue,
} from "./primitives.js";

export type MatchStatus = "scheduled" | "in_progress" | "completed" | "abandoned" | "no_result";

export type MatchResultType = "win" | "tie" | "no_result" | "abandoned" | "super_over";

export interface CanonicalMatch {
  competition: "IPL";
  matchSlug: string;
  sourceMatchId: string | null;
  season: number;
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
  venueName: string | null;
  status: MatchStatus;
  tossWinnerTeamName: string | null;
  tossDecision: "bat" | "bowl" | null;
  winningTeamName: string | null;
  resultType: MatchResultType | null;
}

const matchStatuses = ["scheduled", "in_progress", "completed", "abandoned", "no_result"] as const;
const matchResultTypes = ["win", "tie", "no_result", "abandoned", "super_over"] as const;

export function parseCanonicalMatch(value: unknown): CanonicalMatch {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "match", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "competition",
      "matchSlug",
      "sourceMatchId",
      "season",
      "scheduledStart",
      "teamAName",
      "teamBName",
      "venueName",
      "status",
      "tossWinnerTeamName",
      "tossDecision",
      "winningTeamName",
      "resultType",
    ]),
  );

  const competition = parseString(record["competition"], "match.competition", issues);
  const matchSlug = parseString(record["matchSlug"], "match.matchSlug", issues);
  const sourceMatchId = parseNullableString(record["sourceMatchId"], "match.sourceMatchId", issues);
  const season = parsePositiveInteger(record["season"], "match.season", issues);
  const scheduledStart = parseTimestamptzString(record["scheduledStart"], "match.scheduledStart", issues);
  const teamAName = parseString(record["teamAName"], "match.teamAName", issues);
  const teamBName = parseString(record["teamBName"], "match.teamBName", issues);
  const venueName = parseNullableString(record["venueName"], "match.venueName", issues);
  const status = parseEnumValue(record["status"], matchStatuses, "match.status", issues);
  const tossWinnerTeamName = parseNullableString(record["tossWinnerTeamName"], "match.tossWinnerTeamName", issues);
  const tossDecision = parseNullableEnumValue(record["tossDecision"], ["bat", "bowl"] as const, "match.tossDecision", issues);
  const winningTeamName = parseNullableString(record["winningTeamName"], "match.winningTeamName", issues);
  const resultType = parseNullableEnumValue(record["resultType"], matchResultTypes, "match.resultType", issues);

  if (competition !== "IPL") {
    issues.push({ path: "match.competition", message: "must be IPL" });
  }

  if (issues.length > 0 || matchSlug === null || season === null || scheduledStart === null || teamAName === null || teamBName === null || status === null) {
    throw new DomainValidationError(issues);
  }

  return {
    competition: "IPL",
    matchSlug,
    sourceMatchId,
    season,
    scheduledStart,
    teamAName,
    teamBName,
    venueName,
    status,
    tossWinnerTeamName,
    tossDecision,
    winningTeamName,
    resultType,
  };
}

export function isPredictableMatch(match: CanonicalMatch): boolean {
  if (match.status === "abandoned" || match.status === "no_result") {
    return false;
  }

  if (match.resultType === "no_result" || match.resultType === "super_over" || match.resultType === "abandoned") {
    return false;
  }

  return true;
}

export function assertPredictableMatch(match: CanonicalMatch): void {
  if (!isPredictableMatch(match)) {
    throw new DomainValidationError([
      {
        path: "match.resultType",
        message: "no_result, super_over, and abandoned matches are not valid predictor inputs",
      },
    ]);
  }
}
