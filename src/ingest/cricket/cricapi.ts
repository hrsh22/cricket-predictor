import {
  parseCanonicalMatch,
  type CanonicalMatch,
  type MatchResultType,
  type MatchStatus,
} from "../../domain/index.js";
import {
  isRecord,
  parseRecord,
  parseString,
  type ValidationIssue,
} from "../../domain/primitives.js";
import {
  type AdapterIssue,
  type AdapterRetrievalResult,
  type CricketDataProviderAdapter,
  type CricketFinalResult,
  type CricketInningsState,
  type CricketLifecycleState,
  type CricketTossState,
} from "./adapter.js";

interface ParsedCricapiMatch {
  sourceMatchId: string | null;
  scheduledStart: string;
  season: number;
  teamAName: string;
  teamBName: string;
  venueName: string | null;
  status: MatchStatus;
  tossWinnerTeamName: string | null;
  tossDecision: "bat" | "bowl" | null;
  winningTeamName: string | null;
  resultType: MatchResultType | null;
  score: readonly unknown[];
}

interface ParsedScoreEntry {
  runs: number;
  wickets: number;
  overs: number;
  inningsLabel: string | null;
}

type ParseResult<T> =
  | {
      status: "available";
      value: T;
    }
  | {
      status: "degraded";
      issues: readonly AdapterIssue[];
    };

const CRICAPI_PROVIDER_KEY = "cricapi" as const;
const T20_TOTAL_OVERS = 20;

export const cricapiAdapter: CricketDataProviderAdapter = {
  providerKey: CRICAPI_PROVIDER_KEY,

  getFixture(payload: unknown): AdapterRetrievalResult<CanonicalMatch> {
    const parsed = parseCricapiMatch(payload);

    if (parsed.status === "degraded") {
      return parsed;
    }

    const matchSlug = buildMatchSlug(
      parsed.value.season,
      parsed.value.teamAName,
      parsed.value.teamBName,
      parsed.value.sourceMatchId,
    );

    return {
      status: "available",
      value: parseCanonicalMatch({
        competition: "IPL",
        matchSlug,
        sourceMatchId: parsed.value.sourceMatchId,
        season: parsed.value.season,
        scheduledStart: parsed.value.scheduledStart,
        teamAName: parsed.value.teamAName,
        teamBName: parsed.value.teamBName,
        venueName: parsed.value.venueName,
        status: parsed.value.status,
        tossWinnerTeamName: parsed.value.tossWinnerTeamName,
        tossDecision: parsed.value.tossDecision,
        winningTeamName: parsed.value.winningTeamName,
        resultType: parsed.value.resultType,
      }),
    };
  },

  getToss(payload: unknown): AdapterRetrievalResult<CricketTossState> {
    const parsed = parseCricapiMatch(payload);
    if (parsed.status === "degraded") {
      return parsed;
    }

    const tossWinnerTeamName = parsed.value.tossWinnerTeamName;
    const tossDecision = parsed.value.tossDecision;

    if (tossWinnerTeamName === null && tossDecision === null) {
      return {
        status: "unavailable",
        reason: "toss_not_reported",
      };
    }

    if (tossWinnerTeamName === null || tossDecision === null) {
      return {
        status: "degraded",
        issues: [
          {
            path: "cricapi.toss",
            message: "toss fields must include both winner and decision",
          },
        ],
      };
    }

    return {
      status: "available",
      value: {
        tossWinnerTeamName,
        tossDecision,
      },
    };
  },

  getInningsState(
    payload: unknown,
  ): AdapterRetrievalResult<CricketInningsState> {
    const parsed = parseCricapiMatch(payload);
    if (parsed.status === "degraded") {
      return parsed;
    }

    if (parsed.value.score.length === 0) {
      return {
        status: "unavailable",
        reason: "innings_not_reported",
      };
    }

    const firstInnings = parseScoreEntry(
      parsed.value.score[0],
      "cricapi.score.0",
    );
    if (firstInnings.status === "degraded") {
      return firstInnings;
    }

    if (isPlaceholderInnings(firstInnings.value)) {
      return {
        status: "unavailable",
        reason: "innings_not_reported",
      };
    }

    const battingTeamName = inferBattingTeamName(
      firstInnings.value.inningsLabel,
      parsed.value.teamAName,
      parsed.value.teamBName,
    );
    if (battingTeamName === null) {
      return {
        status: "degraded",
        issues: [
          {
            path: "cricapi.score.0.inning",
            message: "cannot infer batting team from innings label",
          },
        ],
      };
    }

    const bowlingTeamName =
      battingTeamName === parsed.value.teamAName
        ? parsed.value.teamBName
        : parsed.value.teamAName;
    const targetRuns = firstInnings.value.runs + 1;
    const currentRunRate = calculateRunRate(
      firstInnings.value.runs,
      firstInnings.value.overs,
    );
    const requiredRunRate = calculateRequiredRunRate(
      targetRuns,
      T20_TOTAL_OVERS,
      0,
    );

    return {
      status: "available",
      value: {
        inningsNumber: 1,
        battingTeamName,
        bowlingTeamName,
        runs: firstInnings.value.runs,
        wickets: firstInnings.value.wickets,
        overs: firstInnings.value.overs,
        targetRuns,
        currentRunRate,
        requiredRunRate,
      },
    };
  },

  getFinalResult(payload: unknown): AdapterRetrievalResult<CricketFinalResult> {
    const parsed = parseCricapiMatch(payload);
    if (parsed.status === "degraded") {
      return parsed;
    }

    if (
      parsed.value.status !== "completed" &&
      parsed.value.status !== "abandoned" &&
      parsed.value.status !== "no_result"
    ) {
      return {
        status: "unavailable",
        reason: "result_not_reported",
      };
    }

    if (parsed.value.resultType === null) {
      return {
        status: "degraded",
        issues: [
          {
            path: "cricapi.status",
            message: "completed payload must include a supported result type",
          },
        ],
      };
    }

    if (
      parsed.value.resultType === "win" &&
      parsed.value.winningTeamName === null
    ) {
      return {
        status: "degraded",
        issues: [
          {
            path: "cricapi.matchWinner",
            message: "winner is required when resultType is win",
          },
        ],
      };
    }

    if (
      parsed.value.resultType === "super_over" &&
      parsed.value.winningTeamName === null
    ) {
      return {
        status: "degraded",
        issues: [
          {
            path: "cricapi.matchWinner",
            message: "winner is required when resultType is super_over",
          },
        ],
      };
    }

    return {
      status: "available",
      value: {
        winningTeamName: parsed.value.winningTeamName,
        resultType: parsed.value.resultType,
      },
    };
  },

  getLifecycleState(
    payload: unknown,
  ): AdapterRetrievalResult<CricketLifecycleState> {
    const finalResult = this.getFinalResult(payload);
    if (finalResult.status === "degraded") {
      return finalResult;
    }

    if (finalResult.status === "available") {
      return {
        status: "available",
        value: "final_result",
      };
    }

    const inningsState = this.getInningsState(payload);
    if (inningsState.status === "degraded") {
      return inningsState;
    }

    if (inningsState.status === "available") {
      return {
        status: "available",
        value: "innings_break",
      };
    }

    const toss = this.getToss(payload);
    if (toss.status === "degraded") {
      return toss;
    }

    if (toss.status === "available") {
      return {
        status: "available",
        value: "post_toss",
      };
    }

    return {
      status: "available",
      value: "pre_match",
    };
  },
};

function parseCricapiMatch(payload: unknown): ParseResult<ParsedCricapiMatch> {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(payload, "cricapi", issues);

  if (record === null) {
    return {
      status: "degraded",
      issues,
    };
  }

  const sourceMatchId = parseNullableSourceMatchId(
    record["id"],
    "cricapi.id",
    issues,
  );
  const scheduledStart = parseIsoTimestamp(
    record["date"],
    "cricapi.date",
    issues,
  );
  const season = parseSeason(scheduledStart, "cricapi.date", issues);
  const venueName = parseNullableStringField(
    record["venue"],
    "cricapi.venue",
    issues,
  );
  const status = parseMatchStatus(record["status"], "cricapi.status", issues);
  const teams = parseTeams(record, issues);
  const tossWinnerTeamName = parseNullableStringField(
    record["tossWinner"],
    "cricapi.tossWinner",
    issues,
  );
  const tossDecision = parseTossDecision(
    record["tossChoice"],
    "cricapi.tossChoice",
    issues,
  );
  const winningTeamName = parseNullableStringField(
    record["matchWinner"],
    "cricapi.matchWinner",
    issues,
  );
  const resultType = parseMatchResultType(
    record["status"],
    "cricapi.status",
    issues,
  );
  const score = parseScoreArray(record["score"], "cricapi.score", issues);

  if (
    scheduledStart === null ||
    season === null ||
    status === null ||
    teams === null ||
    score === null
  ) {
    return {
      status: "degraded",
      issues,
    };
  }

  const resolvedWinningTeamName =
    winningTeamName ?? inferWinningTeamNameFromStatus(record["status"], teams);

  if (
    status === "completed" &&
    resultType === "win" &&
    resolvedWinningTeamName === null
  ) {
    issues.push({
      path: "cricapi.matchWinner",
      message: "winner is required when status indicates a win",
    });
  }

  if (issues.length > 0) {
    return {
      status: "degraded",
      issues,
    };
  }

  return {
    status: "available",
    value: {
      sourceMatchId,
      scheduledStart,
      season,
      teamAName: teams[0],
      teamBName: teams[1],
      venueName,
      status,
      tossWinnerTeamName,
      tossDecision,
      winningTeamName: resolvedWinningTeamName,
      resultType,
      score,
    },
  };
}

function inferWinningTeamNameFromStatus(
  value: unknown,
  teams: readonly [string, string],
): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const normalizedStatus = value.toLowerCase();
  for (const team of teams) {
    if (normalizedStatus.includes(`${team.toLowerCase()} won`)) {
      return team;
    }
  }

  return null;
}

function parseNullableSourceMatchId(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "string" && value.length > 0) {
    return value;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  issues.push({ path, message: "must be a string, number, or null" });
  return null;
}

function parseIsoTimestamp(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): string | null {
  const parsed = parseString(value, path, issues);
  if (parsed === null) {
    return null;
  }

  if (Number.isNaN(Date.parse(parsed))) {
    issues.push({ path, message: "must be a valid timestamp string" });
    return null;
  }

  return parsed;
}

function parseSeason(
  scheduledStart: string | null,
  path: string,
  issues: ValidationIssue[],
): number | null {
  if (scheduledStart === null) {
    return null;
  }

  const year = new Date(scheduledStart).getUTCFullYear();
  if (!Number.isInteger(year) || year < 2008) {
    issues.push({ path, message: "must map to a valid IPL season year" });
    return null;
  }

  return year;
}

function parseNullableStringField(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    issues.push({ path, message: "must be a string or null" });
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  return trimmed;
}

function parseTeams(
  record: Record<string, unknown>,
  issues: ValidationIssue[],
): readonly [string, string] | null {
  const fromTeamInfo = parseTeamsFromTeamInfo(
    record["teamInfo"],
    "cricapi.teamInfo",
    issues,
  );
  if (fromTeamInfo !== null) {
    return fromTeamInfo;
  }

  const teamsField = record["teams"];
  if (!Array.isArray(teamsField) || teamsField.length !== 2) {
    issues.push({
      path: "cricapi.teams",
      message: "must contain exactly two team names",
    });
    return null;
  }

  const first = parseString(teamsField[0], "cricapi.teams.0", issues);
  const second = parseString(teamsField[1], "cricapi.teams.1", issues);

  if (first === null || second === null) {
    return null;
  }

  return [first, second];
}

function parseTeamsFromTeamInfo(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): readonly [string, string] | null {
  if (!Array.isArray(value)) {
    return null;
  }

  if (value.length !== 2) {
    issues.push({ path, message: "must contain exactly two team objects" });
    return null;
  }

  const first = parseTeamInfoName(value[0], `${path}.0`, issues);
  const second = parseTeamInfoName(value[1], `${path}.1`, issues);

  if (first === null || second === null) {
    return null;
  }

  return [first, second];
}

function parseTeamInfoName(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): string | null {
  if (!isRecord(value)) {
    issues.push({ path, message: "must be a plain object" });
    return null;
  }

  return parseString(value["name"], `${path}.name`, issues);
}

function parseMatchStatus(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): MatchStatus | null {
  const parsed = parseString(value, path, issues);
  if (parsed === null) {
    return null;
  }

  const normalized = parsed.toLowerCase();

  if (normalized.includes("abandon")) {
    return "abandoned";
  }

  if (normalized.includes("no result")) {
    return "no_result";
  }

  if (
    normalized.includes("toss won") ||
    normalized.includes("elected to") ||
    normalized.includes("elected") ||
    normalized.includes("toss update")
  ) {
    return "scheduled";
  }

  if (
    normalized.includes("not started") ||
    normalized.includes("starts at") ||
    normalized.includes("upcoming") ||
    normalized.includes("scheduled")
  ) {
    return "scheduled";
  }

  if (
    normalized.includes("won") ||
    normalized.includes("match ended") ||
    normalized.includes("completed") ||
    normalized.includes("result") ||
    normalized.includes("tie")
  ) {
    return "completed";
  }

  return "in_progress";
}

function parseMatchResultType(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): MatchResultType | null {
  const parsed = parseString(value, path, issues);
  if (parsed === null) {
    return null;
  }

  const normalized = parsed.toLowerCase();

  if (normalized.includes("abandon")) {
    return "abandoned";
  }

  if (normalized.includes("no result")) {
    return "no_result";
  }

  if (normalized.includes("super over")) {
    return "super_over";
  }

  if (normalized.includes("tie")) {
    return "tie";
  }

  if (
    normalized.includes("toss won") ||
    normalized.includes("elected to") ||
    normalized.includes("elected") ||
    normalized.includes("toss update")
  ) {
    return null;
  }

  if (
    normalized.includes("won") ||
    normalized.includes("match ended") ||
    normalized.includes("completed")
  ) {
    return "win";
  }

  return null;
}

function parseTossDecision(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): "bat" | "bowl" | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = parseString(value, path, issues);
  if (parsed === null) {
    return null;
  }

  const normalized = parsed.toLowerCase();
  if (normalized === "bat" || normalized === "batting") {
    return "bat";
  }

  if (
    normalized === "bowl" ||
    normalized === "bowling" ||
    normalized === "field"
  ) {
    return "bowl";
  }

  issues.push({ path, message: "must be bat/batting or bowl/bowling" });
  return null;
}

function parseScoreArray(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): readonly unknown[] | null {
  if (value === null || value === undefined) {
    return [];
  }

  if (!Array.isArray(value)) {
    issues.push({ path, message: "must be an array" });
    return null;
  }

  return value;
}

function parseScoreEntry(
  value: unknown,
  path: string,
): ParseResult<ParsedScoreEntry> {
  const issues: AdapterIssue[] = [];
  if (!isRecord(value)) {
    return {
      status: "degraded",
      issues: [{ path, message: "must be a plain object" }],
    };
  }

  const runs = parseNonNegativeInteger(value["r"], `${path}.r`, issues);
  const wickets = parseNonNegativeInteger(value["w"], `${path}.w`, issues);
  const overs = parseNonNegativeNumber(value["o"], `${path}.o`, issues);
  const inningsLabel = parseOptionalString(
    value["inning"],
    `${path}.inning`,
    issues,
  );

  if (runs === null || wickets === null || overs === null) {
    return {
      status: "degraded",
      issues,
    };
  }

  return {
    status: "available",
    value: {
      runs,
      wickets,
      overs,
      inningsLabel,
    },
  };
}

function isPlaceholderInnings(value: ParsedScoreEntry): boolean {
  return value.runs === 0 && value.wickets === 0 && value.overs === 0;
}

function parseNonNegativeInteger(
  value: unknown,
  path: string,
  issues: AdapterIssue[],
): number | null {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    issues.push({ path, message: "must be a non-negative integer" });
    return null;
  }

  return value;
}

function parseNonNegativeNumber(
  value: unknown,
  path: string,
  issues: AdapterIssue[],
): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    issues.push({ path, message: "must be a non-negative number" });
    return null;
  }

  return value;
}

function parseOptionalString(
  value: unknown,
  path: string,
  issues: AdapterIssue[],
): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    issues.push({ path, message: "must be a string" });
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function inferBattingTeamName(
  inningsLabel: string | null,
  teamAName: string,
  teamBName: string,
): string | null {
  if (inningsLabel === null) {
    return null;
  }

  const normalized = inningsLabel.toLowerCase();
  if (normalized.includes(teamAName.toLowerCase())) {
    return teamAName;
  }

  if (normalized.includes(teamBName.toLowerCase())) {
    return teamBName;
  }

  return null;
}

function calculateRunRate(runs: number, overs: number): number {
  if (overs === 0) {
    return 0;
  }

  return Number((runs / overs).toFixed(4));
}

function calculateRequiredRunRate(
  targetRuns: number,
  totalOvers: number,
  completedOvers: number,
): number {
  const remainingOvers = totalOvers - completedOvers;
  if (remainingOvers <= 0) {
    return 0;
  }

  return Number((targetRuns / remainingOvers).toFixed(4));
}

function buildMatchSlug(
  season: number,
  teamAName: string,
  teamBName: string,
  sourceMatchId: string | null,
): string {
  const teamA = slugify(teamAName);
  const teamB = slugify(teamBName);
  const idSuffix = sourceMatchId === null ? "manual" : slugify(sourceMatchId);

  return `ipl-${season}-${teamA}-vs-${teamB}-${idSuffix}`;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}
