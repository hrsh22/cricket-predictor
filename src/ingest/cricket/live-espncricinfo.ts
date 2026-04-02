import { load } from "cheerio";

import {
  isRecord,
  type JsonObject,
  type UnknownRecord,
} from "../../domain/primitives.js";
import type { CricketSnapshotInput } from "./pipeline.js";

const DEFAULT_ESPNCRICINFO_BASE_URL = "https://www.espncricinfo.com";
const IPL_SERIES_ID = "1510719";
const PAST_WINDOW_DAYS = 2;
const FUTURE_WINDOW_DAYS = 14;

interface FetchLiveEspncricinfoSnapshotsOptions {
  baseUrl?: string;
  fetchedAt?: string;
  fetchImpl?: typeof fetch;
  /** When true, returns only the single next upcoming match (scheduled in the future) */
  nextMatchOnly?: boolean;
}

export async function fetchLiveEspncricinfoSnapshots(
  options: FetchLiveEspncricinfoSnapshotsOptions,
): Promise<readonly CricketSnapshotInput[]> {
  const baseUrl = (options.baseUrl ?? DEFAULT_ESPNCRICINFO_BASE_URL).trim();
  const fetchedAt = normalizeIsoTimestamp(
    options.fetchedAt ?? new Date().toISOString(),
  );
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  const nextMatchOnly = options.nextMatchOnly ?? false;

  const fixturesHtml = await fetchText(
    `${ensureTrailingSlash(baseUrl)}ci/engine/series/${IPL_SERIES_ID}.html?view=fixtures`,
    fetchImpl,
    "text/html,application/xhtml+xml",
  );
  const matchIds = extractFixtureMatchIds(fixturesHtml);

  const snapshots: CricketSnapshotInput[] = [];
  for (const matchId of matchIds) {
    const payload = await fetchEspnMatchPayload({
      baseUrl,
      matchId,
      fetchImpl,
    });
    const snapshot = toEspnSnapshot(payload, fetchedAt, matchId);
    if (snapshot === null || !isWithinRelevantWindow(snapshot, fetchedAt)) {
      continue;
    }
    snapshots.push(snapshot);
  }

  const sorted = snapshots.sort((left, right) =>
    extractPayloadDate(left.payload).localeCompare(
      extractPayloadDate(right.payload),
    ),
  );

  if (nextMatchOnly) {
    const fetchedAtMs = Date.parse(fetchedAt);
    const nextUpcoming = sorted.find((snapshot) => {
      const matchDateStr = extractPayloadDate(snapshot.payload);
      if (matchDateStr.length === 0) {
        return false;
      }
      const matchDateMs = Date.parse(matchDateStr);
      return matchDateMs > fetchedAtMs;
    });
    return nextUpcoming === undefined ? [] : [nextUpcoming];
  }

  return sorted;
}

function extractFixtureMatchIds(html: string): string[] {
  const $ = load(html);
  const ids = new Set<string>();

  $("a[href*='/series/']").each((_, element) => {
    const href = $(element).attr("href") ?? "";
    const match = href.match(/\/series\/\d+\/(?:game|scorecard)\/(\d+)\//u);
    if (match?.[1] !== undefined) {
      ids.add(match[1]);
    }
  });

  return Array.from(ids);
}

async function fetchEspnMatchPayload(input: {
  baseUrl: string;
  matchId: string;
  fetchImpl: typeof fetch;
}): Promise<unknown> {
  const url = `${ensureTrailingSlash(input.baseUrl)}ci/engine/match/${input.matchId}.json`;
  const text = await fetchText(
    url,
    input.fetchImpl,
    "application/json,text/plain",
  );
  return JSON.parse(text) as unknown;
}

function toEspnSnapshot(
  payload: unknown,
  fetchedAt: string,
  matchId: string,
): CricketSnapshotInput | null {
  if (!isRecord(payload)) {
    return null;
  }

  const match = payload["match"];
  if (!isRecord(match)) {
    return null;
  }

  const series = payload["series"];
  if (!Array.isArray(series) || !series.some(isIplSeriesRecord)) {
    return null;
  }

  const teamAName = readString(match, ["team1_name"]);
  const teamBName = readString(match, ["team2_name"]);
  const scheduledStart = normalizeEspnDateTime(
    readString(match, ["start_datetime_gmt_raw", "start_datetime_gmt"]),
  );
  if (teamAName === null || teamBName === null || scheduledStart === null) {
    return null;
  }

  const team1Id = readString(match, ["team1_id"]);
  const team2Id = readString(match, ["team2_id"]);
  const tossWinnerId = readString(match, ["toss_winner_team_id"]);
  const winnerTeamId = readString(match, ["winner_team_id"]);
  const liveStatus = readLiveStatus(payload, match);
  const innings = Array.isArray(payload["innings"]) ? payload["innings"] : [];

  const normalizedPayload: JsonObject = {
    id:
      readString(match, ["object_id"]) ??
      readString(match, ["match_id"]) ??
      matchId,
    name: `${teamAName} vs ${teamBName}`,
    matchType: "t20",
    status: liveStatus,
    venue: readString(match, ["ground_name"]),
    date: scheduledStart,
    teams: [teamAName, teamBName],
    teamInfo: [
      {
        name: teamAName,
        shortname: readString(match, [
          "team1_abbreviation",
          "team1_short_name",
        ]),
      },
      {
        name: teamBName,
        shortname: readString(match, [
          "team2_abbreviation",
          "team2_short_name",
        ]),
      },
    ],
    score: normalizeEspnInnings(innings, {
      team1Id,
      team2Id,
      teamAName,
      teamBName,
    }),
    tossWinner: mapTeamIdToName(tossWinnerId, {
      team1Id,
      team2Id,
      teamAName,
      teamBName,
    }),
    tossChoice: readEspnTossChoice(match, liveStatus),
    matchWinner: mapTeamIdToName(winnerTeamId, {
      team1Id,
      team2Id,
      teamAName,
      teamBName,
    }),
    espn: payload as JsonObject,
  };

  return {
    snapshotTime: fetchedAt,
    payload: normalizedPayload,
  };
}

function readEspnTossChoice(
  match: UnknownRecord,
  liveStatus: string,
): "bat" | "bowl" | null {
  const directChoice = readString(match, ["toss_decision", "toss_choice"]);
  const parsedDirect = normalizeTossChoice(directChoice);
  if (parsedDirect !== null) {
    return parsedDirect;
  }

  return normalizeTossChoice(liveStatus);
}

function normalizeTossChoice(value: string | null): "bat" | "bowl" | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (
    normalized.includes("elected to bat") ||
    normalized.includes("chose to bat") ||
    normalized === "bat" ||
    normalized === "batting"
  ) {
    return "bat";
  }

  if (
    normalized.includes("elected to field") ||
    normalized.includes("elected to bowl") ||
    normalized.includes("chose to field") ||
    normalized.includes("chose to bowl") ||
    normalized === "field" ||
    normalized === "bowl" ||
    normalized === "bowling"
  ) {
    return "bowl";
  }

  return null;
}

function normalizeEspnInnings(
  innings: readonly unknown[],
  teams: {
    team1Id: string | null;
    team2Id: string | null;
    teamAName: string;
    teamBName: string;
  },
): JsonObject[] {
  return innings
    .filter(isRecord)
    .map((entry) => {
      const battingTeamId = readString(entry, ["batting_team_id", "team_id"]);
      const teamName = mapTeamIdToName(battingTeamId, teams);
      const inningsNumber = readString(entry, [
        "innings_number",
        "innings_num",
      ]);
      return {
        r: readNumber(entry, ["runs"]),
        w: readNumber(entry, ["wickets"]),
        o: readNumber(entry, ["overs"]),
        inning:
          teamName === null
            ? null
            : `${teamName} Inning ${inningsNumber ?? "1"}`,
      };
    })
    .filter(
      (entry) => entry.r !== null && entry.w !== null && entry.o !== null,
    );
}

function readLiveStatus(payload: UnknownRecord, match: UnknownRecord): string {
  const live = payload["live"];
  if (isRecord(live)) {
    const status = readString(live, ["status"]);
    if (status !== null) {
      return status;
    }
  }

  return (
    readString(match, ["match_status", "status"]) ??
    "Match scheduled to begin at 19:30 local time (14:00 GMT)"
  );
}

function isIplSeriesRecord(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }

  return readString(value, ["object_id"]) === IPL_SERIES_ID;
}

function mapTeamIdToName(
  teamId: string | null,
  teams: {
    team1Id: string | null;
    team2Id: string | null;
    teamAName: string;
    teamBName: string;
  },
): string | null {
  if (teamId === null) {
    return null;
  }

  if (teams.team1Id !== null && teamId === teams.team1Id) {
    return teams.teamAName;
  }

  if (teams.team2Id !== null && teamId === teams.team2Id) {
    return teams.teamBName;
  }

  return null;
}

function isWithinRelevantWindow(
  snapshot: CricketSnapshotInput,
  fetchedAt: string,
): boolean {
  const payload = snapshot.payload;
  if (!isRecord(payload)) {
    return false;
  }

  const date = payload["date"];
  if (typeof date !== "string") {
    return false;
  }

  const matchTime = Date.parse(date);
  const fetchedTime = Date.parse(fetchedAt);
  const deltaDays = (matchTime - fetchedTime) / (1000 * 60 * 60 * 24);

  return deltaDays >= -PAST_WINDOW_DAYS && deltaDays <= FUTURE_WINDOW_DAYS;
}

function normalizeEspnDateTime(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().replace(" ", "T");
  return normalized.endsWith("Z") ? normalized : `${normalized}Z`;
}

function readString(
  record: UnknownRecord,
  keys: readonly string[],
): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }

  return null;
}

function readNumber(
  record: UnknownRecord,
  keys: readonly string[],
): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return null;
}

async function fetchText(
  url: string,
  fetchImpl: typeof fetch,
  accept: string,
): Promise<string> {
  const response = await fetchImpl(url, {
    headers: {
      accept,
      "user-agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(
      `ESPN Cricinfo request failed with status ${response.status} for ${url}: ${body}`,
    );
  }

  return response.text();
}

function normalizeIsoTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid ESPN live scrape timestamp "${value}".`);
  }
  return date.toISOString();
}

function extractPayloadDate(value: unknown): string {
  if (!isRecord(value)) {
    return "";
  }

  const date = value["date"];
  return typeof date === "string" ? date : "";
}

function ensureTrailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
