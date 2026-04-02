import {
  isRecord,
  type JsonObject,
  type UnknownRecord,
} from "../../domain/primitives.js";
import { fetchLiveEspncricinfoSnapshots } from "./live-espncricinfo.js";
import type { CricketSnapshotInput } from "./pipeline.js";

const DEFAULT_CRICAPI_BASE_URL = "https://api.cricapi.com/v1";
const CRICAPI_PAGE_SIZE = 25;
const IPL_TEAM_NAMES = new Set([
  "Chennai Super Kings",
  "Mumbai Indians",
  "Royal Challengers Bengaluru",
  "Royal Challengers Bangalore",
  "Kolkata Knight Riders",
  "Rajasthan Royals",
  "Sunrisers Hyderabad",
  "Delhi Capitals",
  "Punjab Kings",
  "Kings XI Punjab",
  "Gujarat Titans",
  "Lucknow Super Giants",
]);

export interface CricketLiveSourceConfig {
  provider: "espncricinfo" | "cricapi";
  apiKey: string;
  baseUrl?: string;
}

export interface FetchLiveCricketSnapshotsOptions {
  config: CricketLiveSourceConfig;
  fetchedAt?: string;
  fetchImpl?: typeof fetch;
  nextMatchOnly?: boolean;
}

export async function fetchLiveCricketSnapshots(
  options: FetchLiveCricketSnapshotsOptions,
): Promise<readonly CricketSnapshotInput[]> {
  if (options.config.provider === "espncricinfo") {
    return fetchLiveEspncricinfoSnapshots({
      ...(options.config.baseUrl === undefined
        ? {}
        : { baseUrl: options.config.baseUrl }),
      ...(options.fetchedAt === undefined
        ? {}
        : { fetchedAt: options.fetchedAt }),
      ...(options.fetchImpl === undefined
        ? {}
        : { fetchImpl: options.fetchImpl }),
      ...(options.nextMatchOnly === undefined
        ? {}
        : { nextMatchOnly: options.nextMatchOnly }),
    });
  }

  if (options.config.provider !== "cricapi") {
    throw new Error(
      `Unsupported live cricket provider "${options.config.provider}". Expected espncricinfo or cricapi.`,
    );
  }

  const apiKey = options.config.apiKey.trim();
  if (apiKey.length === 0) {
    throw new Error(
      "Missing CRICAPI_API_KEY. Live cricket fetching requires a CricketData/CricAPI API key.",
    );
  }

  const baseUrl = (options.config.baseUrl ?? DEFAULT_CRICAPI_BASE_URL).trim();
  const fetchedAt = normalizeIsoTimestamp(
    options.fetchedAt ?? new Date().toISOString(),
  );
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;

  if (typeof fetchImpl !== "function") {
    throw new Error("Global fetch is unavailable in this Node runtime.");
  }

  const currentMatches = await fetchCricapiCollection(
    buildCricapiUrl(baseUrl, "currentMatches", apiKey, 0),
    { fetchImpl },
  );
  const scheduledMatches = await fetchScheduledMatchCandidates({
    baseUrl,
    apiKey,
    fetchImpl,
    currentMatches,
  });

  const snapshotsByMatchId = new Map<string, CricketSnapshotInput>();

  for (const entry of [...scheduledMatches, ...currentMatches]) {
    const snapshot = toLiveCricketSnapshot(entry, fetchedAt);
    if (snapshot === null) {
      continue;
    }

    const payload = snapshot.payload;
    if (!isRecord(payload)) {
      continue;
    }

    const matchId = payload["id"];
    if (typeof matchId === "string") {
      snapshotsByMatchId.set(matchId, snapshot);
    }
  }

  const sorted = Array.from(snapshotsByMatchId.values()).sort((left, right) =>
    extractScheduledDate(left.payload).localeCompare(
      extractScheduledDate(right.payload),
    ),
  );

  if (options.nextMatchOnly) {
    const fetchedAtMs = Date.parse(fetchedAt);
    const nextUpcoming = sorted.find((snapshot) => {
      const matchDateStr = extractScheduledDate(snapshot.payload);
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

export function toLiveCricketSnapshot(
  value: unknown,
  fetchedAt: string,
): CricketSnapshotInput | null {
  const payload = toCricapiLikePayload(value);
  if (payload === null || !looksLikeIplPayload(payload)) {
    return null;
  }

  return {
    snapshotTime: normalizeIsoTimestamp(fetchedAt),
    payload,
  };
}

function buildCricapiUrl(
  baseUrl: string,
  endpoint: "currentMatches" | "matches",
  apiKey: string,
  offset: number,
): string {
  const url = new URL(endpoint, ensureTrailingSlash(baseUrl));
  url.searchParams.set("apikey", apiKey);
  url.searchParams.set("offset", String(offset));
  return url.toString();
}

async function fetchPaginatedCricapiCollection(input: {
  baseUrl: string;
  endpoint: "matches";
  apiKey: string;
  pageCount: number;
  fetchImpl: typeof fetch;
}): Promise<readonly unknown[]> {
  const rows: unknown[] = [];

  for (let pageIndex = 0; pageIndex < input.pageCount; pageIndex += 1) {
    const offset = pageIndex * CRICAPI_PAGE_SIZE;
    const pageRows = await fetchCricapiCollection(
      buildCricapiUrl(input.baseUrl, input.endpoint, input.apiKey, offset),
      { fetchImpl: input.fetchImpl },
    );
    rows.push(...pageRows);

    if (pageRows.length < CRICAPI_PAGE_SIZE) {
      break;
    }
  }

  return rows;
}

async function fetchScheduledMatchCandidates(input: {
  baseUrl: string;
  apiKey: string;
  fetchImpl: typeof fetch;
  currentMatches: readonly unknown[];
}): Promise<readonly unknown[]> {
  const iplSeriesIds = extractIplSeriesIds(input.currentMatches);
  if (iplSeriesIds.length === 0) {
    const firstPages = await fetchPaginatedCricapiCollection({
      baseUrl: input.baseUrl,
      endpoint: "matches",
      apiKey: input.apiKey,
      pageCount: 2,
      fetchImpl: input.fetchImpl,
    });

    const fallbackSeriesIds = extractIplSeriesIds(firstPages);
    if (fallbackSeriesIds.length > 0) {
      const seriesMatches = await Promise.all(
        fallbackSeriesIds.map((seriesId) =>
          fetchCricapiSeriesMatchList({
            baseUrl: input.baseUrl,
            apiKey: input.apiKey,
            seriesId,
            fetchImpl: input.fetchImpl,
          }),
        ),
      );

      return seriesMatches.flat();
    }

    return firstPages;
  }

  const seriesMatches = await Promise.all(
    iplSeriesIds.map((seriesId) =>
      fetchCricapiSeriesMatchList({
        baseUrl: input.baseUrl,
        apiKey: input.apiKey,
        seriesId,
        fetchImpl: input.fetchImpl,
      }),
    ),
  );

  return seriesMatches.flat();
}

function extractIplSeriesIds(rows: readonly unknown[]): string[] {
  const seriesIds = new Set<string>();

  for (const row of rows) {
    if (!isRecord(row)) {
      continue;
    }

    const payload = toCricapiLikePayload(row);
    if (payload === null || !looksLikeIplPayload(payload)) {
      continue;
    }

    const seriesId = readRequiredString(row, ["series_id", "seriesId"]);
    if (seriesId !== null) {
      seriesIds.add(seriesId);
    }
  }

  return Array.from(seriesIds);
}

async function fetchCricapiSeriesMatchList(input: {
  baseUrl: string;
  apiKey: string;
  seriesId: string;
  fetchImpl: typeof fetch;
}): Promise<readonly unknown[]> {
  const url = new URL("series_info", ensureTrailingSlash(input.baseUrl));
  url.searchParams.set("apikey", input.apiKey);
  url.searchParams.set("id", input.seriesId);

  const response = await input.fetchImpl(url.toString(), {
    headers: { accept: "application/json" },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(
      `Live cricket series request failed with status ${response.status} for ${url}: ${body}`,
    );
  }

  const json = (await response.json()) as unknown;
  if (!isRecord(json) || !isRecord(json["data"])) {
    throw new Error(
      `Unexpected CricAPI series_info response shape from ${url}.`,
    );
  }

  const data = json["data"];
  const matchList = data["matchList"];
  if (!Array.isArray(matchList)) {
    throw new Error(
      `Unexpected CricAPI series_info matchList payload from ${url}.`,
    );
  }

  return matchList;
}

async function fetchCricapiCollection(
  url: string,
  options: { fetchImpl: typeof fetch },
): Promise<readonly unknown[]> {
  const response = await options.fetchImpl(url, {
    headers: { accept: "application/json" },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(
      `Live cricket request failed with status ${response.status} for ${url}: ${body}`,
    );
  }

  const json = (await response.json()) as unknown;
  if (isRecord(json) && json["status"] === "failure") {
    const reason =
      typeof json["reason"] === "string"
        ? json["reason"]
        : "unknown CricAPI failure";

    if (/no data|no match|no records|no result/i.test(reason)) {
      return [];
    }

    throw new Error(`CricAPI error for ${url}: ${reason}`);
  }

  if (Array.isArray(json)) {
    return json;
  }

  if (isRecord(json) && Array.isArray(json["data"])) {
    return json["data"];
  }

  throw new Error(
    `Unexpected CricAPI response shape from ${url}. Expected an array or { data: [...] } response.`,
  );
}

function toCricapiLikePayload(value: unknown): JsonObject | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = readRequiredString(value, ["id", "matchId", "unique_id"]);
  const teams = readTeams(value);
  const name =
    readOptionalString(value, ["name", "title"]) ??
    (teams === null ? null : `${teams[0]} vs ${teams[1]}`);
  const dateValue = readRequiredString(value, [
    "dateTimeGMT",
    "dateTime",
    "date",
  ]);
  const date = dateValue === null ? null : normalizeProviderDate(dateValue);

  if (id === null || teams === null || name === null || date === null) {
    return null;
  }

  const payload: JsonObject = {
    id,
    name,
    matchType:
      readOptionalString(value, ["matchType", "type"])?.toLowerCase() ?? "t20",
    status:
      readOptionalString(value, ["status"]) ?? inferStatusFromPayload(value),
    venue:
      readOptionalString(value, ["venue"]) ??
      readNestedOptionalString(value, ["venueInfo", "name"]),
    date,
    teams,
    teamInfo: normalizeTeamInfo(value, teams),
    score: normalizeScoreArray(value["score"]),
    tossWinner: readOptionalString(value, ["tossWinner"]),
    tossChoice: normalizeTossChoice(
      readOptionalString(value, ["tossChoice", "tossDecision"]),
    ),
    matchWinner: readOptionalString(value, ["matchWinner", "winner"]),
  };

  return payload;
}

function looksLikeIplPayload(payload: JsonObject): boolean {
  const name = typeof payload["name"] === "string" ? payload["name"] : null;
  if (name !== null && /indian premier league|\bipl\b/i.test(name)) {
    return true;
  }

  const teams = payload["teams"];
  if (!Array.isArray(teams) || teams.length !== 2) {
    return false;
  }

  return teams.every(
    (team) => typeof team === "string" && IPL_TEAM_NAMES.has(team),
  );
}

function readTeams(value: UnknownRecord): [string, string] | null {
  const teams = value["teams"];
  if (
    Array.isArray(teams) &&
    teams.length >= 2 &&
    typeof teams[0] === "string" &&
    typeof teams[1] === "string"
  ) {
    return [teams[0], teams[1]];
  }

  const teamInfo = value["teamInfo"];
  if (!Array.isArray(teamInfo) || teamInfo.length < 2) {
    return null;
  }

  const first = teamInfo[0];
  const second = teamInfo[1];
  if (!isRecord(first) || !isRecord(second)) {
    return null;
  }

  const firstName = readRequiredString(first, ["name"]);
  const secondName = readRequiredString(second, ["name"]);
  return firstName !== null && secondName !== null
    ? [firstName, secondName]
    : null;
}

function normalizeTeamInfo(
  value: UnknownRecord,
  teams: readonly [string, string],
): JsonObject[] {
  const rawTeamInfo = value["teamInfo"];
  if (Array.isArray(rawTeamInfo)) {
    const normalized = rawTeamInfo.filter(isRecord).map((entry) => ({
      name: readRequiredString(entry, ["name"]) ?? "",
      shortname: readOptionalString(entry, ["shortname", "shortName"]),
    }));
    if (
      normalized.length >= 2 &&
      normalized.every((entry) => entry.name !== "")
    ) {
      return normalized.slice(0, 2);
    }
  }

  return teams.map((teamName) => ({ name: teamName, shortname: null }));
}

function normalizeScoreArray(value: unknown): JsonObject[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter(isRecord).map((entry) => ({
    r: readOptionalNumber(entry, ["r", "runs"]),
    w: readOptionalNumber(entry, ["w", "wickets"]),
    o: readOptionalNumber(entry, ["o", "overs"]),
    inning:
      readOptionalString(entry, ["inning", "inningName", "inningLabel"]) ??
      null,
  }));
}

function inferStatusFromPayload(value: UnknownRecord): string {
  const matchEnded = readOptionalBoolean(value, ["matchEnded"]);
  if (matchEnded === true) {
    const winner = readOptionalString(value, ["matchWinner", "winner"]);
    return winner === null ? "Match completed" : `${winner} won`;
  }

  const score = value["score"];
  if (Array.isArray(score) && score.length > 0) {
    return "Innings break";
  }

  const tossWinner = readOptionalString(value, ["tossWinner"]);
  if (tossWinner !== null) {
    return `Toss won by ${tossWinner}`;
  }

  return "Match not started";
}

function readRequiredString(
  value: UnknownRecord,
  keys: readonly string[],
): string | null {
  for (const key of keys) {
    const candidate = value[key];
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate.trim();
    }
  }
  return null;
}

function readOptionalString(
  value: UnknownRecord,
  keys: readonly string[],
): string | null {
  return readRequiredString(value, keys);
}

function readNestedOptionalString(
  value: UnknownRecord,
  path: readonly string[],
): string | null {
  let current: unknown = value;
  for (const key of path) {
    if (!isRecord(current)) {
      return null;
    }
    current = current[key];
  }
  return typeof current === "string" && current.trim().length > 0
    ? current.trim()
    : null;
}

function readOptionalNumber(
  value: UnknownRecord,
  keys: readonly string[],
): number | null {
  for (const key of keys) {
    const candidate = value[key];
    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return candidate;
    }
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      const parsed = Number(candidate);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return null;
}

function readOptionalBoolean(
  value: UnknownRecord,
  keys: readonly string[],
): boolean | null {
  for (const key of keys) {
    const candidate = value[key];
    if (typeof candidate === "boolean") {
      return candidate;
    }
  }
  return null;
}

function normalizeTossChoice(value: string | null): "bat" | "bowl" | null {
  if (value === null) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "bat" || normalized === "batting") {
    return "bat";
  }
  if (normalized === "bowl" || normalized === "bowling") {
    return "bowl";
  }
  return null;
}

function normalizeIsoTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid live cricket timestamp "${value}".`);
  }
  return date.toISOString();
}

function normalizeProviderDate(value: string): string {
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/u.test(trimmed)) {
    return `${trimmed}Z`;
  }

  return trimmed;
}

function ensureTrailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function extractScheduledDate(value: unknown): string {
  if (!isRecord(value)) {
    return "";
  }
  const date = value["date"];
  return typeof date === "string" ? date : "";
}
