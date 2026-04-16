import { load } from "cheerio";

import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalMatch } from "../../src/domain/match.js";
import { createNormalizedRepository } from "../../src/repositories/normalized.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

const DEFAULT_ESPNCRICINFO_BASE_URL = "https://www.espncricinfo.com";
const IPL_SERIES_ID = "1510719";

interface CliOptions {
  season: number;
}

interface EspnTeamEntry {
  team_name?: string;
  team_abbreviation?: string;
  squad?: unknown[];
  team_id?: string | number;
  object_id?: string | number;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);
  const normalized = createNormalizedRepository(pool);

  try {
    const matchIds = await fetchSeasonMatchIds();
    let matchesUpserted = 0;
    let appearancesUpserted = 0;

    for (const matchId of matchIds) {
      const payload = await fetchMatchPayload(matchId);
      const normalizedMatch = normalizeCompletedMatch(
        payload,
        matchId,
        options.season,
      );
      if (normalizedMatch === null) {
        continue;
      }

      const canonicalRecord = await normalized.saveCanonicalMatch(
        parseCanonicalMatch(normalizedMatch.match),
      );
      matchesUpserted += 1;

      for (const appearance of normalizedMatch.appearances) {
        await normalized.saveMatchPlayerAppearance({
          canonicalMatchId: canonicalRecord.id,
          teamName: appearance.teamName,
          playerRegistryId: appearance.playerRegistryId,
          sourcePlayerName: appearance.sourcePlayerName,
          lineupOrder: appearance.lineupOrder,
          metadata: {
            source: "espn_final_team_squad",
            matchId,
          },
        });
        appearancesUpserted += 1;
      }
    }

    process.stdout.write(
      `${JSON.stringify(
        { season: options.season, matchesUpserted, appearancesUpserted },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function fetchSeasonMatchIds(): Promise<string[]> {
  const response = await fetch(
    `${DEFAULT_ESPNCRICINFO_BASE_URL}/ci/engine/series/${IPL_SERIES_ID}.html?view=fixtures`,
    {
      headers: {
        accept: "text/html,application/xhtml+xml",
        "user-agent": "Mozilla/5.0",
      },
    },
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch ESPN fixtures: ${response.status}`);
  }

  const html = await response.text();
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

async function fetchMatchPayload(
  matchId: string,
): Promise<Record<string, unknown>> {
  const response = await fetch(
    `${DEFAULT_ESPNCRICINFO_BASE_URL}/ci/engine/match/${matchId}.json`,
    {
      headers: {
        accept: "application/json,text/plain",
        "user-agent": "Mozilla/5.0",
      },
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to fetch ESPN match ${matchId}: ${response.status}`,
    );
  }

  return (await response.json()) as Record<string, unknown>;
}

function normalizeCompletedMatch(
  payload: Record<string, unknown>,
  matchId: string,
  season: number,
): {
  match: Record<string, unknown>;
  appearances: Array<{
    teamName: string;
    playerRegistryId: number | null;
    sourcePlayerName: string;
    lineupOrder: number;
  }>;
} | null {
  const matchValue = payload["match"];
  const teamsValue = payload["team"];
  const match = isRecord(matchValue) ? matchValue : null;
  const teams = Array.isArray(teamsValue) ? teamsValue.filter(isRecord) : [];
  if (match === null || teams.length < 2) {
    return null;
  }

  const teamAName = readString(match, ["team1_name"]);
  const teamBName = readString(match, ["team2_name"]);
  const scheduledStart = normalizeEspnDateTime(
    readString(match, ["start_datetime_gmt_raw", "start_datetime_gmt"]),
  );
  const venueName = readString(match, ["ground_name"]);
  const winnerTeamId = readString(match, ["winner_team_id"]);
  const tossWinnerTeamId = readString(match, ["toss_winner_team_id"]);
  const tossDecision = normalizeTossChoice(
    readString(match, ["toss_decision", "toss_decision_name"]),
  );
  const resultName = readString(match, ["result_name", "result_short_name"]);

  if (teamAName === null || teamBName === null || scheduledStart === null) {
    return null;
  }

  const status = normalizeMatchStatus(resultName, scheduledStart);
  if (status !== "completed") {
    return null;
  }

  const mappedTeams = mapTeams(teams, teamAName, teamBName);
  const winningTeamName = mapTeamIdToName(winnerTeamId, mappedTeams);

  const appearances = extractFinalTeamAppearances(teams, season, mappedTeams);
  if (appearances.length === 0) {
    return null;
  }

  return {
    match: {
      competition: "IPL",
      matchSlug: buildMatchSlug({
        season,
        teamAName,
        teamBName,
        sourceMatchId: matchId,
      }),
      sourceMatchId: matchId,
      season,
      scheduledStart,
      teamAName,
      teamBName,
      venueName,
      status: "completed",
      tossWinnerTeamName: mapTeamIdToName(tossWinnerTeamId, mappedTeams),
      tossDecision,
      winningTeamName,
      resultType: winningTeamName === null ? null : "win",
    },
    appearances,
  };
}

function extractFinalTeamAppearances(
  teams: Record<string, unknown>[],
  season: number,
  mappedTeams: {
    team1Id: string | null;
    team2Id: string | null;
    teamAName: string;
    teamBName: string;
  },
): Array<{
  teamName: string;
  playerRegistryId: number | null;
  sourcePlayerName: string;
  lineupOrder: number;
}> {
  const appearances: Array<{
    teamName: string;
    playerRegistryId: number | null;
    sourcePlayerName: string;
    lineupOrder: number;
  }> = [];

  for (const team of teams) {
    const squadValue = team["squad"];
    const squad = Array.isArray(squadValue) ? squadValue.filter(isRecord) : [];
    const finalSquad = squad.filter(
      (player) =>
        readString(player, ["squad_type_name"])?.toLowerCase() ===
        "final team squad",
    );
    if (finalSquad.length === 0) {
      continue;
    }

    const teamName =
      mapTeamIdToName(
        readString(team, ["team_id", "object_id"]),
        mappedTeams,
      ) ??
      readString(team, ["team_name"]) ??
      null;
    if (teamName === null) {
      continue;
    }

    for (const [index, player] of finalSquad.entries()) {
      const name =
        readString(player, [
          "known_as",
          "popular_name",
          "card_short",
          "mobile_name",
        ]) ?? null;
      if (name === null) {
        continue;
      }

      appearances.push({
        teamName,
        playerRegistryId: null,
        sourcePlayerName: name,
        lineupOrder: index + 1,
      });
    }
  }

  return appearances;
}

function buildMatchSlug(input: {
  season: number;
  teamAName: string;
  teamBName: string;
  sourceMatchId: string;
}): string {
  return `ipl-${input.season}-${slugify(input.teamAName)}-vs-${slugify(input.teamBName)}-${input.sourceMatchId}`;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
}

function normalizeEspnDateTime(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.trim().replace(" ", "T");
  return normalized.endsWith("Z") ? normalized : `${normalized}Z`;
}

function normalizeMatchStatus(
  resultName: string | null,
  scheduledStart: string,
): "scheduled" | "completed" {
  if (resultName !== null && resultName.trim().length > 0) {
    return "completed";
  }
  if (Date.parse(scheduledStart) < Date.now()) {
    return "completed";
  }
  return "scheduled";
}

function normalizeTossChoice(value: string | null): "bat" | "bowl" | null {
  if (value === null) return null;
  const normalized = value.trim().toLowerCase();
  if (normalized.includes("bat")) return "bat";
  if (normalized.includes("field") || normalized.includes("bowl"))
    return "bowl";
  return null;
}

function mapTeams(
  teams: Record<string, unknown>[],
  teamAName: string,
  teamBName: string,
): {
  team1Id: string | null;
  team2Id: string | null;
  teamAName: string;
  teamBName: string;
} {
  const team1 = teams[0];
  const team2 = teams[1];
  return {
    team1Id:
      team1 === undefined ? null : readString(team1, ["team_id", "object_id"]),
    team2Id:
      team2 === undefined ? null : readString(team2, ["team_id", "object_id"]),
    teamAName,
    teamBName,
  };
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
  if (teamId === null) return null;
  if (teams.team1Id !== null && teamId === teams.team1Id)
    return teams.teamAName;
  if (teams.team2Id !== null && teamId === teams.team2Id)
    return teams.teamBName;
  return null;
}

function readString(
  record: Record<string, unknown>,
  keys: readonly string[],
): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0)
      return value.trim();
    if (typeof value === "number" && Number.isFinite(value))
      return String(value);
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season = 2026;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--season") {
      season = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument "${argument}". Expected --season.`);
  }
  return { season };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed))
    throw new Error(`${flag} requires an integer value.`);
  return parsed;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Import ESPN season appearances failed: ${message}`);
  process.exitCode = 1;
});
