import { load } from "cheerio";

import { loadAppConfig } from "../../src/config/index.js";
import { createNormalizedRepository } from "../../src/repositories/normalized.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

const TEAM_SLUGS = [
  "chennai-super-kings",
  "delhi-capitals",
  "gujarat-titans",
  "kolkata-knight-riders",
  "lucknow-super-giants",
  "mumbai-indians",
  "punjab-kings",
  "rajasthan-royals",
  "royal-challengers-bengaluru",
  "sunrisers-hyderabad",
] as const;

interface CliOptions {
  season: number;
}

interface SquadPlayer {
  teamName: string;
  playerName: string;
  pulseId: string | null;
  squadRole: string | null;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);
  const normalized = createNormalizedRepository(pool);

  try {
    let squadRows = 0;

    for (const teamSlug of TEAM_SLUGS) {
      const squad = await fetchTeamSquad(teamSlug, options.season);
      for (const player of squad.players) {
        const playerRegistry =
          player.pulseId === null
            ? null
            : await normalized.savePlayerRegistry({
                cricsheetPlayerId: `pulse:${player.pulseId}`,
                canonicalName: player.playerName,
                playerRole: normalizeRole(player.squadRole),
                metadata: {
                  externalIds: {
                    pulseId: player.pulseId,
                  },
                  source: "ipl_official_squad",
                },
              });

        await pool.query(
          `
            insert into team_season_squads (
              season,
              team_name,
              player_registry_id,
              source_player_name,
              squad_role,
              source,
              metadata
            ) values ($1, $2, $3, $4, $5, $6, $7)
            on conflict (season, team_name, source_player_name) do update set
              player_registry_id = excluded.player_registry_id,
              squad_role = excluded.squad_role,
              source = excluded.source,
              metadata = excluded.metadata,
              updated_at = now()
          `,
          [
            options.season,
            squad.teamName,
            playerRegistry?.id ?? null,
            player.playerName,
            player.squadRole,
            "ipl_official_squad",
            {
              teamSlug,
              pulseId: player.pulseId,
            },
          ],
        );

        squadRows += 1;
      }
    }

    process.stdout.write(
      `${JSON.stringify({ season: options.season, squadRows }, null, 2)}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function fetchTeamSquad(
  teamSlug: string,
  season: number,
): Promise<{ teamName: string; players: SquadPlayer[] }> {
  const response = await fetch(
    `https://www.iplt20.com/teams/${teamSlug}/squad?season=${season}`,
    {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "text/html",
      },
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to fetch squad page for ${teamSlug}: ${response.status}`,
    );
  }

  const html = await response.text();
  return parseTeamSquadPage(html);
}

export function parseTeamSquadPage(html: string): {
  teamName: string;
  players: SquadPlayer[];
} {
  const $ = load(html);
  const teamName = $("h2").first().text().trim();
  const players: SquadPlayer[] = [];
  let currentRole: string | null = null;

  $("h2, a[href*='/players/']").each((_, element) => {
    const tagName = element.tagName.toLowerCase();
    if (tagName === "h2") {
      const heading = $(element).text().trim();
      if (
        heading === "Batters" ||
        heading === "All Rounders" ||
        heading === "Bowlers"
      ) {
        currentRole = heading;
      }
      return;
    }

    const href = $(element).attr("href") ?? "";
    const match = href.match(/\/players\/[^/]+\/(\d+)\s*$/u);
    const name =
      $(element).find("h2").first().text().trim() || $(element).text().trim();
    if (match === null || name.length === 0) {
      return;
    }

    players.push({
      teamName,
      playerName: normalizeWhitespace(name),
      pulseId: match[1] ?? null,
      squadRole: currentRole,
    });
  });

  const deduped = new Map<string, SquadPlayer>();
  for (const player of players) {
    deduped.set(player.playerName, player);
  }

  return {
    teamName,
    players: Array.from(deduped.values()),
  };
}

function normalizeRole(value: string | null): string | null {
  if (value === null) {
    return null;
  }
  const normalized = value.toLowerCase();
  if (normalized === "batters") return "batter";
  if (normalized === "all rounders") return "all_rounder";
  if (normalized === "bowlers") return "bowler";
  return null;
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/gu, " ").trim();
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
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Import IPL squads failed: ${message}`);
  process.exitCode = 1;
});
