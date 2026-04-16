import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
  limit: number | null;
}

interface PlayerRow {
  id: number;
  canonical_name: string;
}

interface WikipediaStyleFields {
  battingHand: string | null;
  bowlingArm: string | null;
  bowlingStyle: string | null;
  bowlingTypeGroup: string | null;
  playerRole: string | null;
  sourcePage: string | null;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const players = await loadPlayers(pool, options);
    let updated = 0;

    for (const player of players) {
      const style = await fetchWikipediaStyle(player.canonical_name);
      if (
        style.battingHand === null &&
        style.bowlingStyle === null &&
        style.playerRole === null
      ) {
        continue;
      }

      await pool.query(
        `
          update player_registry
          set
            batting_style = coalesce($2, batting_style),
            bowling_style = coalesce($3, bowling_style),
            bowling_type_group = coalesce($4, bowling_type_group),
            player_role = coalesce($5, player_role),
            metadata = metadata::jsonb || jsonb_build_object(
              'styleSource', $6::text,
              'wikipediaTitle', $7::text
            ),
            updated_at = now()
          where id = $1
        `,
        [
          player.id,
          style.battingHand,
          style.bowlingStyle,
          style.bowlingTypeGroup,
          style.playerRole,
          "wikipedia_infobox",
          style.sourcePage ?? "",
        ],
      );
      updated += 1;
    }

    process.stdout.write(
      `${JSON.stringify(
        {
          scanned: players.length,
          updated,
          seasonFrom: options.seasonFrom,
          seasonTo: options.seasonTo,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function loadPlayers(
  pool: ReturnType<typeof createPgPool>,
  options: CliOptions,
): Promise<PlayerRow[]> {
  const result = await pool.query<PlayerRow>(
    `
      select distinct pr.id, pr.canonical_name
      from player_registry pr
      join team_season_squads tss on tss.player_registry_id = pr.id
      where tss.season between $1 and $2
        and (pr.batting_style is null or pr.bowling_style is null or pr.bowling_type_group is null)
      order by pr.canonical_name asc
      ${options.limit === null ? "" : "limit $3"}
    `,
    options.limit === null
      ? [options.seasonFrom, options.seasonTo]
      : [options.seasonFrom, options.seasonTo, options.limit],
  );
  return result.rows;
}

async function fetchWikipediaStyle(
  canonicalName: string,
): Promise<WikipediaStyleFields> {
  const searchTitle = await searchWikipediaTitle(`${canonicalName} cricketer`);
  if (searchTitle === null) {
    return emptyStyle();
  }

  const wikitext = await fetchWikipediaWikitext(searchTitle);
  if (wikitext === null) {
    return emptyStyle();
  }

  return {
    ...parseWikipediaInfoboxFields(wikitext),
    sourcePage: searchTitle,
  };
}

async function searchWikipediaTitle(query: string): Promise<string | null> {
  const url = new URL("https://en.wikipedia.org/w/api.php");
  url.searchParams.set("action", "query");
  url.searchParams.set("list", "search");
  url.searchParams.set("srsearch", query);
  url.searchParams.set("format", "json");
  url.searchParams.set("srlimit", "1");

  const response = await fetch(url.toString(), {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json",
    },
  });
  if (!response.ok) {
    return null;
  }

  const payload = (await response.json()) as {
    query?: { search?: Array<{ title?: string }> };
  };
  return payload.query?.search?.[0]?.title ?? null;
}

async function fetchWikipediaWikitext(title: string): Promise<string | null> {
  const url = new URL("https://en.wikipedia.org/w/api.php");
  url.searchParams.set("action", "query");
  url.searchParams.set("prop", "revisions");
  url.searchParams.set("titles", title);
  url.searchParams.set("rvslots", "main");
  url.searchParams.set("rvprop", "content");
  url.searchParams.set("format", "json");
  url.searchParams.set("formatversion", "2");

  const response = await fetch(url.toString(), {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json",
    },
  });
  if (!response.ok) {
    return null;
  }

  const payload = (await response.json()) as {
    query?: {
      pages?: Array<{
        revisions?: Array<{ slots?: { main?: { content?: string } } }>;
      }>;
    };
  };
  return (
    payload.query?.pages?.[0]?.revisions?.[0]?.slots?.main?.content ?? null
  );
}

export function parseWikipediaInfoboxFields(
  wikitext: string,
): Omit<WikipediaStyleFields, "sourcePage"> {
  const batting = extractInfoboxValue(wikitext, "batting");
  const bowling = extractInfoboxValue(wikitext, "bowling");
  const role = extractInfoboxValue(wikitext, "role");

  return {
    battingHand: normalizeBattingHand(batting),
    bowlingArm: normalizeBowlingArm(bowling),
    bowlingStyle: normalizeBowlingStyle(bowling),
    bowlingTypeGroup: normalizeBowlingTypeGroup(bowling),
    playerRole: normalizeRole(role),
  };
}

function extractInfoboxValue(wikitext: string, field: string): string | null {
  const regex = new RegExp(`^\\|\\s*${field}\\s*=\\s*(.+)$`, "imu");
  const match = wikitext.match(regex);
  if (match === null) {
    return null;
  }

  return cleanWikiValue(match[1] ?? "");
}

function cleanWikiValue(value: string): string | null {
  const truncated = value.split("|")[0] ?? value;
  const cleaned = truncated
    .replace(/\{\{[^{}]*\}\}/gu, " ")
    .replace(/\[\[([^\]|]+\|)?([^\]]+)\]\]/gu, "$2")
    .replace(/<[^>]+>/gu, " ")
    .replace(/''+/gu, "")
    .replace(/&nbsp;/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();

  return cleaned.length === 0 ? null : cleaned;
}

function normalizeBattingHand(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (normalized.includes("left")) return "left";
  if (normalized.includes("right")) return "right";
  return null;
}

function normalizeBowlingArm(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (normalized.includes("left-arm") || normalized.includes("left arm"))
    return "left";
  if (normalized.includes("right-arm") || normalized.includes("right arm"))
    return "right";
  return null;
}

function normalizeBowlingStyle(value: string | null): string | null {
  if (value === null) return null;
  if (value.includes("role =")) {
    return null;
  }
  return value;
}

function normalizeBowlingTypeGroup(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (
    normalized.includes("orthodox") ||
    normalized.includes("legbreak") ||
    normalized.includes("leg break") ||
    normalized.includes("offbreak") ||
    normalized.includes("off break") ||
    normalized.includes("off spin") ||
    normalized.includes("chinaman") ||
    normalized.includes("slow left-arm")
  ) {
    return "spin";
  }
  if (
    normalized.includes("medium") ||
    normalized.includes("fast") ||
    normalized.includes("seam")
  ) {
    return "pace";
  }
  return null;
}

function normalizeRole(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (
    normalized.includes("wicket") &&
    (normalized.includes("batter") || normalized.includes("batsman"))
  ) {
    return "wicketkeeper_batter";
  }
  if (
    normalized.includes("all-rounder") ||
    normalized.includes("all rounder")
  ) {
    return "all_rounder";
  }
  if (normalized.includes("bowler")) {
    return "bowler";
  }
  if (normalized.includes("batter") || normalized.includes("batsman")) {
    return "batter";
  }
  return null;
}

function emptyStyle(): WikipediaStyleFields {
  return {
    battingHand: null,
    bowlingArm: null,
    bowlingStyle: null,
    bowlingTypeGroup: null,
    playerRole: null,
    sourcePage: null,
  };
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom = 2026;
  let seasonTo = 2026;
  let limit: number | null = null;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--season-from") {
      seasonFrom = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }
    if (argument === "--season-to") {
      seasonTo = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }
    if (argument === "--limit") {
      limit = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from, --season-to, optional --limit.`,
    );
  }

  return { seasonFrom, seasonTo, limit };
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
  console.error(`Wikipedia style enrichment failed: ${message}`);
  process.exitCode = 1;
});
