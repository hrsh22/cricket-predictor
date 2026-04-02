import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";
interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
  limit: number | null;
  roleSource: "none" | "pulse_experimental";
}

interface RegisterRow {
  identifier: string;
  key_cricinfo: string;
  key_cricbuzz: string;
  key_pulse: string;
}

interface PlayerRow {
  id: number;
  cricsheet_player_id: string;
  canonical_name: string;
  metadata: Record<string, unknown>;
  player_role: string | null;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const register = await loadCricsheetRegister();
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const players = await loadPlayers(pool, options);
    let updated = 0;
    let roleEnriched = 0;

    for (const player of players) {
      const registerRow = register.get(player.cricsheet_player_id);
      const externalIds = {
        cricinfoId: emptyToNull(registerRow?.key_cricinfo),
        cricbuzzId: emptyToNull(registerRow?.key_cricbuzz),
        pulseId: emptyToNull(registerRow?.key_pulse),
      };

      const nextMetadata = {
        ...player.metadata,
        externalIds,
      };

      let nextRole = player.player_role;
      if (
        options.roleSource === "pulse_experimental" &&
        externalIds.pulseId !== null
      ) {
        const specialization = await fetchIplSpecialization(
          externalIds.pulseId,
          player.canonical_name,
        );
        const normalizedRole = normalizeSpecialization(specialization);
        if (normalizedRole !== null) {
          nextRole = normalizedRole;
          roleEnriched += 1;
        }
      }

      await pool.query(
        `
          update player_registry
          set
            metadata = $2,
            player_role = $3,
            updated_at = now()
          where id = $1
        `,
        [player.id, nextMetadata, nextRole],
      );
      updated += 1;
    }

    process.stdout.write(
      `${JSON.stringify(
        {
          scannedPlayers: players.length,
          updatedPlayers: updated,
          roleEnriched,
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

async function loadCricsheetRegister(): Promise<Map<string, RegisterRow>> {
  const response = await fetch("https://cricsheet.org/register/people.csv");
  if (!response.ok) {
    throw new Error(`Failed to fetch Cricsheet register: ${response.status}`);
  }

  const text = await response.text();
  const lines = text.trim().split(/\r?\n/u);
  const header = lines[0]?.split(",") ?? [];
  const idIndex = header.indexOf("identifier");
  const cricinfoIndex = header.indexOf("key_cricinfo");
  const cricbuzzIndex = header.indexOf("key_cricbuzz");
  const pulseIndex = header.indexOf("key_pulse");

  const rows = new Map<string, RegisterRow>();
  for (const line of lines.slice(1)) {
    const columns = line.split(",");
    const identifier = columns[idIndex]?.trim() ?? "";
    if (identifier.length === 0) {
      continue;
    }

    rows.set(identifier, {
      identifier,
      key_cricinfo: columns[cricinfoIndex]?.trim() ?? "",
      key_cricbuzz: columns[cricbuzzIndex]?.trim() ?? "",
      key_pulse: columns[pulseIndex]?.trim() ?? "",
    });
  }

  return rows;
}

async function loadPlayers(
  pool: ReturnType<typeof createPgPool>,
  options: CliOptions,
): Promise<PlayerRow[]> {
  const result = await pool.query<PlayerRow>(
    `
      select distinct pr.id, pr.cricsheet_player_id, pr.canonical_name, pr.metadata, pr.player_role
      from player_registry pr
      join match_player_appearances mpa on mpa.player_registry_id = pr.id
      join canonical_matches cm on cm.id = mpa.canonical_match_id
      where cm.season between $1 and $2
      order by pr.id asc
      ${options.limit === null ? "" : "limit $3"}
    `,
    options.limit === null
      ? [options.seasonFrom, options.seasonTo]
      : [options.seasonFrom, options.seasonTo, options.limit],
  );

  return result.rows;
}

async function fetchIplSpecialization(
  pulseId: string,
  canonicalName: string,
): Promise<string | null> {
  const response = await fetch(
    `https://www.iplt20.com/players/${slugify(canonicalName)}/${pulseId}`,
    {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "text/html",
      },
    },
  );
  if (!response.ok) {
    return null;
  }

  const html = await response.text();
  return parseIplSpecialization(html);
}

export function parseIplSpecialization(html: string): string | null {
  const match = html.match(
    /<div class="grid-items">\s*<p>([^<]+)<\/p>\s*<span>Specialization<\/span>/iu,
  );
  const value = match?.[1]?.trim() ?? null;
  return value === null || value.length === 0 ? null : value;
}

function normalizeSpecialization(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === "batter" || normalized === "batsman") {
    return "batter";
  }
  if (normalized === "bowler") {
    return "bowler";
  }
  if (normalized === "all rounder" || normalized === "all-rounder") {
    return "all_rounder";
  }
  if (
    normalized === "wicketkeeper batter" ||
    normalized === "wk-batter" ||
    normalized === "wicketkeeper-batter"
  ) {
    return "wicketkeeper_batter";
  }

  return normalized.replace(/[^a-z0-9]+/gu, "_").replace(/^_+|_+$/gu, "");
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom = 2020;
  let seasonTo = 2025;
  let limit: number | null = null;
  let roleSource: "none" | "pulse_experimental" = "none";

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
    if (argument === "--role-source") {
      const value = argv[index + 1];
      if (value !== "none" && value !== "pulse_experimental") {
        throw new Error(
          '--role-source must be either "none" or "pulse_experimental".',
        );
      }
      roleSource = value;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from, --season-to, optional --limit, --role-source.`,
    );
  }

  return { seasonFrom, seasonTo, limit, roleSource };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

function emptyToNull(value: string | undefined): string | null {
  if (value === undefined) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Player enrichment failed: ${message}`);
  process.exitCode = 1;
});
