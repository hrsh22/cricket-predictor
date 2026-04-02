import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
  output: "csv" | "json";
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const result = await pool.query<{
      player_registry_id: number;
      cricsheet_player_id: string;
      canonical_name: string;
      player_role: string | null;
      batting_style: string | null;
      bowling_style: string | null;
      bowling_type_group: string | null;
      external_ids: unknown;
      appearances: number;
      latest_season: number;
    }>(
      `
        select
          pr.id as player_registry_id,
          pr.cricsheet_player_id,
          pr.canonical_name,
          pr.player_role,
          pr.batting_style,
          pr.bowling_style,
          pr.bowling_type_group,
          pr.metadata->'externalIds' as external_ids,
          count(*) as appearances,
          max(cm.season) as latest_season
        from player_registry pr
        join match_player_appearances mpa on mpa.player_registry_id = pr.id
        join canonical_matches cm on cm.id = mpa.canonical_match_id
        left join player_style_profiles psp on psp.player_registry_id = pr.id
        where cm.season between $1 and $2
          and psp.player_registry_id is null
        group by
          pr.id,
          pr.cricsheet_player_id,
          pr.canonical_name,
          pr.player_role,
          pr.batting_style,
          pr.bowling_style,
          pr.bowling_type_group,
          pr.metadata->'externalIds'
        order by latest_season desc, appearances desc, pr.canonical_name asc
      `,
      [options.seasonFrom, options.seasonTo],
    );

    if (options.output === "json") {
      process.stdout.write(`${JSON.stringify(result.rows, null, 2)}\n`);
      return;
    }

    const header = [
      "player_registry_id",
      "cricsheet_player_id",
      "canonical_name",
      "current_player_role",
      "current_batting_style",
      "current_bowling_style",
      "current_bowling_type_group",
      "appearances",
      "latest_season",
      "external_ids_json",
      "source",
      "batting_hand",
      "bowling_arm",
      "bowling_style",
      "bowling_type_group",
      "player_role",
      "confidence",
      "notes",
    ];
    const lines = [header.join(",")];

    for (const row of result.rows) {
      lines.push(
        [
          row.player_registry_id,
          row.cricsheet_player_id,
          row.canonical_name,
          row.player_role ?? "",
          row.batting_style ?? "",
          row.bowling_style ?? "",
          row.bowling_type_group ?? "",
          row.appearances,
          row.latest_season,
          JSON.stringify(row.external_ids ?? {}),
          "curated_manual",
          "",
          "",
          "",
          "",
          "",
          "1.0",
          "",
        ]
          .map(csvEscape)
          .join(","),
      );
    }

    process.stdout.write(`${lines.join("\n")}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom = 2024;
  let seasonTo = 2025;
  let output: "csv" | "json" = "csv";

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
    if (argument === "--output") {
      const value = argv[index + 1];
      if (value !== "csv" && value !== "json") {
        throw new Error('--output must be either "csv" or "json".');
      }
      output = value;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from, --season-to, optional --output.`,
    );
  }

  return { seasonFrom, seasonTo, output };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  if (!/[",\n]/u.test(text)) {
    return text;
  }

  return `"${text.replace(/"/gu, '""')}"`;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Export player-style candidates failed: ${message}`);
  process.exitCode = 1;
});
