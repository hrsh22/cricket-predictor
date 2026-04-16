import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const result = await pool.query<{
      player_registry_id: number;
      player_role: string | null;
      batting_style: string | null;
      bowling_style: string | null;
      bowling_type_group: string | null;
      appearances: number;
    }>(
      `
        select
          pr.id as player_registry_id,
          pr.player_role,
          pr.batting_style,
          pr.bowling_style,
          pr.bowling_type_group,
          count(*) as appearances
        from player_registry pr
        left join match_player_appearances mpa on mpa.player_registry_id = pr.id
        left join canonical_matches cm on cm.id = mpa.canonical_match_id
        left join team_season_squads tss on tss.player_registry_id = pr.id
        where (
          cm.season between $1 and $2
          or tss.season between $1 and $2
        )
          and (
            pr.player_role is not null
            or pr.batting_style is not null
            or pr.bowling_style is not null
            or pr.bowling_type_group is not null
          )
        group by
          pr.id,
          pr.player_role,
          pr.batting_style,
          pr.bowling_style,
          pr.bowling_type_group
      `,
      [options.seasonFrom, options.seasonTo],
    );

    let upserted = 0;
    for (const row of result.rows) {
      await pool.query(
        `
          insert into player_style_profiles (
            player_registry_id,
            source,
            batting_hand,
            bowling_style,
            bowling_type_group,
            player_role,
            confidence,
            notes,
            metadata
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          on conflict (player_registry_id) do update set
            source = excluded.source,
            batting_hand = coalesce(excluded.batting_hand, player_style_profiles.batting_hand),
            bowling_style = coalesce(excluded.bowling_style, player_style_profiles.bowling_style),
            bowling_type_group = coalesce(excluded.bowling_type_group, player_style_profiles.bowling_type_group),
            player_role = coalesce(excluded.player_role, player_style_profiles.player_role),
            confidence = excluded.confidence,
            notes = excluded.notes,
            metadata = excluded.metadata,
            updated_at = now()
        `,
        [
          row.player_registry_id,
          "external_verified",
          normalizeBattingHand(row.batting_style),
          row.bowling_style,
          row.bowling_type_group,
          row.player_role,
          row.batting_style !== null || row.bowling_style !== null ? 0.9 : 0.75,
          `Auto-seeded from enriched player_registry across ${row.appearances} appearances`,
          {
            source: "seed-player-style-profiles",
            appearances: row.appearances,
          },
        ],
      );
      upserted += 1;
    }

    process.stdout.write(`${JSON.stringify({ upserted }, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom = 2024;
  let seasonTo = 2025;

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

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from and --season-to.`,
    );
  }

  return { seasonFrom, seasonTo };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

function normalizeBattingHand(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const normalized = value.toLowerCase();
  if (normalized.includes("left")) {
    return "left";
  }
  if (normalized.includes("right")) {
    return "right";
  }
  return null;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Seed player style profiles failed: ${message}`);
  process.exitCode = 1;
});
