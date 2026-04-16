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
      squad_row_id: number;
      season: number;
      team_name: string;
      source_player_name: string;
      current_player_registry_id: number | null;
      pulse_id: string | null;
      matched_player_registry_id: number | null;
      matched_cricsheet_player_id: string | null;
      matched_metadata: Record<string, unknown> | null;
    }>(
      `
        select
          tss.id as squad_row_id,
          tss.season,
          tss.team_name,
          tss.source_player_name,
          tss.player_registry_id as current_player_registry_id,
          tss.metadata->>'pulseId' as pulse_id,
          pr_match.id as matched_player_registry_id,
          pr_match.cricsheet_player_id as matched_cricsheet_player_id,
          pr_match.metadata as matched_metadata
        from team_season_squads tss
        left join player_registry pr_current on pr_current.id = tss.player_registry_id
        join player_registry pr_match
          on lower(pr_match.canonical_name) = lower(tss.source_player_name)
        where tss.season between $1 and $2
          and pr_match.id <> coalesce(tss.player_registry_id, -1)
          and pr_match.cricsheet_player_id not like 'pulse:%'
      `,
      [options.seasonFrom, options.seasonTo],
    );

    let reconciled = 0;
    for (const row of result.rows) {
      const mergedExternalIds = mergeExternalIds(
        row.matched_metadata,
        row.pulse_id,
      );

      await pool.query(
        `
          update player_registry
          set metadata = jsonb_set(coalesce(metadata,'{}'::jsonb), '{externalIds}', $2::jsonb, true),
              updated_at = now()
          where id = $1
        `,
        [row.matched_player_registry_id, JSON.stringify(mergedExternalIds)],
      );

      await pool.query(
        `
          update team_season_squads
          set player_registry_id = $2,
              updated_at = now()
          where id = $1
        `,
        [row.squad_row_id, row.matched_player_registry_id],
      );

      reconciled += 1;
    }

    process.stdout.write(`${JSON.stringify({ reconciled }, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function mergeExternalIds(
  metadata: Record<string, unknown> | null,
  pulseId: string | null,
): Record<string, string> {
  const raw = metadata?.["externalIds"];
  const base =
    typeof raw === "object" && raw !== null && !Array.isArray(raw)
      ? Object.fromEntries(
          Object.entries(raw).filter(
            (entry): entry is [string, string] =>
              typeof entry[1] === "string" && entry[1].length > 0,
          ),
        )
      : {};

  if (pulseId !== null && pulseId.length > 0) {
    return {
      ...base,
      pulseId,
    };
  }

  return base;
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasonFrom = 2020;
  let seasonTo = 2026;

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
      `Unknown argument "${argument}". Expected --season-from, --season-to.`,
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

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Reconcile squad player registry failed: ${message}`);
  process.exitCode = 1;
});
