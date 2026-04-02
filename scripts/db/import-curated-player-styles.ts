import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  filePath: string;
}

interface StyleCsvRow {
  player_registry_id: string;
  source: string;
  batting_hand: string;
  bowling_arm: string;
  bowling_style: string;
  bowling_type_group: string;
  player_role: string;
  confidence: string;
  notes: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const csv = await readFile(resolve(options.filePath), "utf8");
  const rows = parseCsv(csv);
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    let upserted = 0;
    for (const row of rows) {
      const playerRegistryId = Number.parseInt(row.player_registry_id, 10);
      if (!Number.isInteger(playerRegistryId)) {
        continue;
      }

      const confidence = parseConfidence(row.confidence);
      await pool.query(
        `
          insert into player_style_profiles (
            player_registry_id,
            source,
            batting_hand,
            bowling_arm,
            bowling_style,
            bowling_type_group,
            player_role,
            confidence,
            notes,
            metadata
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          on conflict (player_registry_id) do update set
            source = excluded.source,
            batting_hand = excluded.batting_hand,
            bowling_arm = excluded.bowling_arm,
            bowling_style = excluded.bowling_style,
            bowling_type_group = excluded.bowling_type_group,
            player_role = excluded.player_role,
            confidence = excluded.confidence,
            notes = excluded.notes,
            metadata = excluded.metadata,
            updated_at = now()
        `,
        [
          playerRegistryId,
          normalizeSource(row.source),
          emptyToNull(row.batting_hand),
          emptyToNull(row.bowling_arm),
          emptyToNull(row.bowling_style),
          emptyToNull(row.bowling_type_group),
          emptyToNull(row.player_role),
          confidence,
          emptyToNull(row.notes),
          { sourceFile: resolve(options.filePath) },
        ],
      );

      await pool.query(
        `
          update player_registry
          set
            batting_style = coalesce($2, batting_style),
            bowling_style = coalesce($3, bowling_style),
            bowling_type_group = coalesce($4, bowling_type_group),
            player_role = coalesce($5, player_role),
            updated_at = now()
          where id = $1
        `,
        [
          playerRegistryId,
          emptyToNull(row.batting_hand),
          emptyToNull(row.bowling_style),
          emptyToNull(row.bowling_type_group),
          emptyToNull(row.player_role),
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
  let filePath: string | null = null;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--file") {
      const value = argv[index + 1];
      if (typeof value !== "string" || value.trim().length === 0) {
        throw new Error("--file requires a non-empty value.");
      }
      filePath = value.trim();
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument "${argument}". Expected --file.`);
  }

  if (filePath === null) {
    throw new Error("Missing required --file <path> argument.");
  }

  return { filePath };
}

export function parseCsv(input: string): StyleCsvRow[] {
  const lines = input.trim().split(/\r?\n/u);
  const headerIndex = lines.findIndex((line) =>
    line.startsWith("player_registry_id,"),
  );
  if (headerIndex === -1) {
    throw new Error(
      "Curated style CSV header not found. Expected first real header line to begin with player_registry_id.",
    );
  }

  const header = parseCsvLine(lines[headerIndex] ?? "");
  const rows: StyleCsvRow[] = [];

  for (const line of lines.slice(headerIndex + 1)) {
    if (line.trim().length === 0) {
      continue;
    }
    const columns = parseCsvLine(line);
    const row = Object.fromEntries(
      header.map((key, index) => [key, columns[index] ?? ""]),
    ) as unknown as StyleCsvRow;
    rows.push(row);
  }

  return rows;
}

function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      if (inQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  result.push(current);
  return result;
}

function emptyToNull(value: string | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function normalizeSource(value: string): string {
  return value.trim().length === 0 ? "curated_import" : value.trim();
}

function parseConfidence(value: string): number {
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed)) {
    return 1;
  }
  return Math.max(0, Math.min(1, parsed));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Import curated player styles failed: ${message}`);
    process.exitCode = 1;
  });
}
