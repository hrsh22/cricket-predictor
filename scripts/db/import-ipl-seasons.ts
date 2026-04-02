import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalMatch } from "../../src/domain/match.js";
import { createNormalizedRepository } from "../../src/repositories/normalized.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  inputPath: string;
  seasons: number[];
  dryRun: boolean;
}

interface RawImportRow {
  season: number;
  matchNumber: number;
  sourceMatchId: string | null;
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
  venueName: string | null;
  tossWinnerTeamName: string | null;
  tossDecision: "bat" | "bowl" | null;
  winningTeamName: string | null;
  resultType: "win" | "tie" | "no_result" | "abandoned" | "super_over" | null;
  status: "scheduled" | "in_progress" | "completed" | "abandoned" | "no_result";
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const rows = await loadImportRows(options.inputPath);
  const selected = rows.filter((row) => options.seasons.includes(row.season));

  if (selected.length === 0) {
    throw new Error(
      `No rows found for selected seasons: ${options.seasons.join(", ")}`,
    );
  }

  if (options.dryRun) {
    process.stdout.write(
      `${JSON.stringify(
        {
          dryRun: true,
          seasons: options.seasons,
          totalInputRows: rows.length,
          selectedRows: selected.length,
          sample: selected.slice(0, 3),
        },
        null,
        2,
      )}\n`,
    );
    return;
  }

  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);
  const normalizedRepository = createNormalizedRepository(pool);

  try {
    let persisted = 0;

    for (const row of selected) {
      const matchSlug = buildMatchSlug(row);

      const parsed = parseCanonicalMatch({
        competition: "IPL",
        matchSlug,
        sourceMatchId: row.sourceMatchId,
        season: row.season,
        scheduledStart: row.scheduledStart,
        teamAName: row.teamAName,
        teamBName: row.teamBName,
        venueName: row.venueName,
        status: row.status,
        tossWinnerTeamName: row.tossWinnerTeamName,
        tossDecision: row.tossDecision,
        winningTeamName: row.winningTeamName,
        resultType: row.resultType,
      });

      await normalizedRepository.saveCanonicalMatch(parsed);
      persisted += 1;
    }

    process.stdout.write(
      `${JSON.stringify(
        {
          dryRun: false,
          seasons: options.seasons,
          persistedRows: persisted,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath: string | null = null;
  let seasons: number[] = [2020, 2021, 2025];
  let dryRun = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--input") {
      inputPath = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--seasons") {
      const value = argv[index + 1] ?? "";
      seasons = value
        .split(",")
        .map((token) => Number.parseInt(token.trim(), 10))
        .filter((season) => Number.isInteger(season));
      index += 1;
      continue;
    }

    if (argument === "--dry-run") {
      dryRun = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --input, optional --seasons <csv>, optional --dry-run.`,
    );
  }

  if (inputPath === null || inputPath.trim().length === 0) {
    throw new Error("Missing required --input <path> argument.");
  }

  if (seasons.length === 0) {
    throw new Error("--seasons must include at least one integer season.");
  }

  return {
    inputPath,
    seasons,
    dryRun,
  };
}

async function loadImportRows(inputPath: string): Promise<RawImportRow[]> {
  const fileContent = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const parsed = JSON.parse(fileContent) as unknown;

  if (!Array.isArray(parsed)) {
    throw new Error("Import input must be a JSON array.");
  }

  return parsed.map((value, index) => parseImportRow(value, index));
}

function parseImportRow(value: unknown, index: number): RawImportRow {
  if (typeof value !== "object" || value === null) {
    throw new Error(`Row ${index} must be an object.`);
  }

  const row = value as Record<string, unknown>;

  const season = parseIntegerField(row, "season", index);
  const matchNumber = parseIntegerField(row, "matchNumber", index);
  const sourceMatchId = parseNullableStringField(row, "sourceMatchId", index);
  const scheduledStart = parseStringField(row, "scheduledStart", index);
  const teamAName = parseStringField(row, "teamAName", index);
  const teamBName = parseStringField(row, "teamBName", index);
  const venueName = parseNullableStringField(row, "venueName", index);
  const tossWinnerTeamName = parseNullableStringField(
    row,
    "tossWinnerTeamName",
    index,
  );
  const tossDecision = parseNullableTossDecision(row, "tossDecision", index);
  const winningTeamName = parseNullableStringField(
    row,
    "winningTeamName",
    index,
  );
  const resultType = parseNullableResultType(row, "resultType", index);
  const status = parseStatus(row, "status", index);

  return {
    season,
    matchNumber,
    sourceMatchId,
    scheduledStart,
    teamAName,
    teamBName,
    venueName,
    tossWinnerTeamName,
    tossDecision,
    winningTeamName,
    resultType,
    status,
  };
}

function parseIntegerField(
  row: Record<string, unknown>,
  field: string,
  index: number,
): number {
  const value = row[field];
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new Error(`Row ${index} field ${field} must be an integer.`);
  }

  return value;
}

function parseStringField(
  row: Record<string, unknown>,
  field: string,
  index: number,
): string {
  const value = row[field];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Row ${index} field ${field} must be a non-empty string.`);
  }

  return value.trim();
}

function parseNullableStringField(
  row: Record<string, unknown>,
  field: string,
  index: number,
): string | null {
  const value = row[field];
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    throw new Error(`Row ${index} field ${field} must be a string or null.`);
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function parseNullableTossDecision(
  row: Record<string, unknown>,
  field: string,
  index: number,
): "bat" | "bowl" | null {
  const value = row[field];
  if (value === null || value === undefined) {
    return null;
  }

  if (value !== "bat" && value !== "bowl") {
    throw new Error(
      `Row ${index} field ${field} must be 'bat', 'bowl', or null.`,
    );
  }

  return value;
}

function parseNullableResultType(
  row: Record<string, unknown>,
  field: string,
  index: number,
): "win" | "tie" | "no_result" | "abandoned" | "super_over" | null {
  const value = row[field];
  if (value === null || value === undefined) {
    return null;
  }

  if (
    value !== "win" &&
    value !== "tie" &&
    value !== "no_result" &&
    value !== "abandoned" &&
    value !== "super_over"
  ) {
    throw new Error(
      `Row ${index} field ${field} must be win|tie|no_result|abandoned|super_over|null.`,
    );
  }

  return value;
}

function parseStatus(
  row: Record<string, unknown>,
  field: string,
  index: number,
): "scheduled" | "in_progress" | "completed" | "abandoned" | "no_result" {
  const value = row[field];
  if (
    value !== "scheduled" &&
    value !== "in_progress" &&
    value !== "completed" &&
    value !== "abandoned" &&
    value !== "no_result"
  ) {
    throw new Error(
      `Row ${index} field ${field} must be scheduled|in_progress|completed|abandoned|no_result.`,
    );
  }

  return value;
}

function buildMatchSlug(row: RawImportRow): string {
  const teamASlug = slugifyToken(row.teamAName);
  const teamBSlug = slugifyToken(row.teamBName);
  return `ipl-${row.season}-${teamASlug}-vs-${teamBSlug}-${row.matchNumber}`;
}

function slugifyToken(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Season import failed: ${message}`);
  process.exitCode = 1;
});
