import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalMatch } from "../../src/domain/match.js";
import { createNormalizedRepository } from "../../src/repositories/normalized.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  seasons: number[];
  dryRun: boolean;
  sourceUrl: string;
}

interface CricsheetMatch {
  info?: {
    city?: string;
    dates?: string[];
    event?: { match_number?: number };
    match_type?: string;
    season?: string | number;
    teams?: string[];
    toss?: { winner?: string; decision?: string };
    venue?: string;
    outcome?: {
      winner?: string;
      result?: string;
      eliminator?: string;
    };
    gender?: string;
  };
}

interface NormalizedImportRow {
  season: number;
  sourceMatchId: string;
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
  venueName: string | null;
  tossWinnerTeamName: string | null;
  tossDecision: "bat" | "bowl" | null;
  winningTeamName: string | null;
  resultType: "win" | "tie" | "no_result" | "abandoned" | "super_over" | null;
  status: "completed" | "abandoned" | "no_result";
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const tempDir = await mkdtemp(join(tmpdir(), "ipl-cricsheet-"));
  const zipPath = join(tempDir, "ipl_json.zip");
  const extractDir = join(tempDir, "extract");

  try {
    await downloadZip(options.sourceUrl, zipPath);
    await unzipArchive(zipPath, extractDir);

    const rows = await readCricsheetRows(extractDir, options.seasons);
    const uniqueRows = dedupeBySourceMatchId(rows);

    if (options.dryRun) {
      process.stdout.write(
        `${JSON.stringify(
          {
            dryRun: true,
            seasons: options.seasons,
            discoveredRows: rows.length,
            uniqueRows: uniqueRows.length,
            sample: uniqueRows.slice(0, 3),
          },
          null,
          2,
        )}\n`,
      );
      return;
    }

    const config = loadAppConfig();
    const pool = createPgPool(config.databaseUrl);

    try {
      const normalizedRepository = createNormalizedRepository(pool);
      let persisted = 0;

      for (const row of uniqueRows) {
        const matchSlug = buildMatchSlug(row);
        const canonical = parseCanonicalMatch({
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

        await normalizedRepository.saveCanonicalMatch(canonical);
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
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let seasons: number[] = [2020, 2021, 2025];
  let dryRun = false;
  let sourceUrl = "https://cricsheet.org/downloads/ipl_json.zip";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--seasons") {
      const raw = argv[index + 1] ?? "";
      seasons = raw
        .split(",")
        .map((token) => Number.parseInt(token.trim(), 10))
        .filter((value) => Number.isInteger(value));
      index += 1;
      continue;
    }

    if (argument === "--source-url") {
      sourceUrl = argv[index + 1] ?? sourceUrl;
      index += 1;
      continue;
    }

    if (argument === "--dry-run") {
      dryRun = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --seasons <csv>, optional --source-url <url>, optional --dry-run.`,
    );
  }

  if (seasons.length === 0) {
    throw new Error("--seasons must include at least one integer season.");
  }

  return { seasons, dryRun, sourceUrl };
}

async function downloadZip(
  sourceUrl: string,
  outputPath: string,
): Promise<void> {
  const response = await fetch(sourceUrl);
  if (!response.ok) {
    throw new Error(`Failed to download Cricsheet archive: ${response.status}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  await writeFile(outputPath, Buffer.from(arrayBuffer));
}

async function unzipArchive(zipPath: string, outputDir: string): Promise<void> {
  const child = spawn("unzip", ["-q", zipPath, "-d", outputDir], {
    stdio: ["ignore", "pipe", "pipe"],
  });

  const exitCode: number = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolve(code ?? 1));
  });

  if (exitCode !== 0) {
    throw new Error(
      `Failed to unzip Cricsheet archive with exit code ${exitCode}.`,
    );
  }
}

async function readCricsheetRows(
  extractDir: string,
  seasons: readonly number[],
): Promise<NormalizedImportRow[]> {
  const jsonFiles = await collectJsonFiles(extractDir);
  const rows: NormalizedImportRow[] = [];

  for (const filePath of jsonFiles) {
    const raw = await readFile(filePath.fullPath, "utf8");
    const parsed = JSON.parse(raw) as CricsheetMatch;
    const normalized = normalizeCricsheetMatch(parsed, filePath.sourceMatchId);

    if (normalized === null || !seasons.includes(normalized.season)) {
      continue;
    }

    rows.push(normalized);
  }

  return rows;
}

async function collectJsonFiles(
  rootDir: string,
): Promise<Array<{ fullPath: string; sourceMatchId: string }>> {
  const entries = await readdir(rootDir, {
    recursive: true,
    withFileTypes: true,
  });
  const files: Array<{ fullPath: string; sourceMatchId: string }> = [];

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    if (!entry.name.toLowerCase().endsWith(".json")) {
      continue;
    }

    const sourceMatchId = entry.name.replace(/\.json$/i, "");
    files.push({
      fullPath: join(entry.parentPath, entry.name),
      sourceMatchId,
    });
  }

  return files;
}

export function normalizeCricsheetMatch(
  match: CricsheetMatch,
  sourceMatchId: string,
): NormalizedImportRow | null {
  const info = match.info;
  if (info === undefined) {
    return null;
  }

  if (info.gender !== "male" || info.match_type !== "T20") {
    return null;
  }

  const scheduledStart = normalizeDate(info.dates?.[0] ?? null);
  const season = parseSeason(info.season, scheduledStart);
  const teams = normalizeTeams(info.teams);
  if (season === null || teams === null || scheduledStart === null) {
    return null;
  }

  const outcome = info.outcome ?? {};
  const resultType = normalizeResultType(outcome);
  const status = normalizeStatus(resultType);

  return {
    season,
    sourceMatchId,
    scheduledStart,
    teamAName: normalizeTeamName(teams[0]),
    teamBName: normalizeTeamName(teams[1]),
    venueName: normalizeVenueName(info.venue ?? null, info.city ?? null),
    tossWinnerTeamName:
      info.toss?.winner === undefined
        ? null
        : normalizeTeamName(info.toss.winner),
    tossDecision: normalizeTossDecision(info.toss?.decision ?? null),
    winningTeamName:
      outcome.winner === undefined ? null : normalizeTeamName(outcome.winner),
    resultType,
    status,
  };
}

function dedupeBySourceMatchId(
  rows: readonly NormalizedImportRow[],
): NormalizedImportRow[] {
  const seen = new Set<string>();
  const deduped: NormalizedImportRow[] = [];

  for (const row of rows) {
    if (seen.has(row.sourceMatchId)) {
      continue;
    }

    seen.add(row.sourceMatchId);
    deduped.push(row);
  }

  deduped.sort((left, right) =>
    left.scheduledStart.localeCompare(right.scheduledStart),
  );

  return deduped;
}

export function parseSeason(
  raw: string | number | undefined,
  scheduledStart: string | null,
): number | null {
  if (typeof raw === "number" && Number.isInteger(raw)) {
    return raw;
  }

  if (typeof raw !== "string") {
    if (scheduledStart !== null) {
      const dateYear = new Date(scheduledStart).getUTCFullYear();
      if (Number.isInteger(dateYear)) {
        return dateYear;
      }
    }

    return null;
  }

  const normalized = raw.trim();
  const splitSeason = normalized.match(/^(\d{4})\s*\/\s*(\d{2,4})$/u);
  if (splitSeason !== null) {
    if (scheduledStart !== null) {
      const scheduledYear = new Date(scheduledStart).getUTCFullYear();
      if (Number.isInteger(scheduledYear)) {
        return scheduledYear;
      }
    }

    const firstYear = Number.parseInt(splitSeason[1] ?? "", 10);
    const secondToken = splitSeason[2] ?? "";
    const secondYear =
      secondToken.length === 2
        ? Math.floor(firstYear / 100) * 100 + Number.parseInt(secondToken, 10)
        : Number.parseInt(secondToken, 10);

    if (Number.isInteger(secondYear)) {
      return secondYear;
    }
  }

  const parsed = Number.parseInt(normalized, 10);
  if (Number.isInteger(parsed)) {
    return parsed;
  }

  if (scheduledStart !== null) {
    const dateYear = new Date(scheduledStart).getUTCFullYear();
    if (Number.isInteger(dateYear)) {
      return dateYear;
    }
  }

  return null;
}

function normalizeTeams(teams: string[] | undefined): [string, string] | null {
  if (!Array.isArray(teams) || teams.length !== 2) {
    return null;
  }

  if (typeof teams[0] !== "string" || typeof teams[1] !== "string") {
    return null;
  }

  return [teams[0], teams[1]];
}

function normalizeDate(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const parsed = new Date(`${value}T14:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function normalizeTeamName(value: string): string {
  const trimmed = value.trim();

  if (trimmed === "Royal Challengers Bangalore") {
    return "Royal Challengers Bengaluru";
  }

  if (trimmed === "Delhi Daredevils") {
    return "Delhi Capitals";
  }

  if (trimmed === "Kings XI Punjab") {
    return "Punjab Kings";
  }

  return trimmed;
}

function normalizeVenueName(
  venue: string | null,
  city: string | null,
): string | null {
  const venueTrimmed = venue?.trim() ?? "";
  const cityTrimmed = city?.trim() ?? "";

  if (venueTrimmed.length === 0 && cityTrimmed.length === 0) {
    return null;
  }

  if (venueTrimmed.length > 0 && cityTrimmed.length > 0) {
    return `${venueTrimmed}, ${cityTrimmed}`;
  }

  return venueTrimmed.length > 0 ? venueTrimmed : cityTrimmed;
}

function normalizeTossDecision(value: string | null): "bat" | "bowl" | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === "bat") {
    return "bat";
  }

  if (normalized === "field" || normalized === "bowl") {
    return "bowl";
  }

  return null;
}

function normalizeResultType(
  outcome: NonNullable<CricsheetMatch["info"]>["outcome"],
): "win" | "tie" | "no_result" | "abandoned" | "super_over" | null {
  if (outcome === undefined) {
    return null;
  }

  if (outcome.result === "tie") {
    return outcome.eliminator === undefined ? "tie" : "super_over";
  }

  if (outcome.result === "no result") {
    return "no_result";
  }

  if (outcome.winner !== undefined) {
    return "win";
  }

  return null;
}

function normalizeStatus(
  resultType: "win" | "tie" | "no_result" | "abandoned" | "super_over" | null,
): "completed" | "abandoned" | "no_result" {
  if (resultType === "no_result") {
    return "no_result";
  }

  if (resultType === "abandoned") {
    return "abandoned";
  }

  return "completed";
}

function buildMatchSlug(row: NormalizedImportRow): string {
  const teamASlug = slugify(row.teamAName);
  const teamBSlug = slugify(row.teamBName);
  return `ipl-${row.season}-${teamASlug}-vs-${teamBSlug}-${row.sourceMatchId}`;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Cricsheet import failed: ${message}`);
  process.exitCode = 1;
});
