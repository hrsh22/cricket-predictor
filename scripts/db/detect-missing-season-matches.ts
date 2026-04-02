import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  season: number;
  sourceUrl: string;
}

interface CanonicalMatchRow {
  source_match_id: string | null;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
}

interface CricsheetMatch {
  info?: {
    season?: string;
    teams?: string[];
    dates?: string[];
    match_type?: string;
    gender?: string;
    venue?: string;
    city?: string;
    outcome?: { winner?: string; result?: string; eliminator?: string };
  };
}

interface CricsheetRow {
  sourceMatchId: string;
  season: number;
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
  winningTeamName: string | null;
  resultType: string | null;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));

  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);
  const tempDir = await mkdtemp(join(tmpdir(), "ipl-missing-detector-"));
  const zipPath = join(tempDir, "ipl_json.zip");
  const extractDir = join(tempDir, "extract");

  try {
    const existing = await loadExistingSeasonMatches(pool, options.season);
    await downloadZip(options.sourceUrl, zipPath);
    await unzipArchive(zipPath, extractDir);
    const cricsheetRows = await loadCricsheetSeason(extractDir, options.season);

    const existingIds = new Set(
      existing
        .map((row) => row.source_match_id)
        .filter((value): value is string => value !== null),
    );

    const missingBySourceId = cricsheetRows.filter(
      (row) => !existingIds.has(row.sourceMatchId),
    );

    const existingKeySet = new Set(
      existing.map((row) =>
        toMatchKey({
          scheduledStart: row.scheduled_start.toISOString(),
          teamAName: row.team_a_name,
          teamBName: row.team_b_name,
        }),
      ),
    );

    const missingByMatchKey = cricsheetRows.filter(
      (row) => !existingKeySet.has(toMatchKey(row)),
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          season: options.season,
          existingSeasonRows: existing.length,
          existingWithSourceMatchId: existingIds.size,
          cricsheetSeasonRows: cricsheetRows.length,
          missingBySourceIdCount: missingBySourceId.length,
          missingBySourceId: missingBySourceId,
          missingByMatchKeyCount: missingByMatchKey.length,
          missingByMatchKey: missingByMatchKey,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
    await rm(tempDir, { recursive: true, force: true });
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season = 2024;
  let sourceUrl = "https://cricsheet.org/downloads/ipl_json.zip";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--season") {
      const parsed = Number.parseInt(argv[index + 1] ?? "", 10);
      if (!Number.isInteger(parsed)) {
        throw new Error("--season requires an integer value.");
      }

      season = parsed;
      index += 1;
      continue;
    }

    if (argument === "--source-url") {
      sourceUrl = argv[index + 1] ?? sourceUrl;
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument \"${argument}\".`);
  }

  return { season, sourceUrl };
}

async function loadExistingSeasonMatches(
  pool: ReturnType<typeof createPgPool>,
  season: number,
): Promise<CanonicalMatchRow[]> {
  const result = await pool.query<CanonicalMatchRow>(
    `
      select source_match_id, scheduled_start, team_a_name, team_b_name
      from canonical_matches
      where competition = 'IPL'
        and season = $1
      order by scheduled_start asc
    `,
    [season],
  );

  return result.rows;
}

async function downloadZip(
  sourceUrl: string,
  outputPath: string,
): Promise<void> {
  const response = await fetch(sourceUrl);
  if (!response.ok) {
    throw new Error(`Failed to download Cricsheet archive: ${response.status}`);
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  await writeFile(outputPath, buffer);
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

async function loadCricsheetSeason(
  extractDir: string,
  season: number,
): Promise<CricsheetRow[]> {
  const entries = await readdir(extractDir, {
    recursive: true,
    withFileTypes: true,
  });
  const rows: CricsheetRow[] = [];

  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) {
      continue;
    }

    const sourceMatchId = entry.name.replace(/\.json$/i, "");
    const fullPath = join(entry.parentPath, entry.name);
    const parsed = JSON.parse(
      await readFile(fullPath, "utf8"),
    ) as CricsheetMatch;
    const row = normalizeCricsheetMatch(parsed, sourceMatchId);

    if (row !== null && row.season === season) {
      rows.push(row);
    }
  }

  rows.sort((left, right) =>
    left.scheduledStart.localeCompare(right.scheduledStart),
  );
  return rows;
}

function normalizeCricsheetMatch(
  match: CricsheetMatch,
  sourceMatchId: string,
): CricsheetRow | null {
  const info = match.info;
  if (
    info === undefined ||
    info.gender !== "male" ||
    info.match_type !== "T20"
  ) {
    return null;
  }

  const season = Number.parseInt(info.season ?? "", 10);
  if (!Number.isInteger(season)) {
    return null;
  }

  if (!Array.isArray(info.teams) || info.teams.length !== 2) {
    return null;
  }

  const date = info.dates?.[0];
  if (typeof date !== "string") {
    return null;
  }

  const scheduledStart = new Date(`${date}T14:00:00.000Z`).toISOString();
  const teamAName = normalizeTeamName(info.teams[0] ?? "");
  const teamBName = normalizeTeamName(info.teams[1] ?? "");

  const outcome = info.outcome ?? {};
  const resultType =
    outcome.result === "tie"
      ? outcome.eliminator === undefined
        ? "tie"
        : "super_over"
      : outcome.result === "no result"
        ? "no_result"
        : outcome.winner !== undefined
          ? "win"
          : null;

  return {
    sourceMatchId,
    season,
    scheduledStart,
    teamAName,
    teamBName,
    winningTeamName:
      outcome.winner === undefined ? null : normalizeTeamName(outcome.winner),
    resultType,
  };
}

function normalizeTeamName(value: string): string {
  const trimmed = value.trim();

  if (trimmed === "Royal Challengers Bangalore") {
    return "Royal Challengers Bengaluru";
  }

  if (trimmed === "Kings XI Punjab") {
    return "Punjab Kings";
  }

  if (trimmed === "Delhi Daredevils") {
    return "Delhi Capitals";
  }

  return trimmed;
}

function toMatchKey(input: {
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
}): string {
  const day = input.scheduledStart.slice(0, 10);
  const a = input.teamAName.toLowerCase();
  const b = input.teamBName.toLowerCase();
  const ordered = [a, b].sort().join("::");
  return `${day}::${ordered}`;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Missing-season detector failed: ${message}`);
  process.exitCode = 1;
});
