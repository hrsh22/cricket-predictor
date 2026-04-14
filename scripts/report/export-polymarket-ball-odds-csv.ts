import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { spawn } from "node:child_process";

import { runPolymarketHistoricalBackfill } from "../../src/ingest/polymarket/index.js";
import { generateBallOddsTimeline } from "./ball-odds-timeline.js";

interface CliOptions {
  seasonFrom: number;
  seasonTo: number;
  outputPath: string;
}

interface MatchRow {
  sourceMatchId: string;
  matchSlug: string;
  season: number;
  scheduledStart: Date;
  teamAName: string;
  teamBName: string;
}

interface ExportRow {
  season: number;
  matchDate: string;
  sourceMatchId: string;
  matchSlug: string;
  eventSlug: string;
  commentaryUrl: string;
  deliverySourceMode: string;
  inning: number;
  battingTeam: string;
  bowlingTeam: string;
  ball: string;
  event: string;
  commentary: string | null;
  timestamp: string;
  timestampSource: string;
  primaryTeam: string;
  primaryBeforePct: number | null;
  primaryAfterPct: number | null;
  primaryDeltaPct: number | null;
  secondaryTeam: string;
  secondaryBeforePct: number | null;
  secondaryAfterPct: number | null;
  secondaryDeltaPct: number | null;
  pricingSourceBefore: string | null;
  pricingSourceAfter: string | null;
}

interface CricsheetArchiveMatch {
  info?: {
    season?: string | number;
    dates?: string[];
    teams?: string[];
    gender?: string;
    match_type?: string;
    outcome?: {
      winner?: string;
      result?: string;
    };
  };
}

const TEAM_SLUGS: Record<string, string> = {
  "Chennai Super Kings": "che",
  "Delhi Capitals": "del",
  "Gujarat Titans": "guj",
  "Kolkata Knight Riders": "kol",
  "Lucknow Super Giants": "luc",
  "Mumbai Indians": "mum",
  "Punjab Kings": "pun",
  "Rajasthan Royals": "raj",
  "Royal Challengers Bengaluru": "roy",
  "Royal Challengers Bangalore": "roy",
  "Sunrisers Hyderabad": "sun",
};

const LEGACY_TEAM_CODES: Record<string, string> = {
  "Chennai Super Kings": "csk",
  "Delhi Capitals": "dc",
  "Gujarat Titans": "gt",
  "Kolkata Knight Riders": "kkr",
  "Lucknow Super Giants": "lsg",
  "Mumbai Indians": "mi",
  "Punjab Kings": "pbks",
  "Rajasthan Royals": "rr",
  "Royal Challengers Bengaluru": "rcb",
  "Royal Challengers Bangalore": "rcb",
  "Sunrisers Hyderabad": "srh",
};

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const matches = await loadMatchesFromCricsheetArchive(
    options.seasonFrom,
    options.seasonTo,
  );
  const rows: ExportRow[] = [];
  const skipped: Array<Record<string, string>> = [];
  let exportedMatches = 0;

  for (const match of matches) {
    const eventSlug = await resolvePolymarketEventSlug(match);
    if (eventSlug === null) {
      skipped.push({
        sourceMatchId: match.sourceMatchId,
        matchSlug: match.matchSlug,
        reason: "no_polymarket_event",
      });
      continue;
    }

    try {
      const commentaryUrl = await resolveCommentaryUrl(match.sourceMatchId);
      await runPolymarketHistoricalBackfill({
        eventSlug,
        marketTypes: ["moneyline", "cricket_toss_winner"],
      });
      const result = await generateBallOddsTimeline({
        eventSlug,
        commentaryUrl,
        allowPartial: false,
      });

      if (result.deliverySourceMode !== "full_cricsheet") {
        skipped.push({
          sourceMatchId: match.sourceMatchId,
          matchSlug: match.matchSlug,
          reason: `unsupported_delivery_source:${result.deliverySourceMode}`,
        });
        continue;
      }

      for (const row of result.rows) {
        rows.push({
          season: match.season,
          matchDate: match.scheduledStart.toISOString().slice(0, 10),
          sourceMatchId: match.sourceMatchId,
          matchSlug: match.matchSlug,
          eventSlug,
          commentaryUrl,
          deliverySourceMode: result.deliverySourceMode,
          inning: row.inning,
          battingTeam: row.battingTeam,
          bowlingTeam: row.bowlingTeam,
          ball: row.ball,
          event: row.event,
          commentary: row.commentary,
          timestamp: row.timestamp,
          timestampSource: row.timestampSource,
          primaryTeam: row.primaryTeam,
          primaryBeforePct: row.primaryBefore,
          primaryAfterPct: row.primaryAfter,
          primaryDeltaPct: row.primaryDelta,
          secondaryTeam: row.secondaryTeam,
          secondaryBeforePct: row.secondaryBefore,
          secondaryAfterPct: row.secondaryAfter,
          secondaryDeltaPct: row.secondaryDelta,
          pricingSourceBefore: row.pricingSourceBefore,
          pricingSourceAfter: row.pricingSourceAfter,
        });
      }

      exportedMatches += 1;
    } catch (error: unknown) {
      skipped.push({
        sourceMatchId: match.sourceMatchId,
        matchSlug: match.matchSlug,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  await mkdir(dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, toCsv(rows), "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        outputPath: options.outputPath,
        seasonFrom: options.seasonFrom,
        seasonTo: options.seasonTo,
        scannedMatches: matches.length,
        exportedMatches,
        exportedRows: rows.length,
        skippedCount: skipped.length,
        skipped: skipped.slice(0, 20),
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  const currentYear = new Date().getUTCFullYear();
  let seasonFrom = 2024;
  let seasonTo = currentYear;
  let outputPath = `data/polymarket-ball-odds-ipl-${seasonFrom}-${seasonTo}.csv`;

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
      outputPath = argv[index + 1] ?? outputPath;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season-from, --season-to, optional --output.`,
    );
  }

  if (seasonFrom > seasonTo) {
    throw new Error("--season-from must be less than or equal to --season-to.");
  }

  if (outputPath === `data/polymarket-ball-odds-ipl-2024-${currentYear}.csv`) {
    outputPath = `data/polymarket-ball-odds-ipl-${seasonFrom}-${seasonTo}.csv`;
  }

  return { seasonFrom, seasonTo, outputPath };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }
  return parsed;
}

async function loadMatchesFromCricsheetArchive(
  seasonFrom: number,
  seasonTo: number,
): Promise<MatchRow[]> {
  const tempDir = await mkdtemp(join(tmpdir(), "ipl-cricsheet-export-"));
  const zipPath = join(tempDir, "ipl_json.zip");
  const extractDir = join(tempDir, "extract");

  try {
    const response = await fetch(
      "https://cricsheet.org/downloads/ipl_json.zip",
      {
        headers: { "user-agent": "Mozilla/5.0" },
      },
    );
    if (!response.ok) {
      throw new Error(
        `Failed to download Cricsheet archive: ${response.status}`,
      );
    }

    const arrayBuffer = await response.arrayBuffer();
    await writeFile(zipPath, Buffer.from(arrayBuffer));
    await unzipArchive(zipPath, extractDir);

    const files = await collectJsonFiles(extractDir);
    const matches: MatchRow[] = [];

    for (const file of files) {
      const raw = await readFile(file.fullPath, "utf8");
      const parsed = JSON.parse(raw) as CricsheetArchiveMatch;
      const info = parsed.info;
      const season = Number.parseInt(String(info?.season ?? ""), 10);
      const teams = info?.teams ?? [];
      const scheduledDate = info?.dates?.[0];
      const winner = info?.outcome?.winner;

      if (
        !Number.isInteger(season) ||
        season < seasonFrom ||
        season > seasonTo ||
        info?.gender !== "male" ||
        info?.match_type !== "T20" ||
        teams.length !== 2 ||
        scheduledDate === undefined ||
        winner === undefined
      ) {
        continue;
      }

      matches.push({
        sourceMatchId: file.sourceMatchId,
        matchSlug: `${slugify(teams[0] ?? "team-a")}-vs-${slugify(teams[1] ?? "team-b")}-${file.sourceMatchId}`,
        season,
        scheduledStart: new Date(`${scheduledDate}T00:00:00.000Z`),
        teamAName: teams[0] ?? "",
        teamBName: teams[1] ?? "",
      });
    }

    matches.sort(
      (left, right) =>
        left.scheduledStart.getTime() - right.scheduledStart.getTime(),
    );
    return matches;
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
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

async function collectJsonFiles(
  rootDir: string,
): Promise<Array<{ fullPath: string; sourceMatchId: string }>> {
  const entries = await readdir(rootDir, {
    recursive: true,
    withFileTypes: true,
  });
  const files: Array<{ fullPath: string; sourceMatchId: string }> = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".json")) {
      continue;
    }
    files.push({
      fullPath: join(entry.parentPath, entry.name),
      sourceMatchId: entry.name.replace(/\.json$/i, ""),
    });
  }
  return files;
}

async function resolvePolymarketEventSlug(
  match: MatchRow,
): Promise<string | null> {
  const teamASlug = TEAM_SLUGS[match.teamAName];
  const teamBSlug = TEAM_SLUGS[match.teamBName];
  const teamALegacyCode = LEGACY_TEAM_CODES[match.teamAName];
  const teamBLegacyCode = LEGACY_TEAM_CODES[match.teamBName];
  if (
    teamASlug === undefined ||
    teamBSlug === undefined ||
    teamALegacyCode === undefined ||
    teamBLegacyCode === undefined
  ) {
    return null;
  }

  const date = match.scheduledStart.toISOString().slice(0, 10);
  const candidates = new Set<string>([
    `cricipl-${teamASlug}-${teamBSlug}-${date}`,
    `cricipl-${teamBSlug}-${teamASlug}-${date}`,
    `ipl-${teamALegacyCode}-${teamBLegacyCode}-${date}`,
    `ipl-${teamBLegacyCode}-${teamALegacyCode}-${date}`,
  ]);

  for (const teamAVariant of expandTeamNameVariants(match.teamAName)) {
    for (const teamBVariant of expandTeamNameVariants(match.teamBName)) {
      candidates.add(`${slugify(teamAVariant)}-vs-${slugify(teamBVariant)}`);
      candidates.add(`${slugify(teamBVariant)}-vs-${slugify(teamAVariant)}`);
    }
  }

  for (const slug of candidates) {
    const response = await fetch(
      `https://gamma-api.polymarket.com/events?slug=${slug}`,
      {
        headers: { "user-agent": "Mozilla/5.0", accept: "application/json" },
      },
    );
    if (!response.ok) {
      continue;
    }
    const payload = (await response.json()) as Array<Record<string, unknown>>;
    if (Array.isArray(payload) && payload.length > 0) {
      return slug;
    }
  }

  return null;
}

function expandTeamNameVariants(teamName: string): string[] {
  const variants = new Set<string>([teamName]);
  if (teamName.includes("Bengaluru")) {
    variants.add(teamName.replace("Bengaluru", "Bangalore"));
  }
  if (teamName.includes("Bangalore")) {
    variants.add(teamName.replace("Bangalore", "Bengaluru"));
  }
  return [...variants];
}

async function resolveCommentaryUrl(sourceMatchId: string): Promise<string> {
  const engineUrl = `https://www.espncricinfo.com/ci/engine/match/${sourceMatchId}.html?view=commentary`;
  const response = await fetch(engineUrl, {
    headers: {
      "user-agent": "Mozilla/5.0",
      "accept-language": "en-US,en;q=0.9",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to resolve ESPN commentary URL for match ${sourceMatchId}: ${response.status}`,
    );
  }

  if (response.url.includes("/full-scorecard")) {
    return response.url.replace("/full-scorecard", "/ball-by-ball-commentary");
  }
  if (response.url.includes("/live-cricket-score")) {
    return response.url.replace(
      "/live-cricket-score",
      "/ball-by-ball-commentary",
    );
  }
  return response.url;
}

function toCsv(rows: readonly ExportRow[]): string {
  const header = [
    "season",
    "match_date",
    "source_match_id",
    "match_slug",
    "event_slug",
    "commentary_url",
    "delivery_source_mode",
    "inning",
    "batting_team",
    "bowling_team",
    "ball",
    "event",
    "commentary",
    "timestamp",
    "timestamp_source",
    "primary_team",
    "primary_before_pct",
    "primary_after_pct",
    "primary_delta_pct",
    "secondary_team",
    "secondary_before_pct",
    "secondary_after_pct",
    "secondary_delta_pct",
    "pricing_source_before",
    "pricing_source_after",
  ];

  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.season,
        row.matchDate,
        row.sourceMatchId,
        row.matchSlug,
        row.eventSlug,
        row.commentaryUrl,
        row.deliverySourceMode,
        row.inning,
        row.battingTeam,
        row.bowlingTeam,
        row.ball,
        row.event,
        row.commentary ?? "",
        row.timestamp,
        row.timestampSource,
        row.primaryTeam,
        row.primaryBeforePct ?? "",
        row.primaryAfterPct ?? "",
        row.primaryDeltaPct ?? "",
        row.secondaryTeam,
        row.secondaryBeforePct ?? "",
        row.secondaryAfterPct ?? "",
        row.secondaryDeltaPct ?? "",
        row.pricingSourceBefore ?? "",
        row.pricingSourceAfter ?? "",
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  if (!/[",\n]/u.test(text)) {
    return text;
  }
  return `"${text.replace(/"/gu, '""')}"`;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  console.error(`Export Polymarket ball odds CSV failed: ${message}`);
  process.exitCode = 1;
});
