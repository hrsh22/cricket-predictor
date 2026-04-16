import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { generateMatchBallTimeline } from "./ball-odds-timeline.js";

interface CliOptions {
  inputPath: string;
  outputPath: string;
}

interface CsvRow {
  season: string;
  match_date: string;
  source_match_id: string;
  match_slug: string;
  event_slug: string;
  commentary_url: string;
  delivery_source_mode: string;
  inning: string;
  batting_team: string;
  bowling_team: string;
  ball: string;
  event: string;
  commentary: string;
  timestamp: string;
  timestamp_source: string;
  primary_team: string;
  primary_before_pct: string;
  primary_after_pct: string;
  primary_delta_pct: string;
  secondary_team: string;
  secondary_before_pct: string;
  secondary_after_pct: string;
  secondary_delta_pct: string;
  pricing_source_before: string;
  pricing_source_after: string;
}

interface MatchBallRow {
  inning: 1 | 2;
  ball: string;
  event: string;
  commentary: string | null;
  timestamp: string;
  timestampSource: "exact" | "estimated";
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const { header, rows } = await loadCsv(options.inputPath);
  const rowsByMatch = groupByMatch(rows);

  let repairedMatches = 0;
  let repairedRows = 0;

  for (const [sourceMatchId, matchRows] of rowsByMatch.entries()) {
    if (!needsRepair(matchRows)) {
      continue;
    }

    const commentaryUrl = await resolveCommentaryUrl(sourceMatchId);
    const timeline = await generateMatchBallTimeline({
      commentaryUrl,
      allowPartial: true,
    });
    const timelineMap = new Map<string, MatchBallRow>();
    for (const row of timeline.rows) {
      timelineMap.set(
        buildBallKey(String(row.inning), row.ball, row.event),
        row,
      );
    }

    for (const row of matchRows) {
      row.commentary_url = commentaryUrl;
      row.delivery_source_mode = timeline.deliverySourceMode;
      const timelineRow = timelineMap.get(
        buildBallKey(row.inning, row.ball, row.event),
      );
      if (timelineRow === undefined) {
        continue;
      }

      row.timestamp = timelineRow.timestamp;
      row.timestamp_source = timelineRow.timestampSource;
      if (
        (row.commentary ?? "").trim().length === 0 &&
        timelineRow.commentary !== null
      ) {
        row.commentary = timelineRow.commentary;
      }
      repairedRows += 1;
    }

    normalizeMonotonicTimestamps(matchRows);
    repairedMatches += 1;
  }

  const flattened = [...rowsByMatch.values()].flat();
  flattened.sort(compareRows);
  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv(header, flattened),
    "utf8",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        scannedMatches: rowsByMatch.size,
        repairedMatches,
        repairedRows,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath = "data/polymarket-ball-odds-ipl-2026.csv";
  let outputPath = inputPath;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--input") {
      inputPath = argv[index + 1] ?? inputPath;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? outputPath;
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --input and optional --output.`,
    );
  }

  return { inputPath, outputPath };
}

async function loadCsv(
  inputPath: string,
): Promise<{ header: string[]; rows: CsvRow[] }> {
  const content = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const records = parseCsv(content);
  const [header, ...body] = records;
  if (header === undefined) {
    throw new Error(`CSV is empty: ${inputPath}`);
  }
  const index = new Map<string, number>();
  for (const [position, column] of header.entries()) {
    index.set(column, position);
  }
  return {
    header,
    rows: body.map((record) => parseRow(record, index)),
  };
}

function parseRow(
  record: readonly string[],
  index: ReadonlyMap<string, number>,
): CsvRow {
  const get = (name: keyof CsvRow): string => {
    const position = index.get(name);
    if (position === undefined) {
      throw new Error(`Missing CSV column: ${name}`);
    }
    return record[position] ?? "";
  };

  return {
    season: get("season"),
    match_date: get("match_date"),
    source_match_id: get("source_match_id"),
    match_slug: get("match_slug"),
    event_slug: get("event_slug"),
    commentary_url: get("commentary_url"),
    delivery_source_mode: get("delivery_source_mode"),
    inning: get("inning"),
    batting_team: get("batting_team"),
    bowling_team: get("bowling_team"),
    ball: get("ball"),
    event: get("event"),
    commentary: get("commentary"),
    timestamp: get("timestamp"),
    timestamp_source: get("timestamp_source"),
    primary_team: get("primary_team"),
    primary_before_pct: get("primary_before_pct"),
    primary_after_pct: get("primary_after_pct"),
    primary_delta_pct: get("primary_delta_pct"),
    secondary_team: get("secondary_team"),
    secondary_before_pct: get("secondary_before_pct"),
    secondary_after_pct: get("secondary_after_pct"),
    secondary_delta_pct: get("secondary_delta_pct"),
    pricing_source_before: get("pricing_source_before"),
    pricing_source_after: get("pricing_source_after"),
  };
}

function groupByMatch(rows: readonly CsvRow[]): Map<string, CsvRow[]> {
  const map = new Map<string, CsvRow[]>();
  for (const row of rows) {
    const existing = map.get(row.source_match_id) ?? [];
    existing.push(row);
    map.set(row.source_match_id, existing);
  }
  for (const matchRows of map.values()) {
    matchRows.sort(compareRows);
  }
  return map;
}

function needsRepair(rows: readonly CsvRow[]): boolean {
  let previousTime: number | null = null;
  for (const row of rows) {
    if (row.timestamp.trim().length === 0) {
      return true;
    }
    const currentTime = Date.parse(row.timestamp);
    if (!Number.isFinite(currentTime)) {
      return true;
    }
    if (previousTime !== null && currentTime < previousTime) {
      return true;
    }
    previousTime = currentTime;
  }
  return false;
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

function buildBallKey(inning: string, ball: string, event: string): string {
  return `${inning}:${ball}:${event}`;
}

function normalizeMonotonicTimestamps(rows: readonly CsvRow[]): void {
  let previousMs: number | null = null;
  for (const row of rows) {
    if (row.timestamp.trim().length === 0) {
      continue;
    }
    const currentMs = Date.parse(row.timestamp);
    if (!Number.isFinite(currentMs)) {
      continue;
    }
    if (previousMs !== null && currentMs < previousMs) {
      const normalizedMs: number = previousMs + 1_000;
      row.timestamp = new Date(normalizedMs).toISOString();
      if (row.timestamp_source === "exact") {
        row.timestamp_source = "estimated";
      }
      previousMs = normalizedMs;
      continue;
    }
    previousMs = currentMs;
  }
}

function compareRows(left: CsvRow, right: CsvRow): number {
  if (left.match_date !== right.match_date) {
    return left.match_date.localeCompare(right.match_date);
  }
  if (left.source_match_id !== right.source_match_id) {
    return left.source_match_id.localeCompare(right.source_match_id);
  }
  const inningCompare =
    Number.parseInt(left.inning, 10) - Number.parseInt(right.inning, 10);
  if (inningCompare !== 0) {
    return inningCompare;
  }
  const leftBall = parseBall(left.ball);
  const rightBall = parseBall(right.ball);
  if (leftBall.over !== rightBall.over) {
    return leftBall.over - rightBall.over;
  }
  return leftBall.ballInOver - rightBall.ballInOver;
}

function parseBall(token: string): { over: number; ballInOver: number } {
  const [overText, ballText] = token.split(".");
  return {
    over: Number.parseInt(overText ?? "0", 10),
    ballInOver: Number.parseInt(ballText ?? "0", 10),
  };
}

function toCsv(header: readonly string[], rows: readonly CsvRow[]): string {
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      header
        .map((column) => csvEscape(row[column as keyof CsvRow] ?? ""))
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
}

function csvEscape(value: string): string {
  if (!/[",\n]/u.test(value)) {
    return value;
  }
  return `"${value.replace(/"/gu, '""')}"`;
}

function parseCsv(text: string): string[][] {
  const records: string[][] = [];
  let record: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index] ?? "";
    const next = text[index + 1] ?? "";
    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
        continue;
      }
      if (char === '"') {
        inQuotes = false;
        continue;
      }
      field += char;
      continue;
    }
    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      record.push(field);
      field = "";
      continue;
    }
    if (char === "\n") {
      record.push(field);
      field = "";
      if (record.some((value) => value.length > 0)) {
        records.push(record);
      }
      record = [];
      continue;
    }
    if (char === "\r") {
      continue;
    }
    field += char;
  }

  if (field.length > 0 || record.length > 0) {
    record.push(field);
    if (record.some((value) => value.length > 0)) {
      records.push(record);
    }
  }

  return records;
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
