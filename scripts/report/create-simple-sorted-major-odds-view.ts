import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  inputPath: string;
  outputPath: string;
}

interface SimpleRow {
  match_date: string;
  source_match_id: string;
  inning: string;
  phase: string;
  ball: string;
  batting_team: string;
  bowling_team: string;
  occurrence_types: string;
  event: string;
  signal: string;
  event_window: string;
  timestamp: string;
  timestamp_source: string;
  score_before: string;
  score_after: string;
  balls_remaining_before: string;
  balls_remaining_after: string;
  target: string;
  chase_before: string;
  chase_after: string;
  batting_before_pct: string;
  batting_after_pct: string;
  batting_delta_pct: string;
  bowling_before_pct: string;
  bowling_after_pct: string;
  bowling_delta_pct: string;
  abs_market_delta_pct: string;
  bookmaker_count_before: string;
  bookmaker_count_after: string;
  snapshot_time_before: string;
  snapshot_time_after: string;
}

const OUTPUT_COLUMNS: Array<keyof SimpleRow> = [
  "match_date",
  "source_match_id",
  "inning",
  "phase",
  "ball",
  "batting_team",
  "bowling_team",
  "occurrence_types",
  "event",
  "signal",
  "event_window",
  "timestamp",
  "timestamp_source",
  "score_before",
  "score_after",
  "balls_remaining_before",
  "balls_remaining_after",
  "target",
  "chase_before",
  "chase_after",
  "batting_before_pct",
  "batting_after_pct",
  "batting_delta_pct",
  "bowling_before_pct",
  "bowling_after_pct",
  "bowling_delta_pct",
  "abs_market_delta_pct",
  "bookmaker_count_before",
  "bookmaker_count_after",
  "snapshot_time_before",
  "snapshot_time_after",
];

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const content = await readFile(
    resolve(process.cwd(), options.inputPath),
    "utf8",
  );
  const records = parseCsv(content);
  const [header, ...rows] = records;
  if (header === undefined) {
    throw new Error(`CSV is empty: ${options.inputPath}`);
  }

  const headerIndex = new Map<string, number>();
  for (const [index, column] of header.entries()) {
    headerIndex.set(column, index);
  }

  const outputRows = rows.map((row) => buildSimpleRow(row, headerIndex));
  outputRows.sort(compareSimpleRows);

  await mkdir(dirname(resolve(process.cwd(), options.outputPath)), {
    recursive: true,
  });
  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv(outputRows),
    "utf8",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        rowCount: outputRows.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath =
    "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688-clean.csv";
  let outputPath =
    "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688-simple-sorted.csv";

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
      `Unknown argument "${argument}". Expected --input and --output.`,
    );
  }

  return { inputPath, outputPath };
}

function buildSimpleRow(
  row: readonly string[],
  headerIndex: ReadonlyMap<string, number>,
): SimpleRow {
  const get = (name: string): string => {
    const index = headerIndex.get(name);
    if (index === undefined) {
      return "";
    }
    return row[index] ?? "";
  };

  const battingDelta = get("odds_batting_consensus_delta_pct");

  return {
    match_date: get("match_date"),
    source_match_id: get("source_match_id"),
    inning: get("inning"),
    phase: get("phase"),
    ball: get("ball"),
    batting_team: get("batting_team"),
    bowling_team: get("bowling_team"),
    occurrence_types: get("occurrence_types"),
    event: get("event"),
    signal: get("signal"),
    event_window: get("event_window"),
    timestamp: get("timestamp"),
    timestamp_source: get("timestamp_source"),
    score_before: formatScore(
      get("cumulative_runs_before"),
      get("wickets_before"),
    ),
    score_after: formatScore(
      get("cumulative_runs_after"),
      get("wickets_after"),
    ),
    balls_remaining_before: get("balls_remaining_before"),
    balls_remaining_after: get("balls_remaining_after"),
    target: get("target"),
    chase_before: formatChase(
      get("runs_required_before"),
      get("balls_remaining_before"),
      get("required_run_rate_before"),
    ),
    chase_after: formatChase(
      get("runs_required_after"),
      get("balls_remaining_after"),
      get("required_run_rate_after"),
    ),
    batting_before_pct: get("odds_batting_consensus_before_pct"),
    batting_after_pct: get("odds_batting_consensus_after_pct"),
    batting_delta_pct: battingDelta,
    bowling_before_pct: get("odds_bowling_consensus_before_pct"),
    bowling_after_pct: get("odds_bowling_consensus_after_pct"),
    bowling_delta_pct: get("odds_bowling_consensus_delta_pct"),
    abs_market_delta_pct: absPct(battingDelta),
    bookmaker_count_before: get("odds_bookmaker_count_before"),
    bookmaker_count_after: get("odds_bookmaker_count_after"),
    snapshot_time_before: get("odds_snapshot_time_before"),
    snapshot_time_after: get("odds_snapshot_time_after"),
  };
}

function formatScore(runs: string, wickets: string): string {
  if (runs.trim().length === 0 || wickets.trim().length === 0) {
    return "";
  }
  return `${runs}/${wickets}`;
}

function formatChase(
  runsRequired: string,
  ballsRemaining: string,
  requiredRunRate: string,
): string {
  if (runsRequired.trim().length === 0) {
    return "";
  }
  const ballsText = ballsRemaining.trim().length === 0 ? "?" : ballsRemaining;
  const rrrText = requiredRunRate.trim().length === 0 ? "?" : requiredRunRate;
  return `${runsRequired} off ${ballsText} (rrr ${rrrText})`;
}

function absPct(value: string): string {
  if (value.trim().length === 0) {
    return "";
  }
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed)) {
    return "";
  }
  return (Math.round(Math.abs(parsed) * 10) / 10).toFixed(1);
}

function compareSimpleRows(left: SimpleRow, right: SimpleRow): number {
  const deltaDiff =
    parseSortableNumber(right.abs_market_delta_pct) -
    parseSortableNumber(left.abs_market_delta_pct);
  if (deltaDiff !== 0) {
    return deltaDiff;
  }
  return left.timestamp.localeCompare(right.timestamp);
}

function parseSortableNumber(value: string): number {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : -1;
}

function toCsv(rows: readonly SimpleRow[]): string {
  const lines = [OUTPUT_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(
      OUTPUT_COLUMNS.map((column) => csvEscape(row[column] ?? "")).join(","),
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
