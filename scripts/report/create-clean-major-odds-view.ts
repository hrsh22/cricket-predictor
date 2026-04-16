import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  inputPath: string;
  outputPath: string;
}

const OUTPUT_COLUMNS = [
  "season",
  "match_date",
  "source_match_id",
  "match_slug",
  "inning",
  "phase",
  "ball",
  "batting_team",
  "bowling_team",
  "occurrence_types",
  "signal",
  "event",
  "previous_event",
  "event_pair",
  "event_window",
  "timestamp",
  "timestamp_source",
  "cumulative_runs_before",
  "cumulative_runs_after",
  "wickets_before",
  "wickets_after",
  "balls_remaining_before",
  "balls_remaining_after",
  "target",
  "runs_required_before",
  "runs_required_after",
  "required_run_rate_before",
  "required_run_rate_after",
  "current_run_rate_before",
  "current_run_rate_after",
  "odds_batting_consensus_before_pct",
  "odds_batting_consensus_after_pct",
  "odds_batting_consensus_delta_pct",
  "odds_bowling_consensus_before_pct",
  "odds_bowling_consensus_after_pct",
  "odds_bowling_consensus_delta_pct",
  "odds_batting_betfair_before_pct",
  "odds_batting_betfair_after_pct",
  "odds_batting_betfair_delta_pct",
  "odds_bowling_betfair_before_pct",
  "odds_bowling_betfair_after_pct",
  "odds_bowling_betfair_delta_pct",
  "odds_bookmaker_count_before",
  "odds_bookmaker_count_after",
  "odds_snapshot_time_before",
  "odds_snapshot_time_after",
] as const;

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

  const outputRows = rows.map((row) => buildOutputRow(row, headerIndex));

  await mkdir(dirname(resolve(process.cwd(), options.outputPath)), {
    recursive: true,
  });
  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv([...OUTPUT_COLUMNS], outputRows),
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
    "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688-with-odds.csv";
  let outputPath =
    "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688-clean.csv";

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

function buildOutputRow(
  row: readonly string[],
  headerIndex: ReadonlyMap<string, number>,
): string[] {
  const get = (name: string): string => {
    const index = headerIndex.get(name);
    if (index === undefined) {
      return "";
    }
    return row[index] ?? "";
  };

  const oddsConsensusBefore = get("odds_consensus_before_pct");
  const oddsConsensusAfter = get("odds_consensus_after_pct");
  const oddsConsensusDelta = get("odds_consensus_delta_pct");
  const oddsBetfairBefore = get("odds_betfair_back_before_pct");
  const oddsBetfairAfter = get("odds_betfair_back_after_pct");
  const oddsBetfairDelta = get("odds_betfair_back_delta_pct");

  return [
    get("season"),
    get("match_date"),
    get("source_match_id"),
    get("match_slug"),
    get("inning"),
    get("phase"),
    get("ball"),
    get("batting_team"),
    get("bowling_team"),
    get("occurrence_types"),
    get("signal"),
    get("event"),
    get("previous_event"),
    get("event_pair"),
    get("event_window"),
    get("timestamp"),
    get("timestamp_source"),
    get("cumulative_runs_before"),
    get("cumulative_runs_after"),
    get("wickets_before"),
    get("wickets_after"),
    get("balls_remaining_before"),
    get("balls_remaining_after"),
    get("target"),
    get("runs_required_before"),
    get("runs_required_after"),
    get("required_run_rate_before"),
    get("required_run_rate_after"),
    get("current_run_rate_before"),
    get("current_run_rate_after"),
    oddsConsensusBefore,
    oddsConsensusAfter,
    oddsConsensusDelta,
    complementPct(oddsConsensusBefore),
    complementPct(oddsConsensusAfter),
    negatePct(oddsConsensusDelta),
    oddsBetfairBefore,
    oddsBetfairAfter,
    oddsBetfairDelta,
    complementPct(oddsBetfairBefore),
    complementPct(oddsBetfairAfter),
    negatePct(oddsBetfairDelta),
    get("odds_bookmaker_count_before"),
    get("odds_bookmaker_count_after"),
    get("odds_snapshot_time_before"),
    get("odds_snapshot_time_after"),
  ];
}

function complementPct(value: string): string {
  if (value.trim().length === 0) {
    return "";
  }
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed)) {
    return "";
  }
  return roundOneDecimal(100 - parsed);
}

function negatePct(value: string): string {
  if (value.trim().length === 0) {
    return "";
  }
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed)) {
    return "";
  }
  return roundOneDecimal(-parsed);
}

function roundOneDecimal(value: number): string {
  return (Math.round(value * 10) / 10).toFixed(1).replace(/\.0$/u, ".0");
}

function toCsv(header: readonly string[], rows: readonly string[][]): string {
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(row.map(csvEscape).join(","));
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
