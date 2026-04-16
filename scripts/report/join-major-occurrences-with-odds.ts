import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  majorPath: string;
  oddsPath: string;
  outputPath: string;
}

interface OddsRow {
  inning: string;
  ball: string;
  batting_team: string;
  bowling_team: string;
  event: string;
  timestamp: string;
  consensus_before_pct: string;
  consensus_after_pct: string;
  consensus_delta_pct: string;
  bookmaker_count_before: string;
  bookmaker_count_after: string;
  betfair_back_before_pct: string;
  betfair_back_after_pct: string;
  betfair_back_delta_pct: string;
  snapshot_time_before: string;
  snapshot_time_after: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const major = await loadCsv(options.majorPath);
  const odds = await loadCsv(options.oddsPath);

  const oddsHeaderIndex = new Map<string, number>();
  for (const [index, column] of odds.header.entries()) {
    oddsHeaderIndex.set(column, index);
  }
  const oddsRows = odds.rows.map((row) => parseOddsRow(row, oddsHeaderIndex));
  const oddsByKey = new Map<string, OddsRow>();
  for (const row of oddsRows) {
    oddsByKey.set(
      buildKey(row.inning, row.ball, row.event, row.timestamp),
      row,
    );
  }

  const extraColumns = [
    "odds_consensus_before_pct",
    "odds_consensus_after_pct",
    "odds_consensus_delta_pct",
    "odds_bookmaker_count_before",
    "odds_bookmaker_count_after",
    "odds_betfair_back_before_pct",
    "odds_betfair_back_after_pct",
    "odds_betfair_back_delta_pct",
    "odds_snapshot_time_before",
    "odds_snapshot_time_after",
  ];

  const majorHeaderIndex = new Map<string, number>();
  for (const [index, column] of major.header.entries()) {
    majorHeaderIndex.set(column, index);
  }
  const inningIndex = requireColumn(majorHeaderIndex, "inning");
  const ballIndex = requireColumn(majorHeaderIndex, "ball");
  const eventIndex = requireColumn(majorHeaderIndex, "event");
  const timestampIndex = requireColumn(majorHeaderIndex, "timestamp");

  const mergedRows = major.rows.map((row) => {
    const oddsRow = oddsByKey.get(
      buildKey(
        row[inningIndex] ?? "",
        row[ballIndex] ?? "",
        row[eventIndex] ?? "",
        row[timestampIndex] ?? "",
      ),
    );

    return [
      ...row,
      oddsRow?.consensus_before_pct ?? "",
      oddsRow?.consensus_after_pct ?? "",
      oddsRow?.consensus_delta_pct ?? "",
      oddsRow?.bookmaker_count_before ?? "",
      oddsRow?.bookmaker_count_after ?? "",
      oddsRow?.betfair_back_before_pct ?? "",
      oddsRow?.betfair_back_after_pct ?? "",
      oddsRow?.betfair_back_delta_pct ?? "",
      oddsRow?.snapshot_time_before ?? "",
      oddsRow?.snapshot_time_after ?? "",
    ];
  });

  const outputHeader = [...major.header, ...extraColumns];
  await mkdir(dirname(resolve(process.cwd(), options.outputPath)), {
    recursive: true,
  });
  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv(outputHeader, mergedRows),
    "utf8",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        majorPath: options.majorPath,
        oddsPath: options.oddsPath,
        outputPath: options.outputPath,
        majorRows: major.rows.length,
        oddsRows: odds.rows.length,
        matchedRows: mergedRows.filter((row) => row[major.header.length] !== "")
          .length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let majorPath = "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688.csv";
  let oddsPath = "data/kkr-vs-lsg-1527688-odds.csv";
  let outputPath =
    "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688-with-odds.csv";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--major") {
      majorPath = argv[index + 1] ?? majorPath;
      index += 1;
      continue;
    }
    if (argument === "--odds") {
      oddsPath = argv[index + 1] ?? oddsPath;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? outputPath;
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --major, --odds, --output.`,
    );
  }

  return { majorPath, oddsPath, outputPath };
}

async function loadCsv(
  filePath: string,
): Promise<{ header: string[]; rows: string[][] }> {
  const content = await readFile(resolve(process.cwd(), filePath), "utf8");
  const records = parseCsv(content);
  const [header, ...rows] = records;
  if (header === undefined) {
    throw new Error(`CSV is empty: ${filePath}`);
  }
  return { header, rows };
}

function parseOddsRow(
  record: readonly string[],
  index: ReadonlyMap<string, number>,
): OddsRow {
  const get = (name: keyof OddsRow): string => {
    const position = index.get(name);
    if (position === undefined) {
      throw new Error(`Missing odds column: ${name}`);
    }
    return record[position] ?? "";
  };

  return {
    inning: get("inning"),
    ball: get("ball"),
    batting_team: get("batting_team"),
    bowling_team: get("bowling_team"),
    event: get("event"),
    timestamp: get("timestamp"),
    consensus_before_pct: get("consensus_before_pct"),
    consensus_after_pct: get("consensus_after_pct"),
    consensus_delta_pct: get("consensus_delta_pct"),
    bookmaker_count_before: get("bookmaker_count_before"),
    bookmaker_count_after: get("bookmaker_count_after"),
    betfair_back_before_pct: get("betfair_back_before_pct"),
    betfair_back_after_pct: get("betfair_back_after_pct"),
    betfair_back_delta_pct: get("betfair_back_delta_pct"),
    snapshot_time_before: get("snapshot_time_before"),
    snapshot_time_after: get("snapshot_time_after"),
  };
}

function requireColumn(
  index: ReadonlyMap<string, number>,
  column: string,
): number {
  const position = index.get(column);
  if (position === undefined) {
    throw new Error(`Missing required column: ${column}`);
  }
  return position;
}

function buildKey(
  inning: string,
  ball: string,
  event: string,
  timestamp: string,
): string {
  return [inning, ball, event, timestamp].join("|");
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
