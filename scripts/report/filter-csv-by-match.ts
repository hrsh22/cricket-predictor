import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  inputPath: string;
  outputPath: string;
  sourceMatchId: string;
}

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

  const sourceMatchIdIndex = header.indexOf("source_match_id");
  if (sourceMatchIdIndex === -1) {
    throw new Error(`CSV missing source_match_id column: ${options.inputPath}`);
  }

  const filteredRows = rows.filter(
    (row) => (row[sourceMatchIdIndex] ?? "") === options.sourceMatchId,
  );

  if (filteredRows.length === 0) {
    throw new Error(
      `No rows found for source_match_id=${options.sourceMatchId} in ${options.inputPath}`,
    );
  }

  const csv = toCsv(header, filteredRows);
  await mkdir(dirname(resolve(process.cwd(), options.outputPath)), {
    recursive: true,
  });
  await writeFile(resolve(process.cwd(), options.outputPath), csv, "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        sourceMatchId: options.sourceMatchId,
        rowCount: filteredRows.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath = "data/ipl-2026-major-occurrences.csv";
  let outputPath = "data/ipl-2026-major-occurrences-kkr-vs-lsg-1527688.csv";
  let sourceMatchId = "1527688";

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
    if (argument === "--source-match-id") {
      sourceMatchId = argv[index + 1] ?? sourceMatchId;
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --input, --output, --source-match-id.`,
    );
  }

  return { inputPath, outputPath, sourceMatchId };
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
