import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

interface CliOptions {
  season: number;
  inputPath: string;
  outputPath: string;
}

interface ExistingRow {
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

interface CricsheetMatch {
  info?: {
    season?: string | number;
    dates?: string[];
    teams?: string[];
    gender?: string;
    match_type?: string;
  };
  innings?: Array<{
    team?: string;
    overs?: Array<{
      over?: number;
      deliveries?: Array<Record<string, unknown>>;
      balls?: Array<Record<string, unknown>>;
    }>;
  }>;
}

interface DeliveryRow extends ExistingRow {
  sortKey: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const existing = await loadExistingRows(options.inputPath);
  const existingMatchIds = new Set(
    existing.rows.map((row) => row.source_match_id),
  );
  const missing = await loadMissingCricsheetRows(
    options.season,
    existingMatchIds,
  );
  const merged = [...existing.rows, ...missing].sort(compareDeliveryRows);

  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv(existing.header, merged),
    "utf8",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        season: options.season,
        existingMatches: existingMatchIds.size,
        appendedMatches: new Set(missing.map((row) => row.source_match_id))
          .size,
        appendedRows: missing.length,
        mergedMatches: new Set(merged.map((row) => row.source_match_id)).size,
        mergedRows: merged.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season = 2026;
  let inputPath = `data/polymarket-ball-odds-ipl-${season}.csv`;
  let outputPath = inputPath;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--season") {
      season = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }
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
      `Unknown argument "${argument}". Expected --season, --input, --output.`,
    );
  }

  if (
    inputPath === `data/polymarket-ball-odds-ipl-2026.csv` &&
    season !== 2026
  ) {
    inputPath = `data/polymarket-ball-odds-ipl-${season}.csv`;
  }
  if (
    outputPath === `data/polymarket-ball-odds-ipl-2026.csv` &&
    season !== 2026
  ) {
    outputPath = `data/polymarket-ball-odds-ipl-${season}.csv`;
  }

  return { season, inputPath, outputPath };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }
  return parsed;
}

async function loadExistingRows(
  inputPath: string,
): Promise<{ header: string[]; rows: DeliveryRow[] }> {
  const content = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const records = parseCsv(content);
  const [header, ...body] = records;
  if (header === undefined) {
    throw new Error(`Input CSV is empty: ${inputPath}`);
  }
  const index = new Map<string, number>();
  for (const [position, column] of header.entries()) {
    index.set(column, position);
  }

  const rows = body.map((record) => {
    const row = parseExistingRow(record, index);
    return {
      ...row,
      sortKey: buildSortKey(
        row.match_date,
        row.source_match_id,
        row.inning,
        row.ball,
      ),
    };
  });

  return { header, rows };
}

async function loadMissingCricsheetRows(
  season: number,
  existingMatchIds: ReadonlySet<string>,
): Promise<DeliveryRow[]> {
  const response = await fetch("https://cricsheet.org/downloads/ipl_json.zip", {
    headers: { "user-agent": "Mozilla/5.0" },
  });
  if (!response.ok) {
    throw new Error(`Failed to download Cricsheet archive: ${response.status}`);
  }

  const zipBuffer = Buffer.from(await response.arrayBuffer());
  const tempZipPath = resolve(process.cwd(), ".tmp-ipl-json.zip");
  await writeFile(tempZipPath, zipBuffer);

  try {
    const manifest = await listZipEntries(tempZipPath);
    const rows: DeliveryRow[] = [];

    for (const entry of manifest) {
      if (!entry.toLowerCase().endsWith(".json")) {
        continue;
      }

      const sourceMatchId = entry.replace(/\.json$/iu, "");
      if (existingMatchIds.has(sourceMatchId)) {
        continue;
      }

      const raw = await unzipEntry(tempZipPath, entry);
      const parsed = JSON.parse(raw) as CricsheetMatch;
      const info = parsed.info;
      const seasonValue = Number.parseInt(String(info?.season ?? ""), 10);
      const teams = info?.teams ?? [];
      const matchDate = info?.dates?.[0] ?? "";
      if (
        seasonValue !== season ||
        info?.gender !== "male" ||
        info?.match_type !== "T20" ||
        teams.length !== 2 ||
        matchDate.length === 0
      ) {
        continue;
      }

      const matchSlug = `${slugify(teams[0] ?? "team-a")}-vs-${slugify(teams[1] ?? "team-b")}-${sourceMatchId}`;
      rows.push(
        ...buildRowsFromMatch(
          parsed,
          sourceMatchId,
          matchDate,
          matchSlug,
          teams,
        ),
      );
    }

    return rows;
  } finally {
    await writeFile(tempZipPath, "", "utf8").catch(() => undefined);
  }
}

async function listZipEntries(zipPath: string): Promise<string[]> {
  const { spawn } = await import("node:child_process");
  return await new Promise<string[]>((resolvePromise, reject) => {
    const child = spawn("unzip", ["-Z1", zipPath], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if ((code ?? 1) !== 0) {
        reject(
          new Error(
            `Failed to list zip entries: ${Buffer.concat(stderr).toString("utf8")}`,
          ),
        );
        return;
      }
      resolvePromise(
        Buffer.concat(stdout).toString("utf8").split(/\r?\n/u).filter(Boolean),
      );
    });
  });
}

async function unzipEntry(zipPath: string, entryName: string): Promise<string> {
  const { spawn } = await import("node:child_process");
  return await new Promise<string>((resolvePromise, reject) => {
    const child = spawn("unzip", ["-p", zipPath, entryName], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if ((code ?? 1) !== 0) {
        reject(
          new Error(
            `Failed to read ${entryName}: ${Buffer.concat(stderr).toString("utf8")}`,
          ),
        );
        return;
      }
      resolvePromise(Buffer.concat(stdout).toString("utf8"));
    });
  });
}

function buildRowsFromMatch(
  match: CricsheetMatch,
  sourceMatchId: string,
  matchDate: string,
  matchSlug: string,
  teams: readonly string[],
): DeliveryRow[] {
  const rows: DeliveryRow[] = [];
  const innings = match.innings ?? [];

  for (const [inningIndex, inning] of innings.entries()) {
    const inningNumber = inningIndex === 0 ? "1" : "2";
    const battingTeam = inning.team ?? teams[inningIndex] ?? "";
    const bowlingTeam =
      teams.find((team) => team !== battingTeam) ?? teams[0] ?? "";

    for (const over of inning.overs ?? []) {
      const overNumber = over.over ?? 0;
      const deliveries = Array.isArray(over.balls)
        ? over.balls
        : Array.isArray(over.deliveries)
          ? over.deliveries
          : [];

      for (let ballIndex = 0; ballIndex < deliveries.length; ballIndex += 1) {
        const delivery = deliveries[ballIndex] ?? {};
        const runs = asRecord(delivery["runs"]);
        const extras = asRecord(delivery["extras"]);
        const wides = asInt(extras["wides"]);
        const noballs = asInt(extras["noballs"]);
        const byes = asInt(extras["byes"]);
        const legbyes = asInt(extras["legbyes"]);
        const batsmanRuns = asInt(runs["batter"]);
        const totalRuns = asInt(runs["total"]);
        const wicketEntry = Array.isArray(delivery["wickets"])
          ? delivery["wickets"]?.[0]
          : delivery["wicket"];
        const event = buildEventCode({
          batsmanRuns,
          totalRuns,
          wides,
          noballs,
          byes,
          legbyes,
          wicket: wicketEntry !== undefined && wicketEntry !== null,
        });
        const ball = `${overNumber}.${ballIndex + 1}`;

        const row: DeliveryRow = {
          season: String(matchDate.slice(0, 4)),
          match_date: matchDate,
          source_match_id: sourceMatchId,
          match_slug: matchSlug,
          event_slug: "",
          commentary_url: "",
          delivery_source_mode: "full_cricsheet",
          inning: inningNumber,
          batting_team: battingTeam,
          bowling_team: bowlingTeam,
          ball,
          event,
          commentary: "",
          timestamp: "",
          timestamp_source: "",
          primary_team: "",
          primary_before_pct: "",
          primary_after_pct: "",
          primary_delta_pct: "",
          secondary_team: "",
          secondary_before_pct: "",
          secondary_after_pct: "",
          secondary_delta_pct: "",
          pricing_source_before: "",
          pricing_source_after: "",
          sortKey: buildSortKey(matchDate, sourceMatchId, inningNumber, ball),
        };
        rows.push(row);
      }
    }
  }

  return rows;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function asInt(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function buildEventCode(input: {
  batsmanRuns: number;
  totalRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  wicket: boolean;
}): string {
  const parts: string[] = [];
  if (input.wides > 0)
    parts.push(input.wides === 1 ? "wd" : `${input.wides}wd`);
  if (input.noballs > 0)
    parts.push(input.noballs === 1 ? "nb" : `${input.noballs}nb`);
  if (input.byes > 0) parts.push(input.byes === 1 ? "b" : `${input.byes}b`);
  if (input.legbyes > 0)
    parts.push(input.legbyes === 1 ? "lb" : `${input.legbyes}lb`);
  if (input.batsmanRuns > 0) parts.push(String(input.batsmanRuns));
  if (parts.length === 0)
    parts.push(input.totalRuns === 0 ? "0" : String(input.totalRuns));
  if (input.wicket) parts.push("W");
  return parts.join("+");
}

function buildSortKey(
  matchDate: string,
  sourceMatchId: string,
  inning: string,
  ball: string,
): string {
  const [overText, ballText] = ball.split(".");
  const over = Number.parseInt(overText ?? "0", 10);
  const inOver = Number.parseInt(ballText ?? "0", 10);
  return [
    matchDate,
    sourceMatchId,
    inning.padStart(2, "0"),
    String(over).padStart(2, "0"),
    String(inOver).padStart(2, "0"),
  ].join(":");
}

function compareDeliveryRows(left: DeliveryRow, right: DeliveryRow): number {
  return left.sortKey.localeCompare(right.sortKey);
}

function objectFromRecord(
  record: readonly string[],
  index: ReadonlyMap<string, number>,
): Record<string, string> {
  const output: Record<string, string> = {};
  for (const [column, position] of index.entries()) {
    output[column] = record[position] ?? "";
  }
  return output;
}

function parseExistingRow(
  record: readonly string[],
  index: ReadonlyMap<string, number>,
): ExistingRow {
  const raw = objectFromRecord(record, index);

  return {
    season: raw["season"] ?? "",
    match_date: raw["match_date"] ?? "",
    source_match_id: raw["source_match_id"] ?? "",
    match_slug: raw["match_slug"] ?? "",
    event_slug: raw["event_slug"] ?? "",
    commentary_url: raw["commentary_url"] ?? "",
    delivery_source_mode: raw["delivery_source_mode"] ?? "",
    inning: raw["inning"] ?? "",
    batting_team: raw["batting_team"] ?? "",
    bowling_team: raw["bowling_team"] ?? "",
    ball: raw["ball"] ?? "",
    event: raw["event"] ?? "",
    commentary: raw["commentary"] ?? "",
    timestamp: raw["timestamp"] ?? "",
    timestamp_source: raw["timestamp_source"] ?? "",
    primary_team: raw["primary_team"] ?? "",
    primary_before_pct: raw["primary_before_pct"] ?? "",
    primary_after_pct: raw["primary_after_pct"] ?? "",
    primary_delta_pct: raw["primary_delta_pct"] ?? "",
    secondary_team: raw["secondary_team"] ?? "",
    secondary_before_pct: raw["secondary_before_pct"] ?? "",
    secondary_after_pct: raw["secondary_after_pct"] ?? "",
    secondary_delta_pct: raw["secondary_delta_pct"] ?? "",
    pricing_source_before: raw["pricing_source_before"] ?? "",
    pricing_source_after: raw["pricing_source_after"] ?? "",
  };
}

function toCsv(
  header: readonly string[],
  rows: readonly DeliveryRow[],
): string {
  const lines = [header.join(",")];
  for (const row of rows) {
    const baseRow: ExistingRow = row;
    lines.push(
      header
        .map((column) => csvEscape(baseRow[column as keyof ExistingRow] ?? ""))
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
      if (record.some((value) => value.length > 0)) records.push(record);
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
    if (record.some((value) => value.length > 0)) records.push(record);
  }

  return records;
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
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
