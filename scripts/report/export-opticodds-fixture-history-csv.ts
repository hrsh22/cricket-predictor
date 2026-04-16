import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  fixtureId: string;
  outputPath: string;
  market: string;
  sportsbooks: string[];
}

interface OpticOddsEntry {
  timestamp: number;
  price: number | null;
  points: number | null;
  locked: boolean;
}

interface OpticOddsOdd {
  sportsbook: string;
  market: string;
  selection: string;
  normalized_selection: string;
  entries: OpticOddsEntry[];
}

interface FixturePayload {
  id: string;
  home_team_display: string;
  away_team_display: string;
  start_date: string;
  odds: OpticOddsOdd[];
}

interface HistoryRow {
  fixture_id: string;
  sportsbook: string;
  market: string;
  start_date: string;
  timestamp: string;
  home_team: string;
  away_team: string;
  changed_side: string;
  home_price_decimal: string;
  away_price_decimal: string;
  home_locked: string;
  away_locked: string;
  home_raw_implied_pct: string;
  away_raw_implied_pct: string;
  home_no_vig_pct: string;
  away_no_vig_pct: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const apiKey = process.env["OPTIC_ODDS_API_KEY"]?.trim() ?? "";
  if (apiKey.length === 0) {
    throw new Error("OPTIC_ODDS_API_KEY is required in the environment.");
  }

  const rows: HistoryRow[] = [];
  for (const sportsbook of options.sportsbooks) {
    const fixture = await fetchFixtureHistory({
      apiKey,
      fixtureId: options.fixtureId,
      market: options.market,
      sportsbook,
    });
    rows.push(...buildRowsForSportsbook(fixture, sportsbook, options.market));
  }

  rows.sort((left, right) => {
    if (left.timestamp !== right.timestamp) {
      return left.timestamp.localeCompare(right.timestamp);
    }
    return left.sportsbook.localeCompare(right.sportsbook);
  });

  await mkdir(dirname(resolve(process.cwd(), options.outputPath)), {
    recursive: true,
  });
  await writeFile(
    resolve(process.cwd(), options.outputPath),
    toCsv(rows),
    "utf8",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        fixtureId: options.fixtureId,
        outputPath: options.outputPath,
        sportsbookCount: options.sportsbooks.length,
        rowCount: rows.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let fixtureId = "20260409C60C6A40";
  let outputPath = "data/opticodds-kkr-vs-lsg-20260409-history.csv";
  let market = "Moneyline";
  let sportsbooks = [
    "bet365",
    "Betfair Exchange",
    "Betfair",
    "1XBet",
    "Betano",
  ];

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--fixture-id") {
      fixtureId = argv[index + 1] ?? fixtureId;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? outputPath;
      index += 1;
      continue;
    }
    if (argument === "--market") {
      market = argv[index + 1] ?? market;
      index += 1;
      continue;
    }
    if (argument === "--sportsbooks") {
      sportsbooks = (argv[index + 1] ?? "")
        .split(",")
        .map((value) => value.trim())
        .filter((value) => value.length > 0);
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --fixture-id, --output, --market, --sportsbooks.`,
    );
  }

  return { fixtureId, outputPath, market, sportsbooks };
}

async function fetchFixtureHistory(input: {
  apiKey: string;
  fixtureId: string;
  market: string;
  sportsbook: string;
}): Promise<FixturePayload> {
  const url = new URL(
    "https://api.opticodds.com/api/v3/fixtures/odds/historical",
  );
  url.searchParams.set("fixture_id", input.fixtureId);
  url.searchParams.set("market", input.market);
  url.searchParams.set("sportsbook", input.sportsbook);
  url.searchParams.set("odds_format", "DECIMAL");
  url.searchParams.set("include_timeseries", "true");

  const response = await fetch(url, {
    headers: {
      "X-Api-Key": input.apiKey,
      Accept: "application/json",
      "User-Agent": "Mozilla/5.0",
    },
  });
  if (!response.ok) {
    throw new Error(
      `OpticOdds historical request failed for ${input.sportsbook}: ${response.status}`,
    );
  }

  const payload = (await response.json()) as { data?: FixturePayload[] };
  const fixture = payload.data?.[0];
  if (fixture === undefined) {
    throw new Error(
      `No OpticOdds fixture history returned for ${input.sportsbook}.`,
    );
  }
  return fixture;
}

function buildRowsForSportsbook(
  fixture: FixturePayload,
  sportsbook: string,
  market: string,
): HistoryRow[] {
  const homeTeam = fixture.home_team_display;
  const awayTeam = fixture.away_team_display;
  const odds = Array.isArray(fixture.odds) ? fixture.odds : [];

  const homeOdd = odds.find(
    (odd) => normalize(odd.selection) === normalize(homeTeam),
  );
  const awayOdd = odds.find(
    (odd) => normalize(odd.selection) === normalize(awayTeam),
  );
  if (homeOdd === undefined || awayOdd === undefined) {
    return [];
  }

  const homeEntries = Array.isArray(homeOdd.entries) ? homeOdd.entries : [];
  const awayEntries = Array.isArray(awayOdd.entries) ? awayOdd.entries : [];
  const timestamps = new Set<number>();
  for (const entry of homeEntries) timestamps.add(entry.timestamp);
  for (const entry of awayEntries) timestamps.add(entry.timestamp);
  const sortedTimestamps = [...timestamps].sort((left, right) => left - right);

  const rows: HistoryRow[] = [];
  let homeIndex = 0;
  let awayIndex = 0;
  let currentHome: OpticOddsEntry | null = null;
  let currentAway: OpticOddsEntry | null = null;

  for (const timestamp of sortedTimestamps) {
    let changedSide = "";
    while (
      (homeEntries[homeIndex]?.timestamp ?? Number.POSITIVE_INFINITY) <=
      timestamp
    ) {
      currentHome = homeEntries[homeIndex] ?? null;
      if ((homeEntries[homeIndex]?.timestamp ?? -1) === timestamp) {
        changedSide = changedSide.length === 0 ? "home" : "both";
      }
      homeIndex += 1;
    }
    while (
      (awayEntries[awayIndex]?.timestamp ?? Number.POSITIVE_INFINITY) <=
      timestamp
    ) {
      currentAway = awayEntries[awayIndex] ?? null;
      if ((awayEntries[awayIndex]?.timestamp ?? -1) === timestamp) {
        changedSide = changedSide.length === 0 ? "away" : "both";
      }
      awayIndex += 1;
    }

    if (currentHome === null || currentAway === null) {
      continue;
    }

    const raw = computeRawImplied(currentHome.price, currentAway.price);
    const novig = computeNoVig(currentHome.price, currentAway.price);
    rows.push({
      fixture_id: fixture.id,
      sportsbook,
      market,
      start_date: fixture.start_date,
      timestamp: new Date(timestamp * 1000).toISOString(),
      home_team: homeTeam,
      away_team: awayTeam,
      changed_side: changedSide,
      home_price_decimal: formatDecimal(currentHome.price),
      away_price_decimal: formatDecimal(currentAway.price),
      home_locked: String(currentHome.locked),
      away_locked: String(currentAway.locked),
      home_raw_implied_pct: formatPct(raw.home),
      away_raw_implied_pct: formatPct(raw.away),
      home_no_vig_pct: formatPct(novig.home),
      away_no_vig_pct: formatPct(novig.away),
    });
  }

  return rows;
}

function computeRawImplied(
  homePrice: number | null,
  awayPrice: number | null,
): { home: number | null; away: number | null } {
  return {
    home: homePrice !== null && homePrice > 0 ? 100 / homePrice : null,
    away: awayPrice !== null && awayPrice > 0 ? 100 / awayPrice : null,
  };
}

function computeNoVig(
  homePrice: number | null,
  awayPrice: number | null,
): { home: number | null; away: number | null } {
  if (
    homePrice === null ||
    awayPrice === null ||
    homePrice <= 0 ||
    awayPrice <= 0
  ) {
    return { home: null, away: null };
  }
  const homeRaw = 1 / homePrice;
  const awayRaw = 1 / awayPrice;
  const sum = homeRaw + awayRaw;
  if (sum <= 0) {
    return { home: null, away: null };
  }
  return {
    home: (homeRaw / sum) * 100,
    away: (awayRaw / sum) * 100,
  };
}

function normalize(value: string): string {
  return value.trim().toLowerCase();
}

function formatDecimal(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "";
  }
  return String(value);
}

function formatPct(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "";
  }
  return (Math.round(value * 10) / 10).toFixed(1);
}

function toCsv(rows: readonly HistoryRow[]): string {
  const header: Array<keyof HistoryRow> = [
    "fixture_id",
    "sportsbook",
    "market",
    "start_date",
    "timestamp",
    "home_team",
    "away_team",
    "changed_side",
    "home_price_decimal",
    "away_price_decimal",
    "home_locked",
    "away_locked",
    "home_raw_implied_pct",
    "away_raw_implied_pct",
    "home_no_vig_pct",
    "away_no_vig_pct",
  ];

  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(header.map((column) => csvEscape(row[column])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function csvEscape(value: string): string {
  if (!/[",\n]/u.test(value)) {
    return value;
  }
  return `"${value.replace(/"/gu, '""')}"`;
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
