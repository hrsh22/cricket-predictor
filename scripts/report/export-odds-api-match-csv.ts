import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import {
  generateMatchBallTimeline,
  type MatchBallTimelineRow,
} from "./ball-odds-timeline.js";

interface CliOptions {
  apiKey: string;
  commentaryUrl: string;
  outputPath: string;
  sportKey: string;
  intervalMinutes: number;
  regions: string;
  bookmakers: string | null;
}

interface OddsApiHistoricalResponse {
  timestamp?: string;
  previous_timestamp?: string;
  next_timestamp?: string;
  data?: OddsApiEvent[];
}

interface OddsApiEvent {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers?: OddsApiBookmaker[];
}

interface OddsApiBookmaker {
  key: string;
  title: string;
  last_update: string;
  markets?: OddsApiMarket[];
}

interface OddsApiMarket {
  key: string;
  last_update: string;
  outcomes?: OddsApiOutcome[];
}

interface OddsApiOutcome {
  name: string;
  price: number;
}

interface SnapshotAnalysis {
  snapshotTime: string;
  bookmakerCount: number;
  consensusBattingPct: number | null;
  consensusFieldingPct: number | null;
  betfairBackBattingPct: number | null;
  betfairBackFieldingPct: number | null;
}

interface CsvRow {
  inning: number;
  ball: string;
  battingTeam: string;
  bowlingTeam: string;
  event: string;
  timestamp: string;
  consensusBeforePct: number | null;
  consensusAfterPct: number | null;
  consensusDeltaPct: number | null;
  bookmakerCountBefore: number | null;
  bookmakerCountAfter: number | null;
  betfairBackBeforePct: number | null;
  betfairBackAfterPct: number | null;
  betfairBackDeltaPct: number | null;
  snapshotTimeBefore: string | null;
  snapshotTimeAfter: string | null;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const timeline = await generateMatchBallTimeline({
    commentaryUrl: options.commentaryUrl,
    allowPartial: false,
  });

  const firstRow = timeline.rows[0];
  const secondRow = timeline.rows.find(
    (row) => row.battingTeam !== firstRow?.battingTeam,
  );
  if (firstRow === undefined || secondRow === undefined) {
    throw new Error("Could not determine both teams from the match timeline.");
  }

  const teams = [firstRow.battingTeam, secondRow.battingTeam] as const;
  const snapshots = await fetchHistoricalSnapshots(
    options,
    timeline.rows,
    teams,
  );
  const csvRows = buildCsvRows(timeline.rows, snapshots);

  await mkdir(dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, toCsv(csvRows), "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        outputPath: options.outputPath,
        deliverySourceMode: timeline.deliverySourceMode,
        deliveryCount: timeline.deliveryCount,
        snapshotCount: snapshots.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let apiKey =
    process.env["THE_ODDS_API_KEY_HISTORICAL"] ??
    process.env["THE_ODDS_API_KEY"] ??
    "";
  let commentaryUrl: string | null = null;
  let outputPath: string | null = null;
  let sportKey = "cricket_ipl";
  let intervalMinutes = 10;
  let regions = "uk";
  let bookmakers: string | null = null;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--api-key") {
      apiKey = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (argument === "--commentary-url") {
      commentaryUrl = argv[index + 1] ?? null;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? null;
      index += 1;
      continue;
    }
    if (argument === "--sport-key") {
      sportKey = argv[index + 1] ?? sportKey;
      index += 1;
      continue;
    }
    if (argument === "--interval-minutes") {
      intervalMinutes = parsePositiveInteger(argument, argv[index + 1]);
      index += 1;
      continue;
    }
    if (argument === "--regions") {
      regions = argv[index + 1] ?? regions;
      index += 1;
      continue;
    }
    if (argument === "--bookmakers") {
      bookmakers = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --api-key, --commentary-url, optional --output, --sport-key, --interval-minutes, --regions, --bookmakers.`,
    );
  }

  if (apiKey.trim().length === 0) {
    throw new Error(
      "The Odds API key is required via --api-key or THE_ODDS_API_KEY_HISTORICAL.",
    );
  }
  if (commentaryUrl === null || commentaryUrl.trim().length === 0) {
    throw new Error("--commentary-url is required.");
  }

  return {
    apiKey: apiKey.trim(),
    commentaryUrl: commentaryUrl.trim(),
    outputPath: outputPath?.trim().length
      ? outputPath.trim()
      : "data/the-odds-api-match.csv",
    sportKey,
    intervalMinutes,
    regions,
    bookmakers: bookmakers?.trim().length ? bookmakers.trim() : null,
  };
}

function parsePositiveInteger(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} requires a positive integer.`);
  }
  return parsed;
}

async function fetchHistoricalSnapshots(
  options: CliOptions,
  timelineRows: readonly MatchBallTimelineRow[],
  teams: readonly [string, string],
): Promise<SnapshotAnalysis[]> {
  const startMs = Date.parse(timelineRows[0]?.timestamp ?? "");
  const endMs = Date.parse(
    timelineRows[timelineRows.length - 1]?.timestamp ?? "",
  );
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    throw new Error(
      "Could not determine match time window from the ball timeline.",
    );
  }

  const intervalMs = options.intervalMinutes * 60_000;
  const alignedStartMs = Math.floor(startMs / intervalMs) * intervalMs;
  const snapshots: SnapshotAnalysis[] = [];

  for (
    let timeMs = alignedStartMs;
    timeMs <= endMs + intervalMs;
    timeMs += intervalMs
  ) {
    const snapshot = await fetchHistoricalSnapshotAtTime(
      options,
      new Date(timeMs),
      teams,
    );
    if (snapshot !== null) {
      snapshots.push(snapshot);
    }
  }

  return snapshots;
}

async function fetchHistoricalSnapshotAtTime(
  options: CliOptions,
  snapshotDate: Date,
  teams: readonly [string, string],
): Promise<SnapshotAnalysis | null> {
  const url = new URL(
    `https://api.the-odds-api.com/v4/historical/sports/${options.sportKey}/odds`,
  );
  url.searchParams.set("apiKey", options.apiKey);
  url.searchParams.set("regions", options.regions);
  url.searchParams.set("markets", "h2h");
  url.searchParams.set("oddsFormat", "decimal");
  url.searchParams.set("dateFormat", "iso");
  url.searchParams.set("date", toIsoWithoutMilliseconds(snapshotDate));
  if (options.bookmakers !== null) {
    url.searchParams.set("bookmakers", options.bookmakers);
  }

  const response = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0", accept: "application/json" },
  });

  if (response.status === 401 || response.status === 403) {
    const body = await response.text();
    throw new Error(
      `The Odds API historical endpoint is unavailable for this key: ${body}`,
    );
  }
  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `The Odds API historical request failed with status ${response.status}: ${body}`,
    );
  }

  const payload = (await response.json()) as OddsApiHistoricalResponse;
  const events = Array.isArray(payload.data) ? payload.data : [];
  const event = events.find((candidate) => isSameMatch(candidate, teams));
  if (event === undefined) {
    return null;
  }

  return analyzeEventSnapshot(
    event,
    payload.timestamp ?? snapshotDate.toISOString(),
    teams,
  );
}

function isSameMatch(
  event: OddsApiEvent,
  teams: readonly [string, string],
): boolean {
  const eventTeams = [event.home_team, event.away_team]
    .map(normalizeTeamName)
    .sort();
  const targetTeams = [...teams].map(normalizeTeamName).sort();
  return eventTeams[0] === targetTeams[0] && eventTeams[1] === targetTeams[1];
}

function analyzeEventSnapshot(
  event: OddsApiEvent,
  snapshotTime: string,
  teams: readonly [string, string],
): SnapshotAnalysis {
  const normalizedTeamA = normalizeTeamName(teams[0]);
  const normalizedTeamB = normalizeTeamName(teams[1]);
  const bookmakers = Array.isArray(event.bookmakers) ? event.bookmakers : [];

  const bookmakerProbabilities: Array<{ batting: number; fielding: number }> =
    [];
  let betfairBackBattingPct: number | null = null;
  let betfairBackFieldingPct: number | null = null;
  for (const bookmaker of bookmakers) {
    const markets = Array.isArray(bookmaker.markets) ? bookmaker.markets : [];
    const h2h = markets.find((market) => market.key === "h2h");
    if (h2h !== undefined) {
      const normalized = normalizeTwoWayMarket(
        h2h,
        normalizedTeamA,
        normalizedTeamB,
      );
      if (normalized !== null) {
        bookmakerProbabilities.push(normalized);
        if (bookmaker.key === "betfair_ex_uk") {
          betfairBackBattingPct = roundPct(normalized.batting);
          betfairBackFieldingPct = roundPct(normalized.fielding);
        }
      }
    }
  }

  const consensusBattingPct =
    bookmakerProbabilities.length === 0
      ? null
      : roundPct(
          bookmakerProbabilities.reduce((sum, item) => sum + item.batting, 0) /
            bookmakerProbabilities.length,
        );
  const consensusFieldingPct =
    bookmakerProbabilities.length === 0
      ? null
      : roundPct(
          bookmakerProbabilities.reduce((sum, item) => sum + item.fielding, 0) /
            bookmakerProbabilities.length,
        );

  return {
    snapshotTime,
    bookmakerCount: bookmakerProbabilities.length,
    consensusBattingPct,
    consensusFieldingPct,
    betfairBackBattingPct,
    betfairBackFieldingPct,
  };
}

function normalizeTwoWayMarket(
  market: OddsApiMarket,
  battingTeam: string,
  fieldingTeam: string,
): { batting: number; fielding: number } | null {
  const outcomes = Array.isArray(market.outcomes) ? market.outcomes : [];
  const battingOutcome = outcomes.find(
    (outcome) => normalizeTeamName(outcome.name) === battingTeam,
  );
  const fieldingOutcome = outcomes.find(
    (outcome) => normalizeTeamName(outcome.name) === fieldingTeam,
  );

  if (
    battingOutcome === undefined ||
    fieldingOutcome === undefined ||
    battingOutcome.price <= 0 ||
    fieldingOutcome.price <= 0
  ) {
    return null;
  }

  const battingRaw = 1 / battingOutcome.price;
  const fieldingRaw = 1 / fieldingOutcome.price;
  const sum = battingRaw + fieldingRaw;
  if (sum <= 0) {
    return null;
  }

  return {
    batting: battingRaw / sum,
    fielding: fieldingRaw / sum,
  };
}

function buildCsvRows(
  timelineRows: readonly MatchBallTimelineRow[],
  snapshots: readonly SnapshotAnalysis[],
): CsvRow[] {
  return timelineRows.map((row) => {
    const ballTimeMs = Date.parse(row.timestamp);
    const before = findLatestSnapshotAtOrBefore(snapshots, ballTimeMs);
    const after = findEarliestSnapshotAfter(snapshots, ballTimeMs);

    return {
      inning: row.inning,
      ball: row.ball,
      battingTeam: row.battingTeam,
      bowlingTeam: row.bowlingTeam,
      event: row.event,
      timestamp: row.timestamp,
      consensusBeforePct: before?.consensusBattingPct ?? null,
      consensusAfterPct: after?.consensusBattingPct ?? null,
      consensusDeltaPct: delta(
        before?.consensusBattingPct,
        after?.consensusBattingPct,
      ),
      bookmakerCountBefore: before?.bookmakerCount ?? null,
      bookmakerCountAfter: after?.bookmakerCount ?? null,
      betfairBackBeforePct: before?.betfairBackBattingPct ?? null,
      betfairBackAfterPct: after?.betfairBackBattingPct ?? null,
      betfairBackDeltaPct: delta(
        before?.betfairBackBattingPct,
        after?.betfairBackBattingPct,
      ),
      snapshotTimeBefore: before?.snapshotTime ?? null,
      snapshotTimeAfter: after?.snapshotTime ?? null,
    };
  });
}

function findLatestSnapshotAtOrBefore(
  snapshots: readonly SnapshotAnalysis[],
  timeMs: number,
): SnapshotAnalysis | null {
  let candidate: SnapshotAnalysis | null = null;
  for (const snapshot of snapshots) {
    if (Date.parse(snapshot.snapshotTime) > timeMs) {
      break;
    }
    candidate = snapshot;
  }
  return candidate;
}

function findEarliestSnapshotAfter(
  snapshots: readonly SnapshotAnalysis[],
  startMs: number,
): SnapshotAnalysis | null {
  for (const snapshot of snapshots) {
    const snapshotMs = Date.parse(snapshot.snapshotTime);
    if (snapshotMs <= startMs) {
      continue;
    }
    return snapshot;
  }
  return null;
}

function normalizeTeamName(value: string): string {
  const trimmed = value.trim().toLowerCase();
  if (trimmed === "royal challengers bangalore") {
    return "royal challengers bengaluru";
  }
  return trimmed;
}

function roundPct(probability: number): number {
  return Math.round(probability * 1000) / 10;
}

function delta(
  before: number | null | undefined,
  after: number | null | undefined,
): number | null {
  if (
    before === null ||
    before === undefined ||
    after === null ||
    after === undefined
  ) {
    return null;
  }
  return Math.round((after - before) * 10) / 10;
}

function toCsv(rows: readonly CsvRow[]): string {
  const header = [
    "inning",
    "ball",
    "batting_team",
    "bowling_team",
    "event",
    "timestamp",
    "consensus_before_pct",
    "consensus_after_pct",
    "consensus_delta_pct",
    "bookmaker_count_before",
    "bookmaker_count_after",
    "betfair_back_before_pct",
    "betfair_back_after_pct",
    "betfair_back_delta_pct",
    "snapshot_time_before",
    "snapshot_time_after",
  ];
  const lines = [header.join(",")];

  for (const row of rows) {
    lines.push(
      [
        row.inning,
        row.ball,
        row.battingTeam,
        row.bowlingTeam,
        row.event,
        row.timestamp,
        row.consensusBeforePct ?? "",
        row.consensusAfterPct ?? "",
        row.consensusDeltaPct ?? "",
        row.bookmakerCountBefore ?? "",
        row.bookmakerCountAfter ?? "",
        row.betfairBackBeforePct ?? "",
        row.betfairBackAfterPct ?? "",
        row.betfairBackDeltaPct ?? "",
        row.snapshotTimeBefore ?? "",
        row.snapshotTimeAfter ?? "",
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

function toIsoWithoutMilliseconds(value: Date): string {
  return value.toISOString().replace(/\.\d{3}Z$/u, "Z");
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
