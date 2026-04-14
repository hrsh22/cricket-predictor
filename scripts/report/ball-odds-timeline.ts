import { mkdtemp, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";

import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  eventSlug: string;
  commentaryUrl: string;
  limit: number | null;
  allowPartial: boolean;
}

export interface GenerateBallOddsTimelineOptions {
  eventSlug: string;
  commentaryUrl: string;
  allowPartial?: boolean;
  limit?: number | null;
}

export interface GenerateMatchBallTimelineOptions {
  commentaryUrl: string;
  allowPartial?: boolean;
  limit?: number | null;
}

interface PricePointRow {
  tokenId: string;
  outcomeName: string;
  pointTime: string;
  price: number;
}

interface TradeRow {
  tokenId: string;
  outcomeName: string;
  tradeTime: string;
  price: number;
}

interface PriceSeries {
  tokenId: string;
  outcomeName: string;
  chart: Array<{ timeMs: number; price: number }>;
  trades: Array<{ timeMs: number; price: number }>;
}

interface EspnBall {
  id: number;
  inningNumber: 1 | 2;
  overActual: number;
  timestamp: string;
  commentText: string | null;
  totalRuns: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
  dismissalType: string | null;
}

interface EspnCommentaryData {
  ballsByKey: Map<string, EspnBall>;
  battingTeamsByInning: Map<1 | 2, string>;
}

interface CricsheetDelivery {
  inningNumber: 1 | 2;
  battingTeam: string;
  bowlingTeam: string;
  deliveryIndex: number;
  overNumber: number;
  ballInOver: number;
  displayBall: string;
  eventCode: string;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  wicket: boolean;
  wicketKind: string | null;
  commentary: string | null;
  timestamp: string;
  timestampSource: "exact" | "estimated";
}

export interface MatchBallTimelineRow {
  inning: 1 | 2;
  battingTeam: string;
  bowlingTeam: string;
  ball: string;
  event: string;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
  wicketKind: string | null;
  commentary: string | null;
  timestamp: string;
  timestampSource: "exact" | "estimated";
}

interface PricingSnapshot {
  price: number | null;
  source: "trade" | "chart" | null;
}

interface UnifiedMarketEvent {
  timeMs: number;
  primaryPrice: number;
  source: "trade" | "chart";
}

interface TimelineRow {
  inning: 1 | 2;
  battingTeam: string;
  bowlingTeam: string;
  ball: string;
  event: string;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
  wicketKind: string | null;
  commentary: string | null;
  timestamp: string;
  timestampSource: "exact" | "estimated";
  primaryTeam: string;
  primaryBefore: number | null;
  primaryAfter: number | null;
  primaryDelta: number | null;
  secondaryTeam: string;
  secondaryBefore: number | null;
  secondaryAfter: number | null;
  secondaryDelta: number | null;
  pricingSourceBefore: "trade" | "chart" | null;
  pricingSourceAfter: "trade" | "chart" | null;
}

export interface GenerateBallOddsTimelineResult {
  databaseName: string;
  eventSlug: string;
  commentaryUrl: string;
  deliverySourceMode: "full_cricsheet" | "partial_espn";
  deliveryCount: number;
  rows: TimelineRow[];
}

export interface GenerateMatchBallTimelineResult {
  commentaryUrl: string;
  deliverySourceMode: "full_cricsheet" | "partial_espn";
  deliveryCount: number;
  rows: MatchBallTimelineRow[];
}

interface CricsheetMatch {
  info?: {
    teams?: string[];
  };
  innings?: Array<{
    team?: string;
    overs?: Array<{
      over?: number;
      deliveries?: Array<unknown>;
      balls?: Array<unknown>;
    }>;
  }>;
}

let cachedCricsheetArchivePathPromise: Promise<string> | null = null;

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const result = await generateBallOddsTimeline(options);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

export async function generateMatchBallTimeline(
  options: GenerateMatchBallTimelineOptions,
): Promise<GenerateMatchBallTimelineResult> {
  const deliveries = await resolveDeliveriesFromCommentary(options);
  const limitedRows =
    options.limit === undefined || options.limit === null
      ? deliveries.deliveries
      : deliveries.deliveries.slice(0, options.limit);

  return {
    commentaryUrl: options.commentaryUrl,
    deliverySourceMode: deliveries.sourceMode,
    deliveryCount: deliveries.deliveries.length,
    rows: limitedRows.map((delivery) => ({
      inning: delivery.inningNumber,
      battingTeam: delivery.battingTeam,
      bowlingTeam: delivery.bowlingTeam,
      ball: delivery.displayBall,
      event: delivery.eventCode,
      runsTotal: delivery.runsTotal,
      batsmanRuns: delivery.batsmanRuns,
      wides: delivery.wides,
      noballs: delivery.noballs,
      byes: delivery.byes,
      legbyes: delivery.legbyes,
      isWicket: delivery.wicket,
      wicketKind: delivery.wicketKind,
      commentary: delivery.commentary,
      timestamp: delivery.timestamp,
      timestampSource: delivery.timestampSource,
    })),
  };
}

export async function generateBallOddsTimeline(
  options: GenerateBallOddsTimelineOptions,
): Promise<GenerateBallOddsTimelineResult> {
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const [seriesByOutcome, deliveryResult] = await Promise.all([
      loadMoneylineSeries(pool, options.eventSlug),
      resolveDeliveriesFromCommentary(options),
    ]);

    const deliveries = deliveryResult.deliveries;
    const rows = buildTimelineRows(deliveries, seriesByOutcome);
    const limitedRows =
      options.limit === undefined || options.limit === null
        ? rows
        : rows.slice(0, options.limit);

    return {
      databaseName: config.databaseName,
      eventSlug: options.eventSlug,
      commentaryUrl: options.commentaryUrl,
      deliverySourceMode: deliveryResult.sourceMode,
      deliveryCount: deliveries.length,
      rows: limitedRows,
    };
  } finally {
    await closePgPool(pool);
  }
}

async function resolveDeliveriesFromCommentary(options: {
  commentaryUrl: string;
  allowPartial?: boolean;
}): Promise<{
  sourceMode: "full_cricsheet" | "partial_espn";
  deliveries: CricsheetDelivery[];
}> {
  const espnData = await loadEspnData(options.commentaryUrl);
  const deliveryResult = await loadCricsheetMatch(
    extractMatchId(options.commentaryUrl),
  )
    .then((cricsheetMatch) => ({
      sourceMode: "full_cricsheet" as const,
      deliveries: buildDeliveries(cricsheetMatch, espnData.ballsByKey),
    }))
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes("filename not matched")) {
        throw error;
      }

      return {
        sourceMode: "partial_espn" as const,
        deliveries: buildDeliveriesFromEspn(espnData),
      };
    });

  if (deliveryResult.sourceMode === "partial_espn" && !options.allowPartial) {
    throw new Error(
      [
        "❌ Full ball-by-ball coverage is not available yet.",
        "",
        "This typically occurs 1-2 weeks after match completion while Cricsheet publishes archived data.",
        "",
        "📖 See docs/CRICKET_DATA_SOURCES.md for details and alternative approaches.",
        "",
        "Current data sources:",
        "  • Cricsheet: Not yet published (typically 1-2 week lag after match)",
        "  • ESPN: Partial recent deliveries only (~50-80 balls available)",
        "",
        "Options:",
        "  1. Use --allow-partial to output available ESPN data (incomplete)",
        "  2. Wait 1-2 weeks for Cricsheet publication, then re-run without --allow-partial",
        "  3. Pre-import historical Cricsheet: pnpm db:import-cricsheet-ipl --seasons <year>",
        "",
        "To proceed with incomplete partial data, add --allow-partial flag.",
      ].join("\n"),
    );
  }

  return deliveryResult;
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let eventSlug: string | null = null;
  let commentaryUrl: string | null = null;
  let limit: number | null = null;
  let allowPartial = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--event-slug") {
      eventSlug = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--commentary-url") {
      commentaryUrl = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--limit") {
      limit = parsePositiveInteger(argv[index + 1], "--limit");
      index += 1;
      continue;
    }

    if (argument === "--allow-partial") {
      allowPartial = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --event-slug <slug>, --commentary-url <url>, optional --limit <n>, --allow-partial.`,
    );
  }

  if (eventSlug === null || eventSlug.trim().length === 0) {
    throw new Error("--event-slug is required.");
  }

  if (commentaryUrl === null || commentaryUrl.trim().length === 0) {
    throw new Error("--commentary-url is required.");
  }

  return {
    eventSlug: eventSlug.trim(),
    commentaryUrl: commentaryUrl.trim(),
    limit,
    allowPartial,
  };
}

function parsePositiveInteger(value: string | undefined, flag: string): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} expects a positive integer.`);
  }

  return parsed;
}

function extractMatchId(commentaryUrl: string): string {
  const matches = [...commentaryUrl.matchAll(/-(\d+)(?:\/|$)/gu)];
  const match = matches.at(-1);
  if (match === undefined || match[1] === undefined) {
    throw new Error(
      `Unable to extract match id from commentary URL: ${commentaryUrl}`,
    );
  }

  return match[1];
}

async function loadMoneylineSeries(
  pool: ReturnType<typeof createPgPool>,
  eventSlug: string,
): Promise<Map<string, PriceSeries>> {
  const [priceHistoryResult, tradesResult] = await Promise.all([
    pool.query<PricePointRow>(
      `
        select token_id as "tokenId", outcome_name as "outcomeName", point_time as "pointTime", price::float8 as price
        from raw_polymarket_price_history
        where event_slug = $1 and (market_type = 'moneyline' or market_type is null)
        order by point_time asc
      `,
      [eventSlug],
    ),
    pool.query<TradeRow>(
      `
        select token_id as "tokenId", outcome_name as "outcomeName", trade_time as "tradeTime", price::float8 as price
        from raw_polymarket_trades
        where event_slug = $1 and (market_type = 'moneyline' or market_type is null)
        order by trade_time asc
      `,
      [eventSlug],
    ),
  ]);

  const map = new Map<string, PriceSeries>();

  for (const row of priceHistoryResult.rows) {
    const outcomeName = row.outcomeName.trim();
    const existing = map.get(outcomeName) ?? {
      tokenId: row.tokenId,
      outcomeName,
      chart: [],
      trades: [],
    };
    existing.chart.push({
      timeMs: Date.parse(row.pointTime),
      price: row.price,
    });
    map.set(outcomeName, existing);
  }

  for (const row of tradesResult.rows) {
    const outcomeName = row.outcomeName.trim();
    const existing = map.get(outcomeName) ?? {
      tokenId: row.tokenId,
      outcomeName,
      chart: [],
      trades: [],
    };
    existing.trades.push({
      timeMs: Date.parse(row.tradeTime),
      price: row.price,
    });
    map.set(outcomeName, existing);
  }

  return map;
}

async function loadEspnData(
  commentaryUrl: string,
): Promise<EspnCommentaryData> {
  const response = await fetch(commentaryUrl, {
    headers: {
      "user-agent": "Mozilla/5.0",
      "accept-language": "en-US,en;q=0.9",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch ESPN commentary page: ${response.status}`);
  }

  const html = await response.text();
  const match = html.match(
    /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/u,
  );

  if (match === null || match[1] === undefined) {
    throw new Error("Unable to locate ESPN __NEXT_DATA__ payload.");
  }

  const nextData = JSON.parse(match[1]) as Record<string, unknown>;
  const props = readObject(nextData["props"], "props");
  const appPageProps = readObject(props["appPageProps"], "props.appPageProps");
  const data = readObject(appPageProps["data"], "props.appPageProps.data");
  const content = readObject(
    data["content"],
    "props.appPageProps.data.content",
  );
  const innings = readArray(content["innings"], "content.innings");
  const comments = readArray(content["comments"], "content.comments");
  const recentBallCommentary = readObject(
    content["recentBallCommentary"],
    "content.recentBallCommentary",
  );
  const recentBalls = readArray(
    recentBallCommentary["ballComments"],
    "content.recentBallCommentary.ballComments",
  );

  const map = new Map<string, EspnBall>();
  const battingTeamsByInning = new Map<1 | 2, string>();

  for (const inning of innings) {
    const inningRecord = readObject(inning, "inning");
    const inningNumber = readFiniteNumber(
      inningRecord["inningNumber"],
      "inning.inningNumber",
    );
    const team = readObject(inningRecord["team"], "inning.team");
    battingTeamsByInning.set(
      inningNumber === 1 ? 1 : 2,
      readString(team["longName"] ?? team["name"], "inning.team.longName"),
    );
  }

  for (const inning of innings) {
    const inningRecord = readObject(inning, "inning");
    const inningOvers = readArray(
      inningRecord["inningOvers"],
      "inning.inningOvers",
    );
    for (const over of inningOvers) {
      const overRecord = readObject(over, "over");
      const balls = Array.isArray(overRecord["balls"])
        ? overRecord["balls"]
        : [];
      for (const ball of balls) {
        if (ball === null) {
          continue;
        }
        addEspnBall(map, ball);
      }
    }
  }

  for (const ball of comments) {
    addEspnBall(map, ball);
  }
  for (const ball of recentBalls) {
    addEspnBall(map, ball);
  }

  return { ballsByKey: map, battingTeamsByInning };
}

function addEspnBall(map: Map<string, EspnBall>, value: unknown): void {
  const record = readObject(value, "espnBall");
  const inningNumber = readFiniteNumber(record["inningNumber"], "inningNumber");
  const oversActual = readFiniteNumber(record["oversActual"], "oversActual");
  const timestamp = readNullableString(record["timestamp"]);
  if (timestamp === null) {
    return;
  }
  const id = readFiniteNumber(record["id"], "id");
  const commentTextItems = Array.isArray(record["commentTextItems"])
    ? record["commentTextItems"]
    : [];
  const text = commentTextItems
    .map((item) => {
      const itemRecord = readObject(item, "commentTextItem");
      const html = itemRecord["html"];
      return typeof html === "string" ? stripHtml(html) : "";
    })
    .filter((item) => item.length > 0)
    .join(" ")
    .trim();
  const key = buildEspnBallKey(inningNumber, oversActual);
  map.set(key, {
    id,
    inningNumber: inningNumber === 1 ? 1 : 2,
    overActual: oversActual,
    timestamp,
    commentText: text.length === 0 ? null : text,
    totalRuns: readFiniteNumber(record["totalRuns"], "totalRuns"),
    batsmanRuns: readFiniteNumber(record["batsmanRuns"], "batsmanRuns"),
    wides: readOptionalFiniteNumber(record["wides"]),
    noballs: readOptionalFiniteNumber(record["noballs"]),
    byes: readOptionalFiniteNumber(record["byes"]),
    legbyes: readOptionalFiniteNumber(record["legbyes"]),
    isWicket: Boolean(record["isWicket"]),
    dismissalType: readNullableString(record["dismissalType"]),
  });
}

function buildEspnBallKey(inningNumber: number, oversActual: number): string {
  return `${inningNumber}:${oversActual.toFixed(1)}`;
}

function buildDeliveriesFromEspn(
  data: EspnCommentaryData,
): CricsheetDelivery[] {
  const inningOneTeam = data.battingTeamsByInning.get(1) ?? "Innings 1";
  const inningTwoTeam = data.battingTeamsByInning.get(2) ?? "Innings 2";
  const exactBalls = Array.from(data.ballsByKey.values()).sort((left, right) =>
    left.timestamp.localeCompare(right.timestamp),
  );

  return exactBalls.map((ball, index) => {
    const battingTeam = ball.inningNumber === 1 ? inningOneTeam : inningTwoTeam;
    const bowlingTeam = ball.inningNumber === 1 ? inningTwoTeam : inningOneTeam;

    return {
      inningNumber: ball.inningNumber,
      battingTeam,
      bowlingTeam,
      deliveryIndex: index,
      overNumber: Math.floor(ball.overActual),
      ballInOver: Math.round((ball.overActual % 1) * 10),
      displayBall: ball.overActual.toFixed(1),
      eventCode: buildEventCode({
        batsmanRuns: ball.batsmanRuns,
        totalRuns: ball.totalRuns,
        wides: ball.wides,
        noballs: ball.noballs,
        byes: ball.byes,
        legbyes: ball.legbyes,
        wicket: ball.isWicket,
      }),
      runsTotal: ball.totalRuns,
      batsmanRuns: ball.batsmanRuns,
      wides: ball.wides,
      noballs: ball.noballs,
      byes: ball.byes,
      legbyes: ball.legbyes,
      wicket: ball.isWicket,
      wicketKind: ball.dismissalType,
      commentary: ball.commentText,
      timestamp: ball.timestamp,
      timestampSource: "exact",
    };
  });
}

async function loadCricsheetMatch(matchId: string): Promise<CricsheetMatch> {
  const zipPath = await ensureCricsheetArchivePath();
  const json = await unzipEntry(zipPath, `${matchId}.json`);
  return JSON.parse(json) as CricsheetMatch;
}

async function ensureCricsheetArchivePath(): Promise<string> {
  if (cachedCricsheetArchivePathPromise !== null) {
    return cachedCricsheetArchivePathPromise;
  }

  cachedCricsheetArchivePathPromise = (async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "polymarket-ball-odds-"));
    const zipPath = join(tempDir, "ipl_json.zip");
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
    return zipPath;
  })();

  return cachedCricsheetArchivePathPromise;
}

async function unzipEntry(zipPath: string, entryName: string): Promise<string> {
  const child = spawn("unzip", ["-p", zipPath, entryName], {
    stdio: ["ignore", "pipe", "pipe"],
  });

  const stdoutChunks: Buffer[] = [];
  const stderrChunks: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));

  const exitCode: number = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolve(code ?? 1));
  });

  if (exitCode !== 0) {
    throw new Error(
      `Failed to read ${entryName} from Cricsheet archive: ${Buffer.concat(stderrChunks).toString("utf8")}`,
    );
  }

  return Buffer.concat(stdoutChunks).toString("utf8");
}

function buildDeliveries(
  match: CricsheetMatch,
  espnAnchors: Map<string, EspnBall>,
): CricsheetDelivery[] {
  const teams = match.info?.teams ?? [];
  const innings = match.innings ?? [];
  if (teams.length !== 2 || innings.length < 2) {
    throw new Error("Unexpected Cricsheet match shape for IPL T20 innings.");
  }

  const deliveriesByInning = new Map<1 | 2, CricsheetDelivery[]>();

  for (const [index, inning] of innings.entries()) {
    const inningNumber = index === 0 ? 1 : 2;
    const battingTeam =
      inning.team ?? teams[index] ?? `Innings ${inningNumber}`;
    const bowlingTeam =
      teams.find((team) => team !== battingTeam) ?? teams[0] ?? "";
    const overs = inning.overs ?? [];
    const deliveries: CricsheetDelivery[] = [];
    let deliveryIndex = 0;

    for (const over of overs) {
      const overNumber = over.over ?? 0;
      const balls = Array.isArray(over.balls)
        ? over.balls
        : Array.isArray(over.deliveries)
          ? over.deliveries
          : [];

      for (let ballIndex = 0; ballIndex < balls.length; ballIndex += 1) {
        const delivery = readObject(balls[ballIndex], "delivery");
        const runs = readObject(delivery["runs"], "delivery.runs");
        const extras =
          delivery["extras"] === undefined || delivery["extras"] === null
            ? {}
            : readObject(delivery["extras"], "delivery.extras");
        const wides = readOptionalFiniteNumber(extras["wides"]);
        const noballs = readOptionalFiniteNumber(extras["noballs"]);
        const byes = readOptionalFiniteNumber(extras["byes"]);
        const legbyes = readOptionalFiniteNumber(extras["legbyes"]);
        const batsmanRuns = readFiniteNumber(runs["batter"], "runs.batter");
        const totalRuns = readFiniteNumber(runs["total"], "runs.total");
        const wicketEntry = Array.isArray(delivery["wickets"])
          ? delivery["wickets"][0]
          : delivery["wicket"];
        const wicket = wicketEntry !== undefined && wicketEntry !== null;
        const wicketKind = wicket
          ? readNullableString(
              readObject(wicketEntry, "delivery.wicket")["kind"],
            )
          : null;

        deliveries.push({
          inningNumber,
          battingTeam,
          bowlingTeam,
          deliveryIndex,
          overNumber,
          ballInOver: ballIndex + 1,
          displayBall: `${overNumber}.${ballIndex + 1}`,
          eventCode: buildEventCode({
            batsmanRuns,
            totalRuns,
            wides,
            noballs,
            byes,
            legbyes,
            wicket,
          }),
          runsTotal: totalRuns,
          batsmanRuns,
          wides,
          noballs,
          byes,
          legbyes,
          wicket,
          wicketKind,
          commentary: null,
          timestamp: "",
          timestampSource: "estimated",
        });
        deliveryIndex += 1;
      }
    }

    deliveriesByInning.set(
      inningNumber,
      enrichInningTimestamps(deliveries, espnAnchors),
    );
  }

  return [
    ...(deliveriesByInning.get(1) ?? []),
    ...(deliveriesByInning.get(2) ?? []),
  ];
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
  if (input.wides > 0) {
    parts.push(input.wides === 1 ? "wd" : `${input.wides}wd`);
  }
  if (input.noballs > 0) {
    parts.push(input.noballs === 1 ? "nb" : `${input.noballs}nb`);
  }
  if (input.byes > 0) {
    parts.push(input.byes === 1 ? "b" : `${input.byes}b`);
  }
  if (input.legbyes > 0) {
    parts.push(input.legbyes === 1 ? "lb" : `${input.legbyes}lb`);
  }
  if (input.batsmanRuns > 0) {
    parts.push(String(input.batsmanRuns));
  }
  if (parts.length === 0) {
    parts.push(input.totalRuns === 0 ? "0" : String(input.totalRuns));
  }
  if (input.wicket) {
    parts.push("W");
  }

  return parts.join("+");
}

function enrichInningTimestamps(
  deliveries: CricsheetDelivery[],
  espnAnchors: Map<string, EspnBall>,
): CricsheetDelivery[] {
  const exactAnchors: Array<{
    index: number;
    timeMs: number;
    commentary: string | null;
  }> = [];

  for (const delivery of deliveries) {
    const oversActual = Number.parseFloat(delivery.displayBall);
    const espnBall = espnAnchors.get(
      buildEspnBallKey(delivery.inningNumber, oversActual),
    );
    if (espnBall !== undefined) {
      const timeMs = Date.parse(espnBall.timestamp);
      exactAnchors.push({
        index: delivery.deliveryIndex,
        timeMs,
        commentary: espnBall.commentText,
      });
    }
  }

  exactAnchors.sort((left, right) => left.index - right.index);

  if (deliveries.length === 0) {
    return deliveries;
  }

  if (deliveries[0]?.inningNumber === 1 && exactAnchors.length > 0) {
    const secondInningsFirst = espnAnchors.get(buildEspnBallKey(2, 0.1));
    if (secondInningsFirst !== undefined) {
      exactAnchors.push({
        index: deliveries.length - 1,
        timeMs: Date.parse(secondInningsFirst.timestamp) - 15 * 60 * 1000,
        commentary: null,
      });
      exactAnchors.sort((left, right) => left.index - right.index);
    }
  }

  if (deliveries[0]?.inningNumber === 2 && exactAnchors.length >= 2) {
    exactAnchors.sort((left, right) => left.index - right.index);
  }

  if (exactAnchors.length === 0) {
    throw new Error(
      `No ESPN timing anchors found for inning ${deliveries[0]?.inningNumber ?? "?"}.`,
    );
  }

  for (const delivery of deliveries) {
    const exact = exactAnchors.find(
      (anchor) => anchor.index === delivery.deliveryIndex,
    );
    if (exact !== undefined) {
      delivery.timestamp = new Date(exact.timeMs).toISOString();
      delivery.timestampSource = "exact";
      delivery.commentary = exact.commentary;
      continue;
    }

    const previous = findPreviousAnchor(exactAnchors, delivery.deliveryIndex);
    const next = findNextAnchor(exactAnchors, delivery.deliveryIndex);
    let estimatedMs: number;
    if (previous !== null && next !== null && next.index !== previous.index) {
      const fraction =
        (delivery.deliveryIndex - previous.index) /
        (next.index - previous.index);
      estimatedMs = Math.round(
        previous.timeMs + fraction * (next.timeMs - previous.timeMs),
      );
    } else if (previous !== null) {
      estimatedMs =
        previous.timeMs + (delivery.deliveryIndex - previous.index) * 45_000;
    } else if (next !== null) {
      estimatedMs =
        next.timeMs - (next.index - delivery.deliveryIndex) * 45_000;
    } else {
      estimatedMs = Date.now();
    }

    delivery.timestamp = new Date(estimatedMs).toISOString();
    delivery.timestampSource = "estimated";
  }

  return deliveries;
}

function findPreviousAnchor(
  anchors: readonly {
    index: number;
    timeMs: number;
    commentary: string | null;
  }[],
  index: number,
): { index: number; timeMs: number; commentary: string | null } | null {
  let previous: {
    index: number;
    timeMs: number;
    commentary: string | null;
  } | null = null;
  for (const anchor of anchors) {
    if (anchor.index >= index) {
      break;
    }
    previous = anchor;
  }
  return previous;
}

function findNextAnchor(
  anchors: readonly {
    index: number;
    timeMs: number;
    commentary: string | null;
  }[],
  index: number,
): { index: number; timeMs: number; commentary: string | null } | null {
  for (const anchor of anchors) {
    if (anchor.index > index) {
      return anchor;
    }
  }
  return null;
}

function buildTimelineRows(
  deliveries: readonly CricsheetDelivery[],
  seriesByOutcome: Map<string, PriceSeries>,
): TimelineRow[] {
  const primaryTeam = deliveries[0]?.battingTeam;
  const secondaryTeam = deliveries[0]?.bowlingTeam;

  if (primaryTeam === undefined || secondaryTeam === undefined) {
    throw new Error("No deliveries were available to derive team names.");
  }

  const primarySeries = findSeries(seriesByOutcome, primaryTeam);
  const secondarySeries = findSeries(seriesByOutcome, secondaryTeam);
  const unifiedTradeEvents = buildUnifiedMarketEvents(
    primarySeries,
    secondarySeries,
    "trade",
  );
  const unifiedChartEvents = buildUnifiedMarketEvents(
    primarySeries,
    secondarySeries,
    "chart",
  );

  return deliveries.map((delivery, index) => {
    const ballTimeMs = Date.parse(delivery.timestamp);
    const nextBallTimeMs =
      index < deliveries.length - 1
        ? Date.parse(deliveries[index + 1]?.timestamp ?? delivery.timestamp)
        : ballTimeMs + 120_000;
    const primaryBefore = getBeforePrice(
      unifiedTradeEvents,
      unifiedChartEvents,
      ballTimeMs,
    );
    const primaryAfter = getAfterPrice(
      unifiedTradeEvents,
      unifiedChartEvents,
      ballTimeMs,
      nextBallTimeMs,
    );

    return {
      inning: delivery.inningNumber,
      battingTeam: delivery.battingTeam,
      bowlingTeam: delivery.bowlingTeam,
      ball: delivery.displayBall,
      event: delivery.eventCode,
      runsTotal: delivery.runsTotal,
      batsmanRuns: delivery.batsmanRuns,
      wides: delivery.wides,
      noballs: delivery.noballs,
      byes: delivery.byes,
      legbyes: delivery.legbyes,
      isWicket: delivery.wicket,
      wicketKind: delivery.wicketKind,
      commentary: delivery.commentary,
      timestamp: delivery.timestamp,
      timestampSource: delivery.timestampSource,
      primaryTeam,
      primaryBefore: asPercent(primaryBefore.price),
      primaryAfter: asPercent(primaryAfter.price),
      primaryDelta: asDelta(primaryBefore.price, primaryAfter.price),
      secondaryTeam,
      secondaryBefore: asPercent(complementPrice(primaryBefore.price)),
      secondaryAfter: asPercent(complementPrice(primaryAfter.price)),
      secondaryDelta: asDelta(
        complementPrice(primaryBefore.price),
        complementPrice(primaryAfter.price),
      ),
      pricingSourceBefore: chooseCombinedSource(
        primaryBefore.source,
        primaryBefore.source,
      ),
      pricingSourceAfter: chooseCombinedSource(
        primaryAfter.source,
        primaryAfter.source,
      ),
    };
  });
}

function findSeries(
  seriesByOutcome: Map<string, PriceSeries>,
  outcomeName: string,
): PriceSeries {
  const candidates = resolveTeamNameAliases(outcomeName);
  const series = candidates
    .map((candidate) => seriesByOutcome.get(candidate))
    .find((value) => value !== undefined);
  if (series === undefined) {
    throw new Error(`Missing Polymarket moneyline series for ${outcomeName}.`);
  }
  return series;
}

function resolveTeamNameAliases(outcomeName: string): string[] {
  const aliases = [outcomeName];
  const normalized = outcomeName.toLowerCase();

  if (outcomeName.includes("Bengaluru")) {
    aliases.push(outcomeName.replace("Bengaluru", "Bangalore"));
  }
  if (outcomeName.includes("Bangalore")) {
    aliases.push(outcomeName.replace("Bangalore", "Bengaluru"));
  }
  if (normalized === "delhi capitals") {
    aliases.push("Delhi");
  }
  if (normalized === "mumbai indians") {
    aliases.push("Mumbai");
  }
  if (normalized === "gujarat titans") {
    aliases.push("Gujarat");
  }
  if (normalized === "rajasthan royals") {
    aliases.push("Rajasthan");
  }
  if (normalized === "sunrisers hyderabad") {
    aliases.push("Hyderabad");
  }
  if (normalized === "lucknow super giants") {
    aliases.push("Lucknow");
  }
  if (normalized === "chennai super kings") {
    aliases.push("Chennai");
  }
  if (normalized === "kolkata knight riders") {
    aliases.push("Kolkata");
  }
  if (normalized === "punjab kings") {
    aliases.push("Punjab");
  }
  if (
    normalized === "royal challengers bengaluru" ||
    normalized === "royal challengers bangalore"
  ) {
    aliases.push("Bangalore", "Bengaluru", "Royal Challengers Bangalore");
  }
  return aliases;
}

function buildUnifiedMarketEvents(
  primarySeries: PriceSeries,
  secondarySeries: PriceSeries,
  source: "trade" | "chart",
): UnifiedMarketEvent[] {
  const primaryPoints =
    source === "trade" ? primarySeries.trades : primarySeries.chart;
  const secondaryPoints =
    source === "trade" ? secondarySeries.trades : secondarySeries.chart;

  return [
    ...primaryPoints.map((point) => ({
      timeMs: point.timeMs,
      primaryPrice: point.price,
      source,
    })),
    ...secondaryPoints.map((point) => ({
      timeMs: point.timeMs,
      primaryPrice: 1 - point.price,
      source,
    })),
  ].sort((left, right) => left.timeMs - right.timeMs);
}

function getBeforePrice(
  tradeEvents: readonly UnifiedMarketEvent[],
  chartEvents: readonly UnifiedMarketEvent[],
  timeMs: number,
): PricingSnapshot {
  const trade = findLatestEventBefore(tradeEvents, timeMs, 5 * 60 * 1000);
  if (trade !== null) {
    return { price: trade.primaryPrice, source: "trade" };
  }

  const chart = findLatestEventAtOrBefore(chartEvents, timeMs);
  return {
    price: chart?.primaryPrice ?? null,
    source: chart === null ? null : "chart",
  };
}

function getAfterPrice(
  tradeEvents: readonly UnifiedMarketEvent[],
  chartEvents: readonly UnifiedMarketEvent[],
  startMs: number,
  endMs: number,
): PricingSnapshot {
  const trade = findEarliestEventInWindow(tradeEvents, startMs, endMs);
  if (trade !== null) {
    return { price: trade.primaryPrice, source: "trade" };
  }

  const chart = findLatestEventAtOrBefore(chartEvents, endMs);
  return {
    price: chart?.primaryPrice ?? null,
    source: chart === null ? null : "chart",
  };
}

function findLatestEventBefore(
  events: readonly UnifiedMarketEvent[],
  timeMs: number,
  lookbackMs: number,
): UnifiedMarketEvent | null {
  let candidate: UnifiedMarketEvent | null = null;
  for (const event of events) {
    if (event.timeMs >= timeMs) {
      break;
    }
    if (event.timeMs >= timeMs - lookbackMs) {
      candidate = event;
    }
  }
  return candidate;
}

function findEarliestEventInWindow(
  events: readonly UnifiedMarketEvent[],
  startMs: number,
  endMs: number,
): UnifiedMarketEvent | null {
  for (const event of events) {
    if (event.timeMs <= startMs) {
      continue;
    }
    if (event.timeMs > endMs) {
      break;
    }
    return event;
  }
  return null;
}

function findLatestEventAtOrBefore(
  events: readonly UnifiedMarketEvent[],
  timeMs: number,
): UnifiedMarketEvent | null {
  let candidate: UnifiedMarketEvent | null = null;
  for (const event of events) {
    if (event.timeMs > timeMs) {
      break;
    }
    candidate = event;
  }
  return candidate;
}

function complementPrice(price: number | null): number | null {
  return price === null ? null : 1 - price;
}

function asPercent(value: number | null): number | null {
  return value === null ? null : round(value * 100, 1);
}

function asDelta(before: number | null, after: number | null): number | null {
  if (before === null || after === null) {
    return null;
  }
  return round((after - before) * 100, 1);
}

function chooseCombinedSource(
  first: "trade" | "chart" | null,
  second: "trade" | "chart" | null,
): "trade" | "chart" | null {
  if (first === "trade" || second === "trade") {
    return "trade";
  }
  if (first === "chart" || second === "chart") {
    return "chart";
  }
  return null;
}

function round(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function stripHtml(value: string): string {
  return value
    .replace(/<[^>]+>/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function readObject(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Expected ${path} to be an object.`);
  }
  return value as Record<string, unknown>;
}

function readArray(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`Expected ${path} to be an array.`);
  }
  return value;
}

function readString(value: unknown, path: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Expected ${path} to be a non-empty string.`);
  }
  return value;
}

function readFiniteNumber(value: unknown, path: string): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  throw new Error(`Expected ${path} to be a finite number.`);
}

function readOptionalFiniteNumber(value: unknown): number {
  if (value === undefined || value === null) {
    return 0;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function readNullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  void main().catch((error: unknown) => {
    const message =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
