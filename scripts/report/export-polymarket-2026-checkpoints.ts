import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";

interface CsvRow {
  [key: string]: string;
}

interface MatchContext {
  sourceMatchId: string;
  matchDate: string;
  matchSlug: string;
  eventSlug: string;
  firstBallMs: number;
  winner: string;
}

interface SnapshotRow {
  outcomeName: string;
  pointMs: number;
  pointTime: string;
  price: number;
}

interface GammaEventPayload {
  startDate: string;
  markets: Array<{
    slug: string;
    sportsMarketType: string | null;
    clobTokenIds: string[] | string;
    outcomes: string[] | string;
  }>;
}

interface CheckpointSelection {
  snapshotTime: string | null;
  favorite: string | null;
  favoritePct: number | null;
  underdog: string | null;
  underdogPct: number | null;
}

async function main(): Promise<void> {
  const eventStudyPath = path.resolve(
    "data/analysis/polymarket-event-study-2026.csv",
  );
  const outputCsvPath = path.resolve(
    "data/analysis/polymarket-2026-checkpoint-odds.csv",
  );
  const outputSummaryPath = path.resolve(
    "data/analysis/polymarket-2026-checkpoint-summary.json",
  );

  const eventStudyRows = parseCsv(fs.readFileSync(eventStudyPath, "utf8"));
  const matches = deriveMatches(eventStudyRows);

  const client = new Client({
    connectionString:
      process.env.DATABASE_URL ??
      "postgresql://localhost:5432/sports_predictor_mvp",
  });
  await client.connect();

  try {
    const result = await client.query<{
      event_slug: string;
      market_type: string;
      outcome_name: string;
      point_time: Date;
      price: number;
    }>(`
      select
        event_slug,
        market_type,
        outcome_name,
        point_time,
        price::float8 as price
      from raw_polymarket_price_history
      where event_slug like 'cricipl-%-2026-%'
      order by event_slug asc, market_type asc, point_time asc
    `);

    const byEventMarket = new Map<string, SnapshotRow[]>();
    for (const row of result.rows) {
      const key = `${row.event_slug}::${row.market_type}`;
      if (!byEventMarket.has(key)) {
        byEventMarket.set(key, []);
      }
      byEventMarket.get(key)?.push({
        outcomeName: row.outcome_name.trim(),
        pointMs: row.point_time.getTime(),
        pointTime: row.point_time.toISOString(),
        price: row.price,
      });
    }

    await hydrateMissingEventMarkets(matches, byEventMarket);

    const outputRows = matches.map((match) => {
      const moneylineRows =
        byEventMarket.get(`${match.eventSlug}::moneyline`) ?? [];
      const tossRows =
        byEventMarket.get(`${match.eventSlug}::cricket_toss_winner`) ?? [];

      const preMatch = latestByOutcomeFavorite(
        moneylineRows,
        match.firstBallMs,
      );
      const tossResolveTime = latestUnresolvedTossTime(
        tossRows,
        match.firstBallMs,
      );
      const preToss =
        tossResolveTime === null
          ? emptySelection()
          : latestByOutcomeFavorite(moneylineRows, tossResolveTime);
      const postToss =
        tossResolveTime === null
          ? emptySelection()
          : firstByOutcomeFavoriteAfter(
              moneylineRows,
              tossResolveTime,
              match.firstBallMs,
            );

      return {
        source_match_id: match.sourceMatchId,
        match_date: match.matchDate,
        match_slug: match.matchSlug,
        event_slug: match.eventSlug,
        winner_team: match.winner,
        pre_toss_snapshot_time: preToss.snapshotTime ?? "",
        pre_toss_favorite: preToss.favorite ?? "",
        pre_toss_favorite_pct: fmt(preToss.favoritePct),
        pre_toss_underdog: preToss.underdog ?? "",
        pre_toss_underdog_pct: fmt(preToss.underdogPct),
        post_toss_snapshot_time: postToss.snapshotTime ?? "",
        post_toss_favorite: postToss.favorite ?? "",
        post_toss_favorite_pct: fmt(postToss.favoritePct),
        post_toss_underdog: postToss.underdog ?? "",
        post_toss_underdog_pct: fmt(postToss.underdogPct),
        pre_match_snapshot_time: preMatch.snapshotTime ?? "",
        pre_match_favorite: preMatch.favorite ?? "",
        pre_match_favorite_pct: fmt(preMatch.favoritePct),
        pre_match_underdog: preMatch.underdog ?? "",
        pre_match_underdog_pct: fmt(preMatch.underdogPct),
      };
    });

    writeCsv(outputCsvPath, outputRows);

    const summary = {
      matches_considered: outputRows.length,
      pre_toss: summarizeWinRate(outputRows, "pre_toss_favorite"),
      post_toss: summarizeWinRate(outputRows, "post_toss_favorite"),
      pre_match: summarizeWinRate(outputRows, "pre_match_favorite"),
    };

    fs.mkdirSync(path.dirname(outputSummaryPath), { recursive: true });
    fs.writeFileSync(
      outputSummaryPath,
      JSON.stringify(summary, null, 2) + "\n",
    );

    process.stdout.write(
      JSON.stringify(
        {
          outputCsvPath,
          outputSummaryPath,
          ...summary,
        },
        null,
        2,
      ) + "\n",
    );
  } finally {
    await client.end();
  }
}

async function hydrateMissingEventMarkets(
  matches: MatchContext[],
  byEventMarket: Map<string, SnapshotRow[]>,
): Promise<void> {
  const fetched = new Set<string>();
  for (const match of matches) {
    for (const marketType of ["moneyline", "cricket_toss_winner"] as const) {
      const key = `${match.eventSlug}::${marketType}`;
      if ((byEventMarket.get(key)?.length ?? 0) > 0 || fetched.has(key)) {
        continue;
      }
      const rows = await fetchMarketHistoryRows(
        match.eventSlug,
        marketType,
        match.firstBallMs,
      );
      if (rows.length > 0) {
        byEventMarket.set(key, rows);
      }
      fetched.add(key);
    }
  }
}

async function fetchMarketHistoryRows(
  eventSlug: string,
  marketType: "moneyline" | "cricket_toss_winner",
  firstBallMs: number,
): Promise<SnapshotRow[]> {
  const response = await fetch(
    `https://gamma-api.polymarket.com/events?slug=${eventSlug}`,
    {
      headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" },
    },
  );
  if (!response.ok) {
    return [];
  }
  const payload = (await response.json()) as GammaEventPayload[];
  const event = payload[0];
  if (!event) {
    return [];
  }
  const market = event.markets.find(
    (entry) => entry.sportsMarketType === marketType,
  );
  if (!market) {
    return [];
  }

  const tokenIds = Array.isArray(market.clobTokenIds)
    ? market.clobTokenIds
    : JSON.parse(market.clobTokenIds);
  const outcomes = Array.isArray(market.outcomes)
    ? market.outcomes
    : JSON.parse(market.outcomes);

  const startMs = Math.min(
    Date.parse(event.startDate),
    firstBallMs - 7 * 24 * 60 * 60 * 1000,
  );
  const endMs = firstBallMs + 60 * 60 * 1000;
  const rows: SnapshotRow[] = [];

  for (let index = 0; index < tokenIds.length; index += 1) {
    const tokenId = tokenIds[index];
    const outcomeName = String(outcomes[index] ?? tokenId).trim();
    const url = new URL("https://clob.polymarket.com/prices-history");
    url.searchParams.set("market", tokenId);
    url.searchParams.set("startTs", String(Math.floor(startMs / 1000)));
    url.searchParams.set("endTs", String(Math.floor(endMs / 1000)));
    url.searchParams.set("fidelity", "60");

    const historyResponse = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        Accept: "application/json",
        Origin: "https://polymarket.com",
        Referer: "https://polymarket.com/",
      },
    });
    if (!historyResponse.ok) {
      continue;
    }
    const historyPayload = (await historyResponse.json()) as {
      history?: Array<{ t: number; p: number }>;
    };
    for (const point of historyPayload.history ?? []) {
      rows.push({
        outcomeName,
        pointMs: point.t * 1000,
        pointTime: new Date(point.t * 1000).toISOString(),
        price: point.p,
      });
    }
  }

  rows.sort((left, right) => left.pointMs - right.pointMs);
  return rows;
}

function parseCsv(text: string): CsvRow[] {
  const lines = text.trim().split("\n");
  const header = lines[0]?.split(",") ?? [];
  return lines.slice(1).map((line) => {
    const values: string[] = [];
    let current = "";
    let inQuotes = false;
    for (let index = 0; index < line.length; index += 1) {
      const character = line[index];
      if (character === '"') {
        if (inQuotes && line[index + 1] === '"') {
          current += '"';
          index += 1;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (character === "," && !inQuotes) {
        values.push(current);
        current = "";
      } else {
        current += character;
      }
    }
    values.push(current);
    return Object.fromEntries(
      header.map((name, index) => [name, values[index] ?? ""]),
    );
  });
}

function deriveMatches(rows: CsvRow[]): MatchContext[] {
  const byMatch = new Map<string, CsvRow[]>();
  for (const row of rows) {
    if (!byMatch.has(row.source_match_id)) {
      byMatch.set(row.source_match_id, []);
    }
    byMatch.get(row.source_match_id)?.push(row);
  }

  const matches: MatchContext[] = [];
  for (const [sourceMatchId, matchRows] of byMatch.entries()) {
    matchRows.sort(
      (left, right) =>
        Number(left.inning) - Number(right.inning) ||
        Number(left.ball) - Number(right.ball),
    );

    const innings = new Map<number, { runs: number; team: string }>();
    for (const row of matchRows) {
      const inning = Number(row.inning);
      const current = innings.get(inning) ?? {
        runs: 0,
        team: row.batting_team,
      };
      current.runs += parseEventRuns(row.event).total;
      innings.set(inning, current);
    }
    if (innings.size < 2) {
      continue;
    }

    const firstInnings = innings.get(1);
    const secondInnings = innings.get(2);
    if (!firstInnings || !secondInnings) {
      continue;
    }

    matches.push({
      sourceMatchId,
      matchDate: matchRows[0]?.match_date ?? "",
      matchSlug: matchRows[0]?.match_slug ?? "",
      eventSlug: matchRows[0]?.event_slug ?? "",
      firstBallMs: Date.parse(matchRows[0]?.timestamp ?? ""),
      winner:
        firstInnings.runs > secondInnings.runs
          ? firstInnings.team
          : secondInnings.team,
    });
  }

  matches.sort((left, right) => left.matchDate.localeCompare(right.matchDate));
  return matches;
}

function parseEventRuns(event: string): { total: number } {
  const parts = (event || "").split("+");
  let total = 0;
  for (const part of parts) {
    if (part === "W" || part === "") {
      continue;
    }
    if (part.endsWith("wd")) {
      total += part.slice(0, -2) ? Number.parseInt(part.slice(0, -2), 10) : 1;
      continue;
    }
    if (part.endsWith("nb")) {
      total += part.slice(0, -2) ? Number.parseInt(part.slice(0, -2), 10) : 1;
      continue;
    }
    if (part.endsWith("lb")) {
      total += part.slice(0, -2) ? Number.parseInt(part.slice(0, -2), 10) : 1;
      continue;
    }
    if (part.endsWith("b")) {
      total += part.slice(0, -1) ? Number.parseInt(part.slice(0, -1), 10) : 1;
      continue;
    }
    if (/^\d+$/u.test(part)) {
      total += Number.parseInt(part, 10);
    }
  }
  return { total };
}

function latestByOutcomeFavorite(
  rows: SnapshotRow[],
  cutoffMs: number,
): CheckpointSelection {
  const latest = new Map<string, SnapshotRow>();
  for (const row of rows) {
    if (row.pointMs > cutoffMs) {
      break;
    }
    latest.set(row.outcomeName, row);
  }
  return selectionFromLatest(latest);
}

function latestUnresolvedTossTime(
  rows: SnapshotRow[],
  cutoffMs: number,
): number | null {
  const latest = new Map<string, number>();
  let candidate: number | null = null;
  for (const row of rows) {
    if (row.pointMs > cutoffMs) {
      break;
    }
    latest.set(row.outcomeName, row.price);
    if (latest.size >= 2) {
      const prices = [...latest.values()];
      if (Math.max(...prices) < 0.99 && Math.min(...prices) > 0.01) {
        candidate = row.pointMs;
      }
    }
  }
  return candidate;
}

function firstByOutcomeFavoriteAfter(
  rows: SnapshotRow[],
  afterMs: number,
  beforeMs: number,
): CheckpointSelection {
  const latest = new Map<string, SnapshotRow>();
  for (const row of rows) {
    if (row.pointMs <= afterMs) {
      continue;
    }
    if (row.pointMs > beforeMs) {
      break;
    }
    latest.set(row.outcomeName, row);
    if (latest.size >= 2) {
      return selectionFromLatest(latest);
    }
  }
  return emptySelection();
}

function selectionFromLatest(
  latest: Map<string, SnapshotRow>,
): CheckpointSelection {
  if (latest.size < 2) {
    return emptySelection();
  }
  const ordered = [...latest.values()].sort(
    (left, right) => right.price - left.price,
  );
  const favorite = ordered[0];
  const underdog = ordered[1];
  return {
    snapshotTime:
      favorite.pointTime > underdog.pointTime
        ? favorite.pointTime
        : underdog.pointTime,
    favorite: favorite.outcomeName,
    favoritePct: roundPct(favorite.price),
    underdog: underdog.outcomeName,
    underdogPct: roundPct(underdog.price),
  };
}

function emptySelection(): CheckpointSelection {
  return {
    snapshotTime: null,
    favorite: null,
    favoritePct: null,
    underdog: null,
    underdogPct: null,
  };
}

function roundPct(price: number): number {
  return Math.round(price * 1000) / 10;
}

function fmt(value: number | null): string {
  if (value === null) {
    return "";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

function summarizeWinRate(rows: Array<Record<string, string>>, column: string) {
  const eligible = rows.filter((row) => row[column] !== "");
  const wins = eligible.filter((row) => row[column] === row.winner_team).length;
  return {
    total: eligible.length,
    wins,
    win_rate_pct: eligible.length ? roundPct(wins / eligible.length) : null,
    examples: eligible.slice(0, 3).map((row) => ({
      match_id: row.source_match_id,
      favorite: row[column],
      winner: row.winner_team,
    })),
  };
}

function writeCsv(pathname: string, rows: Array<Record<string, string>>) {
  if (rows.length === 0) {
    throw new Error("No checkpoint rows to write");
  }
  fs.mkdirSync(path.dirname(pathname), { recursive: true });
  const header = Object.keys(rows[0]);
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(header.map((key) => csvEscape(row[key] ?? "")).join(","));
  }
  fs.writeFileSync(pathname, `${lines.join("\n")}\n`);
}

function csvEscape(value: string) {
  return /[",\n]/u.test(value) ? `"${value.replace(/"/gu, '""')}"` : value;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
