import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  eventSlug: string;
  marketSlug: string | null;
  marketType: string | null;
  outcomeName: string | null;
  limitHistory: number;
  limitTrades: number;
}

interface SqlFragment {
  whereClause: string;
  params: unknown[];
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const filters = buildFilters(options);
    const [overview, priceHistoryRows, tradeRows] = await Promise.all([
      loadOverview(pool, filters),
      loadPriceHistoryRows(pool, filters, options.limitHistory),
      loadTradeRows(pool, filters, options.limitTrades),
    ]);

    process.stdout.write(
      `${JSON.stringify(
        {
          databaseName: config.databaseName,
          eventSlug: options.eventSlug,
          filters: {
            marketSlug: options.marketSlug,
            marketType: options.marketType,
            outcomeName: options.outcomeName,
          },
          overview,
          latestPriceHistory: priceHistoryRows,
          latestTrades: tradeRows,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let eventSlug: string | null = null;
  let marketSlug: string | null = null;
  let marketType: string | null = null;
  let outcomeName: string | null = null;
  let limitHistory = 20;
  let limitTrades = 20;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--event-slug") {
      eventSlug = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--market-slug") {
      marketSlug = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--market-type") {
      marketType = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--outcome-name") {
      outcomeName = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--limit-history") {
      limitHistory = parsePositiveInteger(argv[index + 1], "--limit-history");
      index += 1;
      continue;
    }

    if (argument === "--limit-trades") {
      limitTrades = parsePositiveInteger(argv[index + 1], "--limit-trades");
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --event-slug <slug> and optional --market-slug <slug>, --market-type <type>, --outcome-name <name>, --limit-history <n>, --limit-trades <n>.`,
    );
  }

  if (eventSlug === null || eventSlug.trim().length === 0) {
    throw new Error("--event-slug is required.");
  }

  return {
    eventSlug: eventSlug.trim(),
    marketSlug: normalizeOptionalArg(marketSlug),
    marketType: normalizeOptionalArg(marketType),
    outcomeName: normalizeOptionalArg(outcomeName),
    limitHistory,
    limitTrades,
  };
}

function normalizeOptionalArg(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function parsePositiveInteger(value: string | undefined, flag: string): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} expects a positive integer.`);
  }

  return parsed;
}

function buildFilters(options: CliOptions): SqlFragment {
  const clauses = ["event_slug = $1"];
  const params: unknown[] = [options.eventSlug];

  if (options.marketSlug !== null) {
    params.push(options.marketSlug);
    clauses.push(`market_slug = $${params.length}`);
  }

  if (options.marketType !== null) {
    params.push(options.marketType);
    clauses.push(`market_type = $${params.length}`);
  }

  if (options.outcomeName !== null) {
    params.push(options.outcomeName);
    clauses.push(`outcome_name = $${params.length}`);
  }

  return {
    whereClause: clauses.join(" and "),
    params,
  };
}

async function loadOverview(
  pool: ReturnType<typeof createPgPool>,
  filters: SqlFragment,
): Promise<{
  priceHistoryCount: number;
  tradeCount: number;
  markets: Array<Record<string, unknown>>;
}> {
  const priceCountResult = await pool.query<{ count: string }>(
    `select count(*)::text as count from raw_polymarket_price_history where ${filters.whereClause}`,
    filters.params,
  );
  const tradeCountResult = await pool.query<{ count: string }>(
    `select count(*)::text as count from raw_polymarket_trades where ${filters.whereClause}`,
    filters.params,
  );
  const marketsResult = await pool.query<{
    source_market_id: string;
    market_slug: string;
    market_type: string | null;
    price_history_count: string;
    trade_count: string;
    first_price_point_time: Date | null;
    last_price_point_time: Date | null;
    first_trade_time: Date | null;
    last_trade_time: Date | null;
  }>(
    `
      with price_history as (
        select
          source_market_id,
          market_slug,
          market_type,
          count(*)::text as price_history_count,
          min(point_time) as first_price_point_time,
          max(point_time) as last_price_point_time
        from raw_polymarket_price_history
        where ${filters.whereClause}
        group by source_market_id, market_slug, market_type
      ),
      trades as (
        select
          source_market_id,
          count(*)::text as trade_count,
          min(trade_time) as first_trade_time,
          max(trade_time) as last_trade_time
        from raw_polymarket_trades
        where ${filters.whereClause}
        group by source_market_id
      )
      select
        ph.source_market_id,
        ph.market_slug,
        ph.market_type,
        ph.price_history_count,
        coalesce(tr.trade_count, '0') as trade_count,
        ph.first_price_point_time,
        ph.last_price_point_time,
        tr.first_trade_time,
        tr.last_trade_time
      from price_history ph
      left join trades tr on tr.source_market_id = ph.source_market_id
      order by ph.market_slug asc
    `,
    filters.params,
  );

  return {
    priceHistoryCount: Number(priceCountResult.rows[0]?.count ?? 0),
    tradeCount: Number(tradeCountResult.rows[0]?.count ?? 0),
    markets: marketsResult.rows.map((row) => ({
      sourceMarketId: row.source_market_id,
      marketSlug: row.market_slug,
      marketType: row.market_type,
      priceHistoryCount: Number(row.price_history_count),
      tradeCount: Number(row.trade_count),
      firstPricePointTime: row.first_price_point_time?.toISOString() ?? null,
      lastPricePointTime: row.last_price_point_time?.toISOString() ?? null,
      firstTradeTime: row.first_trade_time?.toISOString() ?? null,
      lastTradeTime: row.last_trade_time?.toISOString() ?? null,
    })),
  };
}

async function loadPriceHistoryRows(
  pool: ReturnType<typeof createPgPool>,
  filters: SqlFragment,
  limit: number,
): Promise<Array<Record<string, unknown>>> {
  const result = await pool.query<{
    market_slug: string;
    market_type: string | null;
    token_id: string;
    outcome_name: string;
    point_time: Date;
    price: string;
    fidelity_minutes: number | null;
  }>(
    `
      select
        market_slug,
        market_type,
        token_id,
        outcome_name,
        point_time,
        price::text,
        fidelity_minutes
      from raw_polymarket_price_history
      where ${filters.whereClause}
      order by point_time desc, token_id asc
      limit $${filters.params.length + 1}
    `,
    [...filters.params, limit],
  );

  return result.rows.map((row) => ({
    marketSlug: row.market_slug,
    marketType: row.market_type,
    tokenId: row.token_id,
    outcomeName: row.outcome_name,
    pointTime: row.point_time.toISOString(),
    price: Number(row.price),
    fidelityMinutes: row.fidelity_minutes,
  }));
}

async function loadTradeRows(
  pool: ReturnType<typeof createPgPool>,
  filters: SqlFragment,
  limit: number,
): Promise<Array<Record<string, unknown>>> {
  const result = await pool.query<{
    market_slug: string;
    market_type: string | null;
    token_id: string;
    outcome_name: string;
    trade_time: Date;
    price: string;
    size: string;
    side: "BUY" | "SELL";
    transaction_hash: string | null;
    proxy_wallet: string | null;
  }>(
    `
      select
        market_slug,
        market_type,
        token_id,
        outcome_name,
        trade_time,
        price::text,
        size::text,
        side,
        transaction_hash,
        proxy_wallet
      from raw_polymarket_trades
      where ${filters.whereClause}
      order by trade_time desc, token_id asc
      limit $${filters.params.length + 1}
    `,
    [...filters.params, limit],
  );

  return result.rows.map((row) => ({
    marketSlug: row.market_slug,
    marketType: row.market_type,
    tokenId: row.token_id,
    outcomeName: row.outcome_name,
    tradeTime: row.trade_time.toISOString(),
    price: Number(row.price),
    size: Number(row.size),
    side: row.side,
    transactionHash: row.transaction_hash,
    proxyWallet: row.proxy_wallet,
  }));
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
