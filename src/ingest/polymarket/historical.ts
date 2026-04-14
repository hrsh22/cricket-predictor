import { loadAppConfig } from "../../config/index.js";
import { parseJsonObject, type JsonObject } from "../../domain/primitives.js";
import {
  createRepositorySet,
  type RawPolymarketPriceHistoryInsert,
  type RawPolymarketPriceHistoryRecord,
  type RawPolymarketTradeInsert,
  type RawPolymarketTradeRecord,
  type RawSnapshotRepository,
  type RepositorySet,
} from "../../repositories/index.js";
import type { PolymarketFetch, PolymarketResponseLike } from "./ingest.js";

const DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com";
const DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com";
const DEFAULT_DATA_API_BASE_URL = "https://data-api.polymarket.com";
const DEFAULT_FIDELITY_MINUTES = 60;
const DEFAULT_TRADE_PAGE_SIZE = 1000;
const MAX_TRADE_PAGE_SIZE = 1000;
const MAX_TRADE_OFFSET = 3000;

export interface PolymarketHistoricalBackfillOptions {
  eventSlug: string;
  rawRepository: Pick<
    RawSnapshotRepository,
    "savePolymarketPriceHistoryPoint" | "savePolymarketTrade"
  >;
  dryRun?: boolean;
  fetchImpl?: PolymarketFetch;
  gammaBaseUrl?: string;
  clobBaseUrl?: string;
  dataApiBaseUrl?: string;
  marketTypes?: readonly string[];
  startTs?: number;
  endTs?: number;
  fidelityMinutes?: number;
  tradePageSize?: number;
}

export interface PolymarketHistoricalBackfillCommandOptions {
  eventSlug: string;
  repositories?: RepositorySet;
  dryRun?: boolean;
  marketTypes?: readonly string[];
  startTs?: number;
  endTs?: number;
  fidelityMinutes?: number;
  tradePageSize?: number;
}

export interface PolymarketHistoricalBackfillSummary {
  eventSlug: string;
  sourceEventId: string;
  selectedMarketCount: number;
  selectedTokenCount: number;
  fetchedTradeCount: number;
  persistedTradeCount: number;
  fetchedPricePointCount: number;
  persistedPricePointCount: number;
  queryWindow: {
    startTs: number;
    endTs: number;
    fidelityMinutes: number;
  };
  markets: Array<{
    sourceMarketId: string;
    marketSlug: string;
    conditionId: string;
    marketType: string | null;
    outcomes: string[];
    tokenIds: string[];
  }>;
  tradeWarnings: string[];
  priceHistoryRecords: RawPolymarketPriceHistoryRecord[];
  tradeRecords: RawPolymarketTradeRecord[];
}

interface GammaEventRecord {
  id: string;
  slug: string;
  startDate: string | null;
  endDate: string | null;
  markets: GammaMarketRecord[];
}

interface GammaMarketRecord {
  id: string;
  slug: string;
  conditionId: string;
  marketType: string | null;
  outcomes: readonly [string, string];
  tokenIds: readonly [string, string];
}

interface TokenDescriptor {
  sourceEventId: string;
  sourceMarketId: string;
  eventSlug: string;
  marketSlug: string;
  conditionId: string;
  marketType: string | null;
  outcomeName: string;
  outcomeIndex: 0 | 1;
  tokenId: string;
}

interface PriceHistoryPoint {
  t: number;
  p: number;
}

interface DataApiTrade {
  proxyWallet: string | null;
  side: "BUY" | "SELL";
  asset: string;
  conditionId: string;
  size: number;
  price: number;
  timestamp: number;
  outcome: string;
  outcomeIndex: number;
  transactionHash: string | null;
  raw: Record<string, unknown>;
}

interface TradeFetchResult {
  trades: DataApiTrade[];
  warnings: string[];
}

export async function backfillPolymarketEventHistoricalOdds(
  options: PolymarketHistoricalBackfillOptions,
): Promise<PolymarketHistoricalBackfillSummary> {
  const fetchImpl =
    options.fetchImpl ??
    ((input: string, init?: RequestInit) =>
      fetch(input, init) as Promise<PolymarketResponseLike>);
  const gammaBaseUrl = options.gammaBaseUrl ?? DEFAULT_GAMMA_BASE_URL;
  const clobBaseUrl = options.clobBaseUrl ?? DEFAULT_CLOB_BASE_URL;
  const dataApiBaseUrl = options.dataApiBaseUrl ?? DEFAULT_DATA_API_BASE_URL;
  const fidelityMinutes = options.fidelityMinutes ?? DEFAULT_FIDELITY_MINUTES;
  const tradePageSize = options.tradePageSize ?? DEFAULT_TRADE_PAGE_SIZE;

  if (
    !Number.isInteger(tradePageSize) ||
    tradePageSize <= 0 ||
    tradePageSize > MAX_TRADE_PAGE_SIZE
  ) {
    throw new Error(
      `tradePageSize must be a positive integer no greater than ${MAX_TRADE_PAGE_SIZE}.`,
    );
  }

  const event = await fetchGammaEventBySlug(
    fetchImpl,
    gammaBaseUrl,
    options.eventSlug,
  );
  const selectedMarkets = filterMarkets(event.markets, options.marketTypes);

  if (selectedMarkets.length === 0) {
    throw new Error(
      `No Polymarket markets matched event slug "${options.eventSlug}" and the requested market types.`,
    );
  }

  const tokenDescriptors = selectedMarkets.flatMap((market) =>
    market.tokenIds.map((tokenId, index) => ({
      sourceEventId: event.id,
      sourceMarketId: market.id,
      eventSlug: event.slug,
      marketSlug: market.slug,
      conditionId: market.conditionId,
      marketType: market.marketType,
      outcomeName: market.outcomes[index] ?? market.outcomes[0],
      outcomeIndex: (index === 0 ? 0 : 1) as 0 | 1,
      tokenId,
    })),
  );
  const queryWindow = resolveQueryWindow({
    event,
    startTs: options.startTs,
    endTs: options.endTs,
    fidelityMinutes,
  });

  const priceHistoryRecords: RawPolymarketPriceHistoryRecord[] = [];
  const histories = await Promise.all(
    tokenDescriptors.map(async (token) => ({
      token,
      history: await fetchPriceHistory(fetchImpl, clobBaseUrl, token.tokenId, {
        startTs: queryWindow.startTs,
        endTs: queryWindow.endTs,
        fidelityMinutes: queryWindow.fidelityMinutes,
      }),
    })),
  );

  for (const history of histories) {
    for (const point of history.history) {
      const insert = toPriceHistoryInsert(history.token, point, queryWindow);
      priceHistoryRecords.push(
        options.dryRun
          ? toDryRunPriceHistoryRecord(insert)
          : await options.rawRepository.savePolymarketPriceHistoryPoint(insert),
      );
    }
  }

  const tradeFetch = await fetchAllTrades(fetchImpl, dataApiBaseUrl, {
    conditionIds: selectedMarkets.map((market) => market.conditionId),
    limit: tradePageSize,
  });
  const tokensById = new Map(
    tokenDescriptors.map((token) => [token.tokenId, token]),
  );
  const tradeRecords: RawPolymarketTradeRecord[] = [];

  for (const trade of tradeFetch.trades) {
    const token = tokensById.get(trade.asset);
    if (token === undefined) {
      continue;
    }

    const insert = toTradeInsert(token, trade);
    tradeRecords.push(
      options.dryRun
        ? toDryRunTradeRecord(insert)
        : await options.rawRepository.savePolymarketTrade(insert),
    );
  }

  return {
    eventSlug: event.slug,
    sourceEventId: event.id,
    selectedMarketCount: selectedMarkets.length,
    selectedTokenCount: tokenDescriptors.length,
    tradeWarnings: tradeFetch.warnings,
    fetchedTradeCount: tradeFetch.trades.length,
    persistedTradeCount: tradeRecords.length,
    fetchedPricePointCount: histories.reduce(
      (total, current) => total + current.history.length,
      0,
    ),
    persistedPricePointCount: priceHistoryRecords.length,
    queryWindow,
    markets: selectedMarkets.map((market) => ({
      sourceMarketId: market.id,
      marketSlug: market.slug,
      conditionId: market.conditionId,
      marketType: market.marketType,
      outcomes: [...market.outcomes],
      tokenIds: [...market.tokenIds],
    })),
    priceHistoryRecords,
    tradeRecords,
  };
}

export async function runPolymarketHistoricalBackfill(
  options: PolymarketHistoricalBackfillCommandOptions,
): Promise<PolymarketHistoricalBackfillSummary> {
  if (options.dryRun === true && options.repositories === undefined) {
    return backfillPolymarketEventHistoricalOdds({
      eventSlug: options.eventSlug,
      rawRepository: createDryRunRawRepository(),
      dryRun: true,
      ...(options.marketTypes === undefined
        ? {}
        : { marketTypes: options.marketTypes }),
      ...(options.startTs === undefined ? {} : { startTs: options.startTs }),
      ...(options.endTs === undefined ? {} : { endTs: options.endTs }),
      ...(options.fidelityMinutes === undefined
        ? {}
        : { fidelityMinutes: options.fidelityMinutes }),
      ...(options.tradePageSize === undefined
        ? {}
        : { tradePageSize: options.tradePageSize }),
    });
  }

  const repositories =
    options.repositories ?? createRepositorySet(loadAppConfig());

  try {
    return await backfillPolymarketEventHistoricalOdds({
      eventSlug: options.eventSlug,
      rawRepository: repositories.raw,
      ...(options.dryRun === undefined ? {} : { dryRun: options.dryRun }),
      ...(options.marketTypes === undefined
        ? {}
        : { marketTypes: options.marketTypes }),
      ...(options.startTs === undefined ? {} : { startTs: options.startTs }),
      ...(options.endTs === undefined ? {} : { endTs: options.endTs }),
      ...(options.fidelityMinutes === undefined
        ? {}
        : { fidelityMinutes: options.fidelityMinutes }),
      ...(options.tradePageSize === undefined
        ? {}
        : { tradePageSize: options.tradePageSize }),
    });
  } finally {
    if (options.repositories === undefined) {
      await repositories.close();
    }
  }
}

export function createTradeKey(
  trade: Pick<
    DataApiTrade,
    | "asset"
    | "timestamp"
    | "price"
    | "size"
    | "side"
    | "transactionHash"
    | "proxyWallet"
  >,
): string {
  return [
    trade.asset,
    trade.transactionHash ?? "no-tx",
    trade.timestamp,
    trade.side,
    trade.price,
    trade.size,
    trade.proxyWallet ?? "no-wallet",
  ].join(":");
}

async function fetchGammaEventBySlug(
  fetchImpl: PolymarketFetch,
  baseUrl: string,
  eventSlug: string,
): Promise<GammaEventRecord> {
  const url = new URL("/events", baseUrl);
  url.searchParams.set("slug", eventSlug);
  const payload = await fetchJson(fetchImpl, url.toString());

  if (!Array.isArray(payload) || payload.length === 0) {
    throw new Error(`No Polymarket event found for slug "${eventSlug}".`);
  }

  const value = payload[0];
  if (typeof value !== "object" || value === null) {
    throw new Error(
      `Malformed Polymarket event payload for slug "${eventSlug}".`,
    );
  }

  const record = value as Record<string, unknown>;
  const markets = record["markets"];
  if (!Array.isArray(markets)) {
    throw new Error(
      `Malformed Polymarket event payload for slug "${eventSlug}".`,
    );
  }

  return {
    id: readString(record["id"], "event.id"),
    slug: readString(record["slug"], "event.slug"),
    startDate: readNullableString(record["startDate"]),
    endDate: readNullableString(record["endDate"]),
    markets: markets.map(parseGammaMarket),
  };
}

function parseGammaMarket(value: unknown): GammaMarketRecord {
  if (typeof value !== "object" || value === null) {
    throw new Error("Malformed Polymarket market payload.");
  }

  const record = value as Record<string, unknown>;
  return {
    id: readString(record["id"], "market.id"),
    slug: readString(record["slug"], "market.slug"),
    conditionId: readString(record["conditionId"], "market.conditionId"),
    marketType: readNullableString(record["sportsMarketType"]),
    outcomes: readStringPair(record["outcomes"], "market.outcomes"),
    tokenIds: readStringPair(record["clobTokenIds"], "market.clobTokenIds"),
  };
}

function filterMarkets(
  markets: readonly GammaMarketRecord[],
  marketTypes: readonly string[] | undefined,
): GammaMarketRecord[] {
  if (marketTypes === undefined || marketTypes.length === 0) {
    return [...markets];
  }

  const allowed = new Set(
    marketTypes.map((value) => value.trim().toLowerCase()),
  );
  return markets.filter((market) => {
    const marketType = market.marketType;
    if (marketType !== null) {
      return allowed.has(marketType.toLowerCase());
    }

    return (
      allowed.has("moneyline") &&
      market.outcomes.length === 2 &&
      market.tokenIds.length === 2
    );
  });
}

function resolveQueryWindow(options: {
  event: Pick<GammaEventRecord, "startDate" | "endDate">;
  startTs: number | undefined;
  endTs: number | undefined;
  fidelityMinutes: number;
}): { startTs: number; endTs: number; fidelityMinutes: number } {
  const nowTs = Math.floor(Date.now() / 1000);
  const fallbackStartTs =
    options.event.startDate === null
      ? nowTs - 14 * 24 * 60 * 60
      : Math.floor(Date.parse(options.event.startDate) / 1000);
  const fallbackEndTs =
    options.event.endDate === null
      ? nowTs
      : Math.floor(Date.parse(options.event.endDate) / 1000) + 12 * 60 * 60;
  const startTs = options.startTs ?? fallbackStartTs;
  const endTs = options.endTs ?? Math.min(nowTs, fallbackEndTs);

  if (!Number.isInteger(startTs) || !Number.isInteger(endTs)) {
    throw new Error("startTs and endTs must be integer unix timestamps.");
  }

  if (startTs >= endTs) {
    throw new Error("startTs must be before endTs.");
  }

  if (
    !Number.isInteger(options.fidelityMinutes) ||
    options.fidelityMinutes <= 0
  ) {
    throw new Error("fidelityMinutes must be a positive integer.");
  }

  return {
    startTs,
    endTs,
    fidelityMinutes: options.fidelityMinutes,
  };
}

async function fetchPriceHistory(
  fetchImpl: PolymarketFetch,
  baseUrl: string,
  tokenId: string,
  options: { startTs: number; endTs: number; fidelityMinutes: number },
): Promise<PriceHistoryPoint[]> {
  const url = new URL("/prices-history", baseUrl);
  url.searchParams.set("market", tokenId);
  url.searchParams.set("startTs", String(options.startTs));
  url.searchParams.set("endTs", String(options.endTs));
  url.searchParams.set("fidelity", String(options.fidelityMinutes));
  const payload = await fetchJson(fetchImpl, url.toString());

  if (typeof payload !== "object" || payload === null) {
    throw new Error(`Malformed price history payload for token ${tokenId}.`);
  }

  const historyValue = (payload as Record<string, unknown>)["history"];
  if (!Array.isArray(historyValue)) {
    throw new Error(`Malformed price history payload for token ${tokenId}.`);
  }

  return historyValue.map((entry) => parsePriceHistoryPoint(entry, tokenId));
}

function parsePriceHistoryPoint(
  value: unknown,
  tokenId: string,
): PriceHistoryPoint {
  if (typeof value !== "object" || value === null) {
    throw new Error(`Malformed price history point for token ${tokenId}.`);
  }

  const record = value as Record<string, unknown>;
  return {
    t: Math.trunc(readFiniteNumber(record["t"], `priceHistory.${tokenId}.t`)),
    p: readFiniteNumber(record["p"], `priceHistory.${tokenId}.p`),
  };
}

async function fetchAllTrades(
  fetchImpl: PolymarketFetch,
  baseUrl: string,
  options: { conditionIds: readonly string[]; limit: number },
): Promise<TradeFetchResult> {
  const trades = new Map<string, DataApiTrade>();
  const warnings: string[] = [];

  for (const conditionId of options.conditionIds) {
    for (const side of ["BUY", "SELL"] as const) {
      let offset = 0;

      while (true) {
        const url = new URL("/trades", baseUrl);
        url.searchParams.set("market", conditionId);
        url.searchParams.set("side", side);
        url.searchParams.set("limit", String(options.limit));
        url.searchParams.set("offset", String(offset));
        const payload = await fetchJson(fetchImpl, url.toString());
        if (!Array.isArray(payload)) {
          throw new Error(
            `Malformed trades payload for market ${conditionId} (${side}).`,
          );
        }

        const page = payload.map(parseDataApiTrade);
        for (const trade of page) {
          trades.set(createTradeKey(trade), trade);
        }

        if (page.length < options.limit) {
          break;
        }

        offset += options.limit;
        if (offset > MAX_TRADE_OFFSET) {
          warnings.push(
            `Public Polymarket trade history was truncated for market ${conditionId} (${side}) after the accessible offset window. Exact trade coverage is partial; use a richer trade archive for full reconstruction.`,
          );
          break;
        }
      }
    }
  }

  return {
    trades: Array.from(trades.values()).sort(
      (left, right) => right.timestamp - left.timestamp,
    ),
    warnings,
  };
}

function parseDataApiTrade(value: unknown): DataApiTrade {
  if (typeof value !== "object" || value === null) {
    throw new Error("Malformed Polymarket trade payload.");
  }

  const record = value as Record<string, unknown>;
  const side = readString(record["side"], "trade.side");
  if (side !== "BUY" && side !== "SELL") {
    throw new Error(`Unsupported trade side "${side}".`);
  }

  return {
    proxyWallet: readNullableString(record["proxyWallet"]),
    side,
    asset: readString(record["asset"], "trade.asset"),
    conditionId: readString(record["conditionId"], "trade.conditionId"),
    size: readFiniteNumber(record["size"], "trade.size"),
    price: readFiniteNumber(record["price"], "trade.price"),
    timestamp: Math.trunc(
      readFiniteNumber(record["timestamp"], "trade.timestamp"),
    ),
    outcome: readString(record["outcome"], "trade.outcome"),
    outcomeIndex: Math.trunc(
      readFiniteNumber(record["outcomeIndex"], "trade.outcomeIndex"),
    ),
    transactionHash: readNullableString(record["transactionHash"]),
    raw: record,
  };
}

function toPriceHistoryInsert(
  token: TokenDescriptor,
  point: PriceHistoryPoint,
  queryWindow: { startTs: number; endTs: number; fidelityMinutes: number },
): RawPolymarketPriceHistoryInsert {
  const pointPayload: JsonObject = {
    t: point.t,
    p: point.p,
  };
  const queryWindowPayload: JsonObject = {
    startTs: queryWindow.startTs,
    endTs: queryWindow.endTs,
    fidelityMinutes: queryWindow.fidelityMinutes,
  };

  return {
    sourceEventId: token.sourceEventId,
    sourceMarketId: token.sourceMarketId,
    eventSlug: token.eventSlug,
    marketSlug: token.marketSlug,
    conditionId: token.conditionId,
    marketType: token.marketType,
    tokenId: token.tokenId,
    outcomeName: token.outcomeName,
    outcomeIndex: token.outcomeIndex,
    pointTime: new Date(point.t * 1000).toISOString(),
    price: point.p,
    queryStartTime: new Date(queryWindow.startTs * 1000).toISOString(),
    queryEndTime: new Date(queryWindow.endTs * 1000).toISOString(),
    fidelityMinutes: queryWindow.fidelityMinutes,
    payload: {
      source: "clob-prices-history",
      tokenId: token.tokenId,
      point: pointPayload,
      queryWindow: queryWindowPayload,
    },
  };
}

function toTradeInsert(
  token: TokenDescriptor,
  trade: DataApiTrade,
): RawPolymarketTradeInsert {
  return {
    tradeKey: createTradeKey(trade),
    sourceEventId: token.sourceEventId,
    sourceMarketId: token.sourceMarketId,
    eventSlug: token.eventSlug,
    marketSlug: token.marketSlug,
    conditionId: token.conditionId,
    marketType: token.marketType,
    tokenId: token.tokenId,
    outcomeName: token.outcomeName,
    outcomeIndex: token.outcomeIndex,
    tradeTime: new Date(trade.timestamp * 1000).toISOString(),
    price: trade.price,
    size: trade.size,
    side: trade.side,
    transactionHash: trade.transactionHash,
    proxyWallet: trade.proxyWallet,
    payload: {
      source: "data-api-trades",
      trade: toJsonObject(trade.raw, "trade.raw"),
    },
  };
}

function toDryRunPriceHistoryRecord(
  point: RawPolymarketPriceHistoryInsert,
): RawPolymarketPriceHistoryRecord {
  return {
    id: 0,
    competition: "IPL",
    createdAt: point.pointTime,
    ...point,
  };
}

function toDryRunTradeRecord(
  trade: RawPolymarketTradeInsert,
): RawPolymarketTradeRecord {
  return {
    id: 0,
    competition: "IPL",
    createdAt: trade.tradeTime,
    ...trade,
  };
}

function createDryRunRawRepository(): Pick<
  RawSnapshotRepository,
  "savePolymarketPriceHistoryPoint" | "savePolymarketTrade"
> {
  return {
    async savePolymarketPriceHistoryPoint(
      point: RawPolymarketPriceHistoryInsert,
    ): Promise<RawPolymarketPriceHistoryRecord> {
      return toDryRunPriceHistoryRecord(point);
    },
    async savePolymarketTrade(
      trade: RawPolymarketTradeInsert,
    ): Promise<RawPolymarketTradeRecord> {
      return toDryRunTradeRecord(trade);
    },
  };
}

async function fetchJson(
  fetchImpl: PolymarketFetch,
  url: string,
  init: RequestInit | undefined = undefined,
): Promise<unknown> {
  const response = await fetchImpl(url, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Polymarket request failed (${response.status}) for ${url}: ${body}`,
    );
  }

  return response.json();
}

function readString(value: unknown, path: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Expected ${path} to be a non-empty string.`);
  }

  return value;
}

function readNullableString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  return typeof value === "string" && value.trim().length > 0 ? value : null;
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

function readStringPair(
  value: unknown,
  path: string,
): readonly [string, string] {
  const values = parseStringArrayValue(value, path);
  if (values.length !== 2) {
    throw new Error(`Expected ${path} to contain exactly two string values.`);
  }

  return [values[0] as string, values[1] as string];
}

function parseStringArrayValue(value: unknown, path: string): string[] {
  if (Array.isArray(value)) {
    return value.map((entry, index) => readString(entry, `${path}[${index}]`));
  }

  if (typeof value === "string") {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) {
      throw new Error(`Expected ${path} to decode to an array.`);
    }

    return parsed.map((entry, index) => readString(entry, `${path}[${index}]`));
  }

  throw new Error(`Expected ${path} to be an array or JSON-encoded array.`);
}

function toJsonObject(
  value: Record<string, unknown>,
  path: string,
): JsonObject {
  const parsed = parseJsonObject(value, path, []);
  if (parsed === null) {
    throw new Error(`Expected ${path} to be a JSON object.`);
  }

  return parsed;
}
