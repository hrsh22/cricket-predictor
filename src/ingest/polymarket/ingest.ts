import { loadAppConfig } from "../../config/index.js";
import type { MarketSnapshot } from "../../domain/market.js";
import {
  parseJsonObject,
  type JsonObject,
  type ValidationIssue,
} from "../../domain/primitives.js";
import {
  createRepositorySet,
  type RawMarketSnapshotRecord,
  type RawSnapshotRepository,
  type RepositorySet,
} from "../../repositories/index.js";
import {
  normalizePolymarketIplWinnerMarket,
  type PolymarketGammaMarketQuery,
  type PolymarketGammaReadClient,
  type PolymarketIplWinnerMarketDiscovery,
} from "./client.js";

const DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com";
const DEFAULT_IPL_TAG_ID = "101988";
const DEFAULT_PAGE_SIZE = 100;

export interface PolymarketRetryPolicy {
  maxRetries: number;
  initialDelayMs: number;
  backoffMultiplier: number;
  maxDelayMs: number;
}

export interface PolymarketResponseLike {
  ok: boolean;
  status: number;
  headers: {
    get(name: string): string | null;
  };
  json(): Promise<unknown>;
  text(): Promise<string>;
}

export type PolymarketFetch = (
  input: string,
  init?: RequestInit,
) => Promise<PolymarketResponseLike>;

export interface PolymarketGammaHttpClientOptions {
  baseUrl?: string;
  iplTagId?: string;
  pageSize?: number;
  retryPolicy?: Partial<PolymarketRetryPolicy>;
  fetchImpl?: PolymarketFetch;
  sleep?: (milliseconds: number) => Promise<void>;
}

export interface PolymarketIngestionOptions {
  gammaClient?: PolymarketGammaReadClient;
  rawRepository: RawSnapshotRepository;
  fetchedAt?: string;
  trigger?: "manual" | "scheduled";
  endpoint?: string;
  query?: PolymarketGammaMarketQuery;
}

export interface PolymarketIngestionSummary {
  trigger: "manual" | "scheduled";
  fetchedAt: string;
  fetchedCount: number;
  candidateCount: number;
  skippedCount: number;
  duplicateCount: number;
  persistedCount: number;
  endpoint: string;
  snapshots: RawMarketSnapshotRecord[];
}

export interface PolymarketIngestionCommandOptions {
  trigger?: "manual" | "scheduled";
  gammaClient?: PolymarketGammaReadClient;
  repositories?: RepositorySet;
  fetchedAt?: string;
}

const defaultRetryPolicy: PolymarketRetryPolicy = {
  maxRetries: 3,
  initialDelayMs: 250,
  backoffMultiplier: 2,
  maxDelayMs: 2_000,
};

export function createPolymarketGammaReadClient(
  options: PolymarketGammaHttpClientOptions = {},
): PolymarketGammaReadClient {
  const baseUrl = options.baseUrl ?? DEFAULT_GAMMA_BASE_URL;
  const iplTagId = options.iplTagId ?? DEFAULT_IPL_TAG_ID;
  const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
  const retryPolicy = {
    ...defaultRetryPolicy,
    ...options.retryPolicy,
  };
  const fetchImpl =
    options.fetchImpl ??
    ((input: string, init?: RequestInit) =>
      fetch(input, init) as Promise<PolymarketResponseLike>);
  const sleep = options.sleep ?? defaultSleep;

  return {
    async listMarkets(
      query: PolymarketGammaMarketQuery = {},
    ): Promise<readonly unknown[]> {
      const allMarkets: unknown[] = [];
      let offset = 0;

      while (true) {
        const url = buildGammaMarketsUrl({
          baseUrl,
          query,
          offset,
          limit: pageSize,
          iplTagId,
        });

        const page = await fetchJsonArrayWithRetry(url, {
          fetchImpl,
          retryPolicy,
          sleep,
        });

        allMarkets.push(...page);

        if (page.length < pageSize) {
          break;
        }

        offset += pageSize;
      }

      return allMarkets;
    },
  };
}

export async function ingestPolymarketIplWinnerMarkets(
  options: PolymarketIngestionOptions,
): Promise<PolymarketIngestionSummary> {
  const gammaClient = options.gammaClient ?? createPolymarketGammaReadClient();
  const trigger = options.trigger ?? "manual";
  const fetchedAt = normalizeIsoTimestamp(
    options.fetchedAt ?? new Date().toISOString(),
  );
  const endpoint = options.endpoint ?? `${DEFAULT_GAMMA_BASE_URL}/markets`;
  const query: PolymarketGammaMarketQuery = options.query ?? {
    tag: "ipl",
    active: true,
    closed: false,
    archived: false,
  };

  const fetchedMarkets = await gammaClient.listMarkets(query);
  const dedupedSnapshots = new Map<string, MarketSnapshot>();
  let candidateCount = 0;
  let skippedCount = 0;

  for (const market of fetchedMarkets) {
    const discovery = toPolymarketIplWinnerDiscovery(market, fetchedAt);

    if (discovery === null) {
      skippedCount += 1;
      continue;
    }

    candidateCount += 1;

    const normalizedSnapshot = normalizePolymarketIplWinnerMarket(discovery);
    const dedupeKey = createSnapshotDedupeKey(normalizedSnapshot);

    dedupedSnapshots.set(
      dedupeKey,
      withFetchMetadata(normalizedSnapshot, {
        dedupeKey,
        fetchedAt,
        trigger,
        endpoint,
      }),
    );
  }

  const snapshots: RawMarketSnapshotRecord[] = [];

  for (const snapshot of dedupedSnapshots.values()) {
    const savedSnapshot =
      await options.rawRepository.saveMarketSnapshot(snapshot);
    snapshots.push(savedSnapshot);
  }

  return {
    trigger,
    fetchedAt,
    fetchedCount: fetchedMarkets.length,
    candidateCount,
    skippedCount,
    duplicateCount: candidateCount - dedupedSnapshots.size,
    persistedCount: snapshots.length,
    endpoint,
    snapshots,
  };
}

export async function runPolymarketIplWinnerIngestion(
  options: PolymarketIngestionCommandOptions = {},
): Promise<PolymarketIngestionSummary> {
  const repositories =
    options.repositories ?? createRepositorySet(loadAppConfig());

  try {
    return await ingestPolymarketIplWinnerMarkets({
      rawRepository: repositories.raw,
      ...(options.gammaClient === undefined
        ? {}
        : { gammaClient: options.gammaClient }),
      ...(options.fetchedAt === undefined
        ? {}
        : { fetchedAt: options.fetchedAt }),
      ...(options.trigger === undefined ? {} : { trigger: options.trigger }),
    });
  } finally {
    if (options.repositories === undefined) {
      await repositories.close();
    }
  }
}

export function createSnapshotDedupeKey(
  snapshot: Pick<MarketSnapshot, "sourceMarketId" | "snapshotTime">,
): string {
  return `${snapshot.sourceMarketId}:${snapshot.snapshotTime}`;
}

export function toPolymarketIplWinnerDiscovery(
  value: unknown,
  fetchedAt: string,
): PolymarketIplWinnerMarketDiscovery | null {
  const rawPayloadIssues: ValidationIssue[] = [];
  const rawPayload = parseJsonObject(
    value,
    "polymarket.live_market",
    rawPayloadIssues,
  );

  if (rawPayload === null || rawPayloadIssues.length > 0) {
    return null;
  }

  const market = toRecord(value);
  if (market === null) {
    return null;
  }

  const sportsMarketType = readNullableString(market["sportsMarketType"]);
  if (sportsMarketType !== null && sportsMarketType !== "moneyline") {
    return null;
  }

  const eventRecord = readFirstEventRecord(market["events"]);
  const seriesSlug =
    eventRecord === null ? null : readNullableString(eventRecord["seriesSlug"]);
  const eventTitle =
    eventRecord === null ? null : readNullableString(eventRecord["title"]);

  if (!looksLikeIplEvent(seriesSlug, eventTitle)) {
    return null;
  }

  const id = readString(market["id"]);
  const slug = readString(market["slug"]);
  const question = readString(market["question"]);
  const active = readBoolean(market["active"]);
  const closed = readBoolean(market["closed"]);
  const archived = readBoolean(market["archived"]);
  const endDateIso = readString(market["endDate"]);
  const updatedAt = readNullableString(market["updatedAt"]);
  const eventSlug =
    eventRecord === null
      ? slug
      : (readNullableString(eventRecord["slug"]) ?? slug);
  const outcomes = readStringPair(market["outcomes"]);
  const outcomePrices = readNumberPair(market["outcomePrices"]);
  const outcomeTokenIds = readStringPair(market["clobTokenIds"]);
  const liquidity =
    readNullableNumber(market["liquidityClob"]) ??
    readNullableNumber(market["liquidityNum"]) ??
    readNullableNumber(market["liquidity"]);
  const lastTradePrice = readNullableNumber(market["lastTradePrice"]);

  if (
    id === null ||
    slug === null ||
    question === null ||
    active === null ||
    closed === null ||
    archived === null ||
    endDateIso === null ||
    eventSlug === null ||
    outcomes === null ||
    outcomePrices === null ||
    outcomeTokenIds === null
  ) {
    return null;
  }

  const snapshotTime = normalizeIsoTimestamp(updatedAt ?? fetchedAt);
  const tags = buildIplTags(seriesSlug, eventTitle);
  const clobRaw: JsonObject = {
    source: "gamma-market-derived",
    marketId: id,
    lastTradePrice,
    liquidity,
    updatedAt,
  };

  return {
    snapshotTime,
    gamma: {
      id,
      slug,
      event_slug: eventSlug,
      question,
      market_type: "binary",
      sports_market_type: sportsMarketType ?? "moneyline",
      active,
      closed,
      archived,
      end_date_iso: endDateIso,
      updated_at: updatedAt,
      outcomes,
      outcome_token_ids: outcomeTokenIds,
      outcome_prices: outcomePrices,
      liquidity,
      tags,
      raw: rawPayload,
    },
    clob: {
      market_id: id,
      liquidity,
      last_traded_price: lastTradePrice,
      updated_at: updatedAt,
      raw: clobRaw,
    },
  };
}

function withFetchMetadata(
  snapshot: MarketSnapshot,
  context: {
    dedupeKey: string;
    fetchedAt: string;
    trigger: "manual" | "scheduled";
    endpoint: string;
  },
): MarketSnapshot {
  return {
    ...snapshot,
    payload: {
      ...snapshot.payload,
      dedupeKey: context.dedupeKey,
      fetchMetadata: {
        fetchedAt: context.fetchedAt,
        trigger: context.trigger,
        endpoint: context.endpoint,
      },
    },
  };
}

function buildGammaMarketsUrl(options: {
  baseUrl: string;
  query: PolymarketGammaMarketQuery;
  offset: number;
  limit: number;
  iplTagId: string;
}): string {
  const url = new URL("/markets", options.baseUrl);
  url.searchParams.set("limit", String(options.limit));
  url.searchParams.set("offset", String(options.offset));

  if (options.query.active !== undefined) {
    url.searchParams.set("active", String(options.query.active));
  }

  if (options.query.closed !== undefined) {
    url.searchParams.set("closed", String(options.query.closed));
  }

  if (options.query.archived !== undefined) {
    url.searchParams.set("archived", String(options.query.archived));
  }

  const tagId = resolveGammaTagId(options.query.tag, options.iplTagId);
  if (tagId !== null) {
    url.searchParams.set("tag_id", tagId);
  }

  return url.toString();
}

function resolveGammaTagId(
  tag: string | undefined,
  fallbackIplTagId: string,
): string | null {
  if (tag === undefined) {
    return null;
  }

  if (/^\d+$/.test(tag)) {
    return tag;
  }

  const normalizedTag = tag.trim().toLowerCase();
  if (normalizedTag === "ipl" || normalizedTag === "cricipl") {
    return fallbackIplTagId;
  }

  return null;
}

async function fetchJsonArrayWithRetry(
  url: string,
  options: {
    fetchImpl: PolymarketFetch;
    retryPolicy: PolymarketRetryPolicy;
    sleep: (milliseconds: number) => Promise<void>;
  },
): Promise<readonly unknown[]> {
  let attempt = 0;

  while (true) {
    const response = await options.fetchImpl(url, {
      headers: {
        accept: "application/json",
      },
    });

    if (response.ok) {
      const json = await response.json();

      if (!Array.isArray(json)) {
        throw new Error(`Expected Polymarket list response array from ${url}.`);
      }

      return json;
    }

    if (
      !isRetryableStatus(response.status) ||
      attempt >= options.retryPolicy.maxRetries
    ) {
      const body = await response.text().catch(() => "");
      throw new Error(
        `Polymarket request failed with status ${response.status} for ${url}: ${body}`,
      );
    }

    const retryAfterHeader = response.headers.get("retry-after");
    const delayMs = computeRetryDelayMs({
      attempt,
      retryAfterHeader,
      retryPolicy: options.retryPolicy,
    });
    await options.sleep(delayMs);
    attempt += 1;
  }
}

function computeRetryDelayMs(options: {
  attempt: number;
  retryAfterHeader: string | null;
  retryPolicy: PolymarketRetryPolicy;
}): number {
  const retryAfterSeconds = Number(options.retryAfterHeader);
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    return retryAfterSeconds * 1_000;
  }

  const delay =
    options.retryPolicy.initialDelayMs *
    options.retryPolicy.backoffMultiplier ** options.attempt;

  return Math.min(delay, options.retryPolicy.maxDelayMs);
}

function isRetryableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

function buildIplTags(
  seriesSlug: string | null,
  eventTitle: string | null,
): readonly string[] {
  const tags = ["ipl", "cricket", "winner"];

  if (seriesSlug !== null) {
    tags.push(seriesSlug.toLowerCase());
  }

  if (eventTitle !== null) {
    tags.push(eventTitle.toLowerCase());
  }

  return tags;
}

function looksLikeIplEvent(
  seriesSlug: string | null,
  eventTitle: string | null,
): boolean {
  if (
    seriesSlug !== null &&
    seriesSlug.toLowerCase() === "indian-premier-league"
  ) {
    return true;
  }

  return (
    eventTitle !== null &&
    eventTitle.toLowerCase().includes("indian premier league")
  );
}

function normalizeIsoTimestamp(value: string): string {
  return new Date(value).toISOString();
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function readNullableString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function readNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function readStringPair(value: unknown): readonly [string, string] | null {
  const parsed = readStringArray(value);
  if (parsed === null || parsed.length !== 2) {
    return null;
  }

  const first = parsed[0];
  const second = parsed[1];

  if (first === undefined || second === undefined) {
    return null;
  }

  return [first, second];
}

function readNumberPair(value: unknown): readonly [number, number] | null {
  const parsed = readNumberArray(value);
  if (parsed === null || parsed.length !== 2) {
    return null;
  }

  const first = parsed[0];
  const second = parsed[1];

  if (first === undefined || second === undefined) {
    return null;
  }

  return [first, second];
}

function readStringArray(value: unknown): string[] | null {
  const parsed = parseArrayValue(value);
  if (parsed === null) {
    return null;
  }

  const strings = parsed.map((entry) => readString(entry));
  return strings.every((entry) => entry !== null)
    ? (strings as string[])
    : null;
}

function readNumberArray(value: unknown): number[] | null {
  const parsed = parseArrayValue(value);
  if (parsed === null) {
    return null;
  }

  const numbers = parsed.map((entry) => readNullableNumber(entry));
  return numbers.every((entry) => entry !== null)
    ? (numbers as number[])
    : null;
}

function parseArrayValue(value: unknown): unknown[] | null {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function readFirstEventRecord(value: unknown): Record<string, unknown> | null {
  if (!Array.isArray(value)) {
    return null;
  }

  return toRecord(value[0]);
}

function toRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

async function defaultSleep(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export const polymarketDefaults = {
  gammaBaseUrl: DEFAULT_GAMMA_BASE_URL,
  iplTagId: DEFAULT_IPL_TAG_ID,
  pageSize: DEFAULT_PAGE_SIZE,
  retryPolicy: defaultRetryPolicy,
};
