import {
  parseMarketSnapshot,
  type MarketSnapshot,
} from "../../domain/index.js";
import {
  collectUnknownKeys,
  DomainValidationError,
  parseJsonObject,
  parseNullableFiniteNumber,
  parseNullableString,
  parseNullableTimestamptzString,
  parseProbability,
  parseRecord,
  parseString,
  parseTimestamptzString,
  type JsonObject,
  type ValidationIssue,
} from "../../domain/primitives.js";

export interface PolymarketGammaMarketQuery {
  tag?: string;
  active?: boolean;
  closed?: boolean;
  archived?: boolean;
}

export interface PolymarketGammaReadClient {
  listMarkets(query?: PolymarketGammaMarketQuery): Promise<readonly unknown[]>;
}

export interface PolymarketClobReadClient {
  getMarket(tokenId: string): Promise<unknown>;
}

export interface PolymarketIplWinnerMarketDiscovery {
  gamma: unknown;
  clob: unknown;
  snapshotTime: string;
}

interface ParsedGammaMarket {
  id: string;
  slug: string;
  eventSlug: string;
  question: string;
  marketType: string;
  sportsMarketType: string | null;
  active: boolean;
  closed: boolean;
  archived: boolean;
  endDateIso: string;
  updatedAt: string | null;
  outcomes: readonly [string, string];
  outcomeTokenIds: readonly [string, string];
  outcomePrices: readonly [number, number];
  liquidity: number | null;
  tags: readonly string[];
  payload: JsonObject;
}

interface ParsedClobMarket {
  marketId: string;
  liquidity: number | null;
  lastTradedPrice: number | null;
  updatedAt: string | null;
  payload: JsonObject;
}

export function normalizePolymarketIplWinnerMarket(
  value: unknown,
): MarketSnapshot {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "polymarket", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(...collectUnknownKeys(record, ["gamma", "clob", "snapshotTime"]));

  const gamma = parseGammaMarket(record["gamma"], issues);
  const clob = parseClobMarket(record["clob"], issues);
  const snapshotTime = parseTimestamptzString(
    record["snapshotTime"],
    "polymarket.snapshotTime",
    issues,
  );

  if (
    gamma === null ||
    clob === null ||
    snapshotTime === null ||
    issues.length > 0
  ) {
    throw new DomainValidationError(issues);
  }

  if (gamma.marketType !== "binary") {
    issues.push({
      path: "polymarket.gamma.market_type",
      message: "must be a winner market",
    });
  }

  if (!hasIplTag(gamma.tags) || !looksLikeWinnerMarket(gamma)) {
    issues.push({ path: "polymarket.gamma", message: "unsupported market" });
  }

  if (!gamma.active || gamma.closed || gamma.archived) {
    issues.push({
      path: "polymarket.gamma",
      message: "stale market is not supported",
    });
  }

  if (Date.parse(gamma.endDateIso) < Date.parse(snapshotTime)) {
    issues.push({
      path: "polymarket.gamma.end_date_iso",
      message: "must not be before snapshotTime",
    });
  }

  if (gamma.id !== clob.marketId) {
    issues.push({
      path: "polymarket.clob.market_id",
      message: "must match gamma.id",
    });
  }

  if (issues.length > 0) {
    throw new DomainValidationError(issues);
  }

  return parseMarketSnapshot({
    competition: "IPL",
    sourceMarketId: gamma.id,
    marketSlug: gamma.slug,
    eventSlug: gamma.eventSlug,
    snapshotTime,
    marketStatus: "open",
    yesOutcomeName: gamma.outcomes[0],
    noOutcomeName: gamma.outcomes[1],
    outcomeProbabilities: {
      yes: gamma.outcomePrices[0],
      no: gamma.outcomePrices[1],
    },
    lastTradedPrice: clob.lastTradedPrice,
    liquidity: clob.liquidity ?? gamma.liquidity,
    payload: {
      gamma: gamma.payload,
      clob: clob.payload,
      tokenIds: {
        yes: gamma.outcomeTokenIds[0],
        no: gamma.outcomeTokenIds[1],
      },
      snapshotTime,
    },
  });
}

export function normalizePolymarketIplWinnerMarkets(
  values: readonly unknown[],
): MarketSnapshot[] {
  const normalized = values.map((value) =>
    normalizePolymarketIplWinnerMarket(value),
  );
  const seenMarketIds = new Set<string>();
  const seenEventSlugs = new Set<string>();
  const issues: ValidationIssue[] = [];

  for (const market of normalized) {
    if (seenMarketIds.has(market.sourceMarketId)) {
      issues.push({
        path: "polymarket.markets",
        message: `duplicate market id: ${market.sourceMarketId}`,
      });
    } else {
      seenMarketIds.add(market.sourceMarketId);
    }

    if (market.eventSlug !== null) {
      if (seenEventSlugs.has(market.eventSlug)) {
        issues.push({
          path: "polymarket.markets",
          message: `duplicate event slug: ${market.eventSlug}`,
        });
      } else {
        seenEventSlugs.add(market.eventSlug);
      }
    }
  }

  if (issues.length > 0) {
    throw new DomainValidationError(issues);
  }

  return normalized;
}

function parseGammaMarket(
  value: unknown,
  issues: ValidationIssue[],
): ParsedGammaMarket | null {
  const record = parseJsonObject(value, "polymarket.gamma", issues);

  if (record === null) {
    return null;
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "id",
      "slug",
      "event_slug",
      "question",
      "market_type",
      "sports_market_type",
      "active",
      "closed",
      "archived",
      "end_date_iso",
      "updated_at",
      "outcomes",
      "outcome_token_ids",
      "outcome_prices",
      "liquidity",
      "tags",
      "raw",
    ]),
  );

  const id = parseString(record["id"], "polymarket.gamma.id", issues);
  const slug = parseString(record["slug"], "polymarket.gamma.slug", issues);
  const eventSlug = parseString(
    record["event_slug"],
    "polymarket.gamma.event_slug",
    issues,
  );
  const question = parseString(
    record["question"],
    "polymarket.gamma.question",
    issues,
  );
  const marketType = parseString(
    record["market_type"],
    "polymarket.gamma.market_type",
    issues,
  );
  const sportsMarketType = parseNullableString(
    record["sports_market_type"] ?? null,
    "polymarket.gamma.sports_market_type",
    issues,
  );
  const active = parseBoolean(
    record["active"],
    "polymarket.gamma.active",
    issues,
  );
  const closed = parseBoolean(
    record["closed"],
    "polymarket.gamma.closed",
    issues,
  );
  const archived = parseBoolean(
    record["archived"],
    "polymarket.gamma.archived",
    issues,
  );
  const endDateIso = parseTimestamptzString(
    record["end_date_iso"],
    "polymarket.gamma.end_date_iso",
    issues,
  );
  const updatedAt = parseNullableTimestamptzString(
    record["updated_at"],
    "polymarket.gamma.updated_at",
    issues,
  );
  const outcomes = parseStringPair(
    record["outcomes"],
    "polymarket.gamma.outcomes",
    issues,
  );
  const outcomeTokenIds = parseStringPair(
    record["outcome_token_ids"],
    "polymarket.gamma.outcome_token_ids",
    issues,
  );
  const outcomePrices = parseNumberPair(
    record["outcome_prices"],
    "polymarket.gamma.outcome_prices",
    issues,
  );
  const liquidity = parseNullableFiniteNumber(
    record["liquidity"],
    "polymarket.gamma.liquidity",
    issues,
  );
  const tags = parseStringArray(
    record["tags"],
    "polymarket.gamma.tags",
    issues,
  );

  if (
    id === null ||
    slug === null ||
    eventSlug === null ||
    question === null ||
    marketType === null ||
    active === null ||
    closed === null ||
    archived === null ||
    endDateIso === null ||
    outcomes === null ||
    outcomeTokenIds === null ||
    outcomePrices === null ||
    tags === null
  ) {
    return null;
  }

  return {
    id,
    slug,
    eventSlug,
    question,
    marketType,
    sportsMarketType,
    active,
    closed,
    archived,
    endDateIso,
    updatedAt,
    outcomes,
    outcomeTokenIds,
    outcomePrices,
    liquidity,
    tags,
    payload: record,
  };
}

function parseClobMarket(
  value: unknown,
  issues: ValidationIssue[],
): ParsedClobMarket | null {
  const record = parseJsonObject(value, "polymarket.clob", issues);

  if (record === null) {
    return null;
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "market_id",
      "liquidity",
      "last_traded_price",
      "updated_at",
      "raw",
    ]),
  );

  const marketId = parseString(
    record["market_id"],
    "polymarket.clob.market_id",
    issues,
  );
  const liquidity = parseNullableFiniteNumber(
    record["liquidity"],
    "polymarket.clob.liquidity",
    issues,
  );
  const lastTradedPrice = parseNullableFiniteNumber(
    record["last_traded_price"],
    "polymarket.clob.last_traded_price",
    issues,
  );
  const updatedAt = parseNullableTimestamptzString(
    record["updated_at"],
    "polymarket.clob.updated_at",
    issues,
  );

  if (marketId === null) {
    return null;
  }

  return {
    marketId,
    liquidity,
    lastTradedPrice,
    updatedAt,
    payload: record,
  };
}

function parseBoolean(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): boolean | null {
  if (typeof value !== "boolean") {
    issues.push({ path, message: "must be a boolean" });
    return null;
  }

  return value;
}

function parseStringArray(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): readonly string[] | null {
  if (!Array.isArray(value)) {
    issues.push({ path, message: "must be an array of strings" });
    return null;
  }

  const parsed: string[] = [];

  for (let index = 0; index < value.length; index += 1) {
    const entry = parseString(value[index], `${path}.${index}`, issues);
    if (entry === null) {
      return null;
    }

    parsed.push(entry);
  }

  return parsed;
}

function parseStringPair(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): readonly [string, string] | null {
  const parsed = parseStringArray(value, path, issues);

  if (parsed === null) {
    return null;
  }

  if (parsed.length !== 2) {
    issues.push({ path, message: "must contain exactly 2 strings" });
    return null;
  }

  const first = parsed[0];
  const second = parsed[1];

  if (first === undefined || second === undefined) {
    issues.push({ path, message: "must contain exactly 2 strings" });
    return null;
  }

  return [first, second];
}

function parseNumberPair(
  value: unknown,
  path: string,
  issues: ValidationIssue[],
): readonly [number, number] | null {
  if (!Array.isArray(value)) {
    issues.push({ path, message: "must be an array of numbers" });
    return null;
  }

  if (value.length !== 2) {
    issues.push({ path, message: "must contain exactly 2 numbers" });
    return null;
  }

  const first = parseProbability(value[0], `${path}.0`, issues);
  const second = parseProbability(value[1], `${path}.1`, issues);

  if (first === null || second === null) {
    return null;
  }

  return [first, second];
}

function hasIplTag(tags: readonly string[]): boolean {
  return tags.some((tag) => tag.toLowerCase() === "ipl");
}

function looksLikeWinnerMarket(
  market: Pick<ParsedGammaMarket, "slug" | "question" | "sportsMarketType">,
): boolean {
  if (market.sportsMarketType === "moneyline") {
    return true;
  }

  return (
    looksLikeWinnerSlug(market.slug) && looksLikeWinnerQuestion(market.question)
  );
}

function looksLikeWinnerSlug(slug: string): boolean {
  return slug.toLowerCase().includes("winner");
}

function looksLikeWinnerQuestion(question: string): boolean {
  return question.toLowerCase().includes("who will win");
}
