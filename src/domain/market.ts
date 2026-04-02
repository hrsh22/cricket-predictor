import {
  collectUnknownKeys,
  DomainValidationError,
  parseJsonObject,
  parseNullableFiniteNumber,
  parseNullableString,
  parseProbabilityRecord,
  parseRecord,
  parseString,
  parseTimestamptzString,
  type JsonObject,
  type ValidationIssue,
} from "./primitives.js";

export interface MarketSnapshot {
  competition: "IPL";
  sourceMarketId: string;
  marketSlug: string;
  eventSlug: string | null;
  snapshotTime: string;
  marketStatus: string | null;
  yesOutcomeName: string | null;
  noOutcomeName: string | null;
  outcomeProbabilities: Record<string, number>;
  lastTradedPrice: number | null;
  liquidity: number | null;
  payload: JsonObject;
}

export function parseMarketSnapshot(value: unknown): MarketSnapshot {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "market", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(
    ...collectUnknownKeys(record, [
      "competition",
      "sourceMarketId",
      "marketSlug",
      "eventSlug",
      "snapshotTime",
      "marketStatus",
      "yesOutcomeName",
      "noOutcomeName",
      "outcomeProbabilities",
      "lastTradedPrice",
      "liquidity",
      "payload",
    ]),
  );

  const competition = parseString(record["competition"], "market.competition", issues);
  const sourceMarketId = parseString(record["sourceMarketId"], "market.sourceMarketId", issues);
  const marketSlug = parseString(record["marketSlug"], "market.marketSlug", issues);
  const eventSlug = parseNullableString(record["eventSlug"], "market.eventSlug", issues);
  const snapshotTime = parseTimestamptzString(record["snapshotTime"], "market.snapshotTime", issues);
  const marketStatus = parseNullableString(record["marketStatus"], "market.marketStatus", issues);
  const yesOutcomeName = parseNullableString(record["yesOutcomeName"], "market.yesOutcomeName", issues);
  const noOutcomeName = parseNullableString(record["noOutcomeName"], "market.noOutcomeName", issues);
  const outcomeProbabilities = parseProbabilityRecord(record["outcomeProbabilities"], "market.outcomeProbabilities", issues);
  const lastTradedPrice = parseNullableFiniteNumber(record["lastTradedPrice"], "market.lastTradedPrice", issues);
  const liquidity = parseNullableFiniteNumber(record["liquidity"], "market.liquidity", issues);
  const payload = parseJsonObject(record["payload"], "market.payload", issues);

  if (competition !== "IPL") {
    issues.push({ path: "market.competition", message: "must be IPL" });
  }

  if (issues.length > 0 || sourceMarketId === null || marketSlug === null || snapshotTime === null || outcomeProbabilities === null || payload === null) {
    throw new DomainValidationError(issues);
  }

  return {
    competition: "IPL",
    sourceMarketId,
    marketSlug,
    eventSlug,
    snapshotTime,
    marketStatus,
    yesOutcomeName,
    noOutcomeName,
    outcomeProbabilities,
    lastTradedPrice,
    liquidity,
    payload,
  };
}
