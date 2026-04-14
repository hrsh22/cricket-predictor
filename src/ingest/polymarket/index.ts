export {
  normalizePolymarketIplWinnerMarket,
  normalizePolymarketIplWinnerMarkets,
  type PolymarketClobReadClient,
  type PolymarketGammaMarketQuery,
  type PolymarketGammaReadClient,
  type PolymarketIplWinnerMarketDiscovery,
} from "./client.js";

export {
  createPolymarketGammaReadClient,
  createSnapshotDedupeKey,
  ingestPolymarketIplWinnerMarkets,
  polymarketDefaults,
  runPolymarketIplWinnerIngestion,
  toPolymarketIplWinnerDiscovery,
} from "./ingest.js";

export {
  backfillPolymarketEventHistoricalOdds,
  createTradeKey,
  runPolymarketHistoricalBackfill,
} from "./historical.js";

export type {
  PolymarketFetch,
  PolymarketGammaHttpClientOptions,
  PolymarketIngestionCommandOptions,
  PolymarketIngestionOptions,
  PolymarketIngestionSummary,
  PolymarketResponseLike,
  PolymarketRetryPolicy,
} from "./ingest.js";

export type {
  PolymarketHistoricalBackfillCommandOptions,
  PolymarketHistoricalBackfillOptions,
  PolymarketHistoricalBackfillSummary,
} from "./historical.js";
