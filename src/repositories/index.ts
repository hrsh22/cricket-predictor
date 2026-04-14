import { loadAppConfig, type AppConfig } from "../config/index.js";
import { closePgPool, createPgPool } from "./postgres.js";
import {
  createNormalizedRepository,
  type NormalizedRepository,
} from "./normalized.js";
import {
  createRawSnapshotRepository,
  type RawSnapshotRepository,
} from "./raw.js";
import { createReadModelRepository, type ReadModelRepository } from "./read.js";
import {
  createModelingRepository,
  type ModelingRepository,
} from "./modeling.js";
import {
  createMatchingRepository,
  type MatchingRepository,
} from "./matching.js";

export interface RepositorySet {
  raw: RawSnapshotRepository;
  normalized: NormalizedRepository;
  read: ReadModelRepository;
  modeling: ModelingRepository;
  matching: MatchingRepository;
  close(): Promise<void>;
}

export function createRepositorySet(
  config: Pick<AppConfig, "databaseUrl"> = loadAppConfig(),
): RepositorySet {
  const pool = createPgPool(config.databaseUrl);

  return {
    raw: createRawSnapshotRepository(pool),
    normalized: createNormalizedRepository(pool),
    read: createReadModelRepository(pool),
    modeling: createModelingRepository(pool),
    matching: createMatchingRepository(pool),
    close: () => closePgPool(pool),
  };
}

export { createPgPool };
export { createRawSnapshotRepository };
export { createNormalizedRepository };
export { createReadModelRepository };
export { createModelingRepository };
export { createMatchingRepository };
export type { AppConfig } from "../config/index.js";
export type {
  RawCricketSnapshotInsert,
  RawCricketSnapshotRecord,
  RawMarketSnapshotRecord,
  RawPolymarketPriceHistoryInsert,
  RawPolymarketPriceHistoryRecord,
  RawPolymarketTradeInsert,
  RawPolymarketTradeRecord,
  RawSnapshotRepository,
} from "./raw.js";
export type {
  CanonicalMatchRecord,
  CheckpointStateRecord,
  FeatureRowRecord,
  NormalizedRepository,
} from "./normalized.js";
export type { MatchReadModel, ReadModelRepository } from "./read.js";
export type {
  BacktestInsert,
  BacktestRecord,
  ModelingRepository,
  ModelRegistryInsert,
  ModelRegistryRecord,
  ModelScoreInsert,
  ModelScoreRecord,
  ScoringRunInsert,
  ScoringRunRecord,
} from "./modeling.js";
export type {
  MappingStatus,
  MarketMatchMappingInsert,
  MarketMatchMappingRecord,
  MatchResolutionCandidate,
  MatchResolutionMarketSnapshot,
  MatchingRepository,
  ResolverCandidate,
} from "./matching.js";
