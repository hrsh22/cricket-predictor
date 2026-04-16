import type { Pool } from "pg";

import { parseMarketSnapshot, type CheckpointType } from "../domain/index.js";
import {
  buildInningsBreakFeatureRow,
  buildBaselinePreMatchFeatureRow,
  type PreMatchFeatureContext,
} from "../features/index.js";
import {
  ingestCricketSnapshots,
  type CricketProviderKey,
  type CricketSnapshotInput,
} from "../ingest/cricket/index.js";
import {
  ingestPolymarketIplWinnerMarkets,
  type PolymarketGammaReadClient,
} from "../ingest/polymarket/index.js";
import { resolveAndPersistMarketMatchMappings } from "../matching/index.js";
import {
  scoreAndPersistPreMatchValuation,
  scoreInningsBreakCheckpoint,
  scorePostTossValuation,
} from "../scoring/index.js";
import type { PreMatchValuationResult } from "../scoring/pre-match.js";
import { createMatchingRepository } from "../repositories/matching.js";
import {
  createModelingRepository,
  type ModelRegistryRecord,
  type ModelScoreRecord,
} from "../repositories/modeling.js";
import {
  createNormalizedRepository,
  type CheckpointStateRecord,
} from "../repositories/normalized.js";
import { createRawSnapshotRepository } from "../repositories/raw.js";
import { createReadModelRepository } from "../repositories/read.js";
import type { JsonObject } from "../domain/primitives.js";
import type {
  CanonicalMatch,
  CanonicalCheckpoint,
  ValuationResult,
} from "../domain/checkpoint.js";
import type { MarketSnapshot } from "../domain/market.js";
import type { SqlExecutor } from "../repositories/postgres.js";

const SUPPORTED_CHECKPOINT_TYPES: readonly CheckpointType[] = [
  "pre_match",
  "post_toss",
  "innings_break",
];

const EMPTY_PRE_MATCH_CONTEXT: PreMatchFeatureContext = {
  teamRatings: {},
  teamRatingDeviations: {},
  teamRecentForm: {},
  teamSchedule: {},
  teamVenueStrength: {},
  teamHeadToHeadStrength: {},
  venueTossDecisionWinRate: {},
  teamLineupContext: {},
  teamRoleCompositionContext: {},
  teamStyleCompositionContext: {},
};

interface TransactionalRepositorySet {
  executor: SqlExecutor;
  raw: ReturnType<typeof createRawSnapshotRepository>;
  normalized: ReturnType<typeof createNormalizedRepository>;
  read: ReturnType<typeof createReadModelRepository>;
  modeling: ReturnType<typeof createModelingRepository>;
  matching: ReturnType<typeof createMatchingRepository>;
}

export interface RecurringRunRequest {
  checkpointType: CheckpointType;
  runKey?: string;
  triggeredBy?: string;
  marketIngestion?: {
    gammaClient?: PolymarketGammaReadClient;
    trigger?: "manual" | "scheduled";
  };
  cricketIngestion?: {
    snapshots: readonly CricketSnapshotInput[];
    provider?: CricketProviderKey;
  };
  featureContextByMatchSlug?: Record<string, PreMatchFeatureContext>;
}

export function scopeEligibleMappingsToRun<
  T extends { sourceMarketId: string },
>(
  eligibleMappings: readonly T[],
  sourceMarketIds: readonly string[],
): readonly T[] {
  if (sourceMarketIds.length === 0) {
    return eligibleMappings;
  }

  return eligibleMappings.filter((mapping) =>
    sourceMarketIds.includes(mapping.sourceMarketId),
  );
}

export interface RecurringRunReportRow {
  checkpointType: CheckpointType;
  matchSlug: string;
  sourceMarketId: string;
  sourceMarketSnapshotId: number;
  modelKey: string;
  modelVersion: string;
  teamAName: string;
  teamBName: string;
  yesOutcomeName: string;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  spread: number | null;
  note: string;
  scoredAt: string;
  tradeThesis?: {
    position: "bet_yes" | "bet_no" | "hold";
    outcomeName: string;
    edgeCents: number;
    contractPriceCents: number;
    fairValueCents: number;
    conviction: "fragile" | "tradable" | "strong";
    mispricingSummary: string;
    counterpartySummary: string;
  };
}

export interface RecurringRunSummary {
  runKey: string;
  checkpointType: CheckpointType;
  triggeredBy: string;
  ingest: {
    marketsPersisted: number;
    cricketSnapshotsPersisted: number;
    marketSourceIds: string[];
  };
  normalize: {
    normalizedSnapshots: number;
    checkpointSnapshots: number;
    featureRows: number;
  };
  map: {
    totalMarkets: number;
    resolvedCount: number;
    ambiguousCount: number;
    unresolvedCount: number;
  };
  score: {
    scoredCount: number;
    skippedCount: number;
    skipped: Array<{
      matchSlug: string;
      reason: string;
      detail: string;
    }>;
  };
  report: {
    noData: boolean;
    rows: RecurringRunReportRow[];
  };
}

export async function runRecurringPipeline(
  pool: Pool,
  request: RecurringRunRequest,
): Promise<RecurringRunSummary> {
  assertSupportedCheckpointType(request.checkpointType);

  const client = await pool.connect();
  const startedAt = new Date().toISOString();
  const runKey =
    request.runKey ?? buildRecurringRunKey(request.checkpointType, startedAt);
  const triggeredBy = request.triggeredBy ?? "scheduled";
  const repositories = createTransactionalRepositorySet(client);

  await client.query("begin");

  try {
    await claimRecurringRun(client, {
      runKey,
      checkpointType: request.checkpointType,
      triggeredBy,
      startedAt,
      metadata: {
        source: "task-19-recurring-orchestration",
        checkpointType: request.checkpointType,
      },
    });

    const ingestSummary = await runIngestStage({
      repositories,
      marketIngestion: request.marketIngestion,
      cricketIngestion: request.cricketIngestion,
    });

    const mapSummary = await resolveAndPersistMarketMatchMappings({
      repository: repositories.matching,
      ...(ingestSummary.marketSourceIds.length === 0
        ? {}
        : { sourceMarketIds: ingestSummary.marketSourceIds }),
    });

    const scoreSummary = await runScoreStage({
      repositories,
      checkpointType: request.checkpointType,
      runKey,
      featureContextByMatchSlug: request.featureContextByMatchSlug ?? {},
      sourceMarketIds: ingestSummary.marketSourceIds,
      cricketMatchSlugs: ingestSummary.cricketMatchSlugs,
    });

    const report = buildRecurringRunReport(scoreSummary.rows);

    await completeRecurringRun(client, {
      runKey,
      completedAt: new Date().toISOString(),
      metadata: {
        source: "task-19-recurring-orchestration",
        checkpointType: request.checkpointType,
        scoredCount: scoreSummary.scoredCount,
        skippedCount: scoreSummary.skippedCount,
        reportRowCount: report.rows.length,
      },
    });

    await client.query("commit");

    return {
      runKey,
      checkpointType: request.checkpointType,
      triggeredBy,
      ingest: ingestSummary,
      normalize: {
        normalizedSnapshots: ingestSummary.cricketSnapshotsPersisted,
        checkpointSnapshots: ingestSummary.cricketSnapshotsPersisted,
        featureRows: scoreSummary.scoredCount,
      },
      map: {
        totalMarkets: mapSummary.totalMarkets,
        resolvedCount: mapSummary.resolvedCount,
        ambiguousCount: mapSummary.ambiguousCount,
        unresolvedCount: mapSummary.unresolvedCount,
      },
      score: {
        scoredCount: scoreSummary.scoredCount,
        skippedCount: scoreSummary.skippedCount,
        skipped: scoreSummary.skipped,
      },
      report,
    };
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}

async function runIngestStage(input: {
  repositories: TransactionalRepositorySet;
  marketIngestion?: RecurringRunRequest["marketIngestion"];
  cricketIngestion?: RecurringRunRequest["cricketIngestion"];
}): Promise<RecurringRunSummary["ingest"] & { cricketMatchSlugs: string[] }> {
  let marketsPersisted = 0;
  let cricketSnapshotsPersisted = 0;
  let marketSourceIds: string[] = [];
  let cricketMatchSlugs: string[] = [];

  if (input.marketIngestion !== undefined) {
    const marketSummary = await ingestPolymarketIplWinnerMarkets({
      rawRepository: input.repositories.raw,
      ...(input.marketIngestion.gammaClient === undefined
        ? {}
        : { gammaClient: input.marketIngestion.gammaClient }),
      ...(input.marketIngestion.trigger === undefined
        ? {}
        : { trigger: input.marketIngestion.trigger }),
    });

    marketsPersisted = marketSummary.persistedCount;
    marketSourceIds = marketSummary.snapshots.map(
      (snapshot) => snapshot.sourceMarketId,
    );
  }

  if (input.cricketIngestion !== undefined) {
    const cricketSummary = await ingestCricketSnapshots(
      {
        raw: input.repositories.raw,
        normalized: input.repositories.normalized,
      },
      input.cricketIngestion.snapshots,
      input.cricketIngestion.provider ?? "cricapi",
    );

    cricketSnapshotsPersisted = cricketSummary.results.filter(
      (result) => result.status === "normalized",
    ).length;

    cricketMatchSlugs = cricketSummary.results
      .filter(
        (result) =>
          result.status === "normalized" && result.canonicalMatch !== null,
      )
      .map((result) => result.canonicalMatch!.matchSlug);
  }

  return {
    marketsPersisted,
    cricketSnapshotsPersisted,
    marketSourceIds,
    cricketMatchSlugs,
  };
}

async function runScoreStage(input: {
  repositories: TransactionalRepositorySet;
  checkpointType: CheckpointType;
  runKey: string;
  featureContextByMatchSlug: Record<string, PreMatchFeatureContext>;
  sourceMarketIds: readonly string[];
  cricketMatchSlugs: readonly string[];
}): Promise<{
  scoredCount: number;
  skippedCount: number;
  skipped: Array<{ matchSlug: string; reason: string; detail: string }>;
  rows: RecurringRunReportRow[];
}> {
  const eligibleMappings =
    await input.repositories.matching.listScorerEligibleMappings();
  const marketScopedMappings = scopeEligibleMappingsToRun(
    eligibleMappings,
    input.sourceMarketIds,
  );
  const scopedMappings =
    input.cricketMatchSlugs.length > 0
      ? marketScopedMappings.filter(
          (mapping) =>
            mapping.matchSlug !== null &&
            input.cricketMatchSlugs.includes(mapping.matchSlug),
        )
      : [];
  const scoredRows: RecurringRunReportRow[] = [];
  const skipped: Array<{ matchSlug: string; reason: string; detail: string }> =
    [];

  for (const mapping of scopedMappings) {
    if (mapping.matchSlug === null || mapping.canonicalMatchId === null) {
      continue;
    }

    const modelRegistry = await loadActiveModelRegistry(
      input.repositories.executor,
      input.checkpointType,
    );

    const readModel = await input.repositories.read.getMatchReadModel(
      mapping.matchSlug,
    );

    if (readModel === null) {
      skipped.push({
        matchSlug: mapping.matchSlug,
        reason: "missing_read_model",
        detail: "canonical match read model was unavailable",
      });
      continue;
    }

    const checkpointState = selectLatestCheckpointState(
      readModel.checkpointStates,
      input.checkpointType,
    );

    if (checkpointState === null) {
      skipped.push({
        matchSlug: mapping.matchSlug,
        reason: "missing_checkpoint_state",
        detail: `no ${input.checkpointType} checkpoint was available`,
      });
      continue;
    }

    const checkpoint = toCanonicalCheckpoint(readModel.match, checkpointState);
    const marketSnapshot = await loadMarketSnapshotById(
      input.repositories.executor,
      mapping.sourceMarketSnapshotId,
    );

    if (input.checkpointType === "pre_match") {
      if (
        checkpointState.sourceMarketSnapshotId !== null &&
        checkpointState.sourceMarketSnapshotId !==
          mapping.sourceMarketSnapshotId
      ) {
        skipped.push({
          matchSlug: mapping.matchSlug,
          reason: "market_snapshot_mismatch",
          detail:
            "pre-match checkpoint-linked market snapshot differs from latest resolved mapping snapshot",
        });
        continue;
      }

      const context =
        input.featureContextByMatchSlug[readModel.match.matchSlug] ??
        input.featureContextByMatchSlug["*"] ??
        EMPTY_PRE_MATCH_CONTEXT;
      const featureRow = buildBaselinePreMatchFeatureRow(checkpoint, context);
      await input.repositories.normalized.saveFeatureRow(
        checkpointState.id,
        featureRow,
      );

      const persisted = await scoreAndPersistPreMatchValuation({
        modelingRepository: input.repositories.modeling,
        mapping,
        marketSnapshot,
        featureRow,
        checkpointStateId: checkpointState.id,
        scoringRunKey: input.runKey,
        modelKey: modelRegistry.modelKey,
        modelMetadata: modelRegistry.metadata,
        scoredAt: featureRow.generatedAt,
      });

      scoredRows.push(
        buildReportRowFromPreMatch({
          persistedScore: persisted.persistedScore,
          valuation: persisted.valuation,
          mapping,
          modelRegistry,
          marketSnapshot,
          teamAName: checkpoint.match.teamAName,
          teamBName: checkpoint.match.teamBName,
        }),
      );
      continue;
    }

    if (input.checkpointType === "post_toss") {
      const context =
        input.featureContextByMatchSlug[readModel.match.matchSlug] ??
        input.featureContextByMatchSlug["*"] ??
        EMPTY_PRE_MATCH_CONTEXT;
      const result = scorePostTossValuation({
        checkpoint,
        featureContext: context,
        marketImpliedProbability: readTeamAMarketImpliedProbability(
          marketSnapshot,
          checkpoint.match.teamAName,
          checkpoint.match.teamBName,
        ),
        evaluatedAt: checkpoint.state.snapshotTime,
        modelMetadata: modelRegistry.metadata,
      });
      await input.repositories.normalized.saveFeatureRow(
        checkpointState.id,
        result.featureRow,
      );

      const persistedScore = await input.repositories.modeling.saveModelScore({
        checkpointStateId: checkpointState.id,
        checkpointType: "post_toss",
        scoringRunKey: input.runKey,
        modelKey: modelRegistry.modelKey,
        fairWinProbability: result.valuation.fairWinProbability,
        marketImpliedProbability: result.valuation.marketImpliedProbability,
        edge: result.valuation.edge,
        scoredAt: result.valuation.evaluatedAt,
        scorePayload: result.valuation.valuationPayload,
      });

      scoredRows.push(
        buildReportRowFromValuation({
          checkpointType: "post_toss",
          mapping,
          modelRegistry,
          valuation: result.valuation,
          persistedScore,
          marketSnapshot,
          teamAName: checkpoint.match.teamAName,
          teamBName: checkpoint.match.teamBName,
          note: `Toss ${checkpoint.match.tossWinnerTeamName ?? "unknown"} ${checkpoint.match.tossDecision ?? "unknown"}`,
        }),
      );
      continue;
    }

    const result = scoreInningsBreakCheckpoint({
      checkpoint,
      marketImpliedProbability: readTeamAMarketImpliedProbability(
        marketSnapshot,
        checkpoint.match.teamAName,
        checkpoint.match.teamBName,
      ),
      evaluatedAt: checkpoint.state.snapshotTime,
    });

    if (result.status === "skipped") {
      skipped.push({
        matchSlug: mapping.matchSlug,
        reason: result.reason,
        detail: result.detail,
      });
      continue;
    }

    const featureRowResult = buildInningsBreakFeatureRow(checkpoint);
    if (featureRowResult.status === "skipped") {
      skipped.push({
        matchSlug: mapping.matchSlug,
        reason: featureRowResult.reason,
        detail: featureRowResult.detail,
      });
      continue;
    }

    await input.repositories.normalized.saveFeatureRow(
      checkpointState.id,
      featureRowResult.featureRow,
    );

    const persistedScore = await input.repositories.modeling.saveModelScore({
      checkpointStateId: checkpointState.id,
      checkpointType: "innings_break",
      scoringRunKey: input.runKey,
      modelKey: modelRegistry.modelKey,
      fairWinProbability: result.valuation.fairWinProbability,
      marketImpliedProbability: result.valuation.marketImpliedProbability,
      edge: result.valuation.edge,
      scoredAt: result.valuation.evaluatedAt,
      scorePayload: result.valuation.valuationPayload,
    });

    scoredRows.push(
      buildReportRowFromValuation({
        checkpointType: "innings_break",
        mapping,
        modelRegistry,
        valuation: result.valuation,
        persistedScore,
        marketSnapshot,
        teamAName: checkpoint.match.teamAName,
        teamBName: checkpoint.match.teamBName,
        note: `Innings break ${checkpoint.state.runs}/${checkpoint.state.wickets} in ${checkpoint.state.overs} overs`,
      }),
    );
  }

  return {
    scoredCount: scoredRows.length,
    skippedCount: skipped.length,
    skipped,
    rows: scoredRows,
  };
}

function buildRecurringRunReport(rows: readonly RecurringRunReportRow[]): {
  noData: boolean;
  rows: RecurringRunReportRow[];
} {
  const sortedRows = [...rows].sort((left, right) => {
    const leftSpread = left.spread ?? Number.NEGATIVE_INFINITY;
    const rightSpread = right.spread ?? Number.NEGATIVE_INFINITY;

    if (rightSpread !== leftSpread) {
      return rightSpread - leftSpread;
    }

    return left.matchSlug.localeCompare(right.matchSlug);
  });

  return {
    noData: sortedRows.length === 0,
    rows: sortedRows,
  };
}

function buildReportRowFromPreMatch(input: {
  persistedScore: ModelScoreRecord;
  valuation: PreMatchValuationResult;
  mapping: { sourceMarketId: string; sourceMarketSnapshotId: number };
  modelRegistry: ModelRegistryRecord;
  marketSnapshot: MarketSnapshot;
  teamAName: string;
  teamBName: string;
}): RecurringRunReportRow {
  const tradeThesis = extractRecurringTradeThesis(input.valuation.scorePayload);

  return {
    checkpointType: "pre_match",
    matchSlug: input.persistedScore.matchSlug,
    sourceMarketId: input.mapping.sourceMarketId,
    sourceMarketSnapshotId: input.mapping.sourceMarketSnapshotId,
    modelKey: input.persistedScore.modelKey,
    modelVersion: input.valuation.modelVersion,
    teamAName: input.teamAName,
    teamBName: input.teamBName,
    yesOutcomeName: input.marketSnapshot.yesOutcomeName ?? input.teamAName,
    fairWinProbability: input.persistedScore.fairWinProbability,
    marketImpliedProbability: input.persistedScore.marketImpliedProbability,
    spread: input.persistedScore.edge,
    note: input.valuation.socialAdjustmentNote,
    scoredAt: input.persistedScore.scoredAt,
    ...(tradeThesis === undefined ? {} : { tradeThesis }),
  };
}

function buildReportRowFromValuation(input: {
  checkpointType: CheckpointType;
  persistedScore: ModelScoreRecord;
  valuation: ValuationResult;
  mapping: { sourceMarketId: string; sourceMarketSnapshotId: number };
  modelRegistry: ModelRegistryRecord;
  note: string;
  marketSnapshot: MarketSnapshot;
  teamAName: string;
  teamBName: string;
}): RecurringRunReportRow {
  return {
    checkpointType: input.checkpointType,
    matchSlug: input.persistedScore.matchSlug,
    sourceMarketId: input.mapping.sourceMarketId,
    sourceMarketSnapshotId: input.mapping.sourceMarketSnapshotId,
    modelKey: input.persistedScore.modelKey,
    modelVersion: input.modelRegistry.version,
    teamAName: input.teamAName,
    teamBName: input.teamBName,
    yesOutcomeName: input.marketSnapshot.yesOutcomeName ?? input.teamAName,
    fairWinProbability: input.persistedScore.fairWinProbability,
    marketImpliedProbability: input.persistedScore.marketImpliedProbability,
    spread: input.persistedScore.edge,
    note: input.note,
    scoredAt: input.persistedScore.scoredAt,
  };
}

function extractRecurringTradeThesis(
  scorePayload: JsonObject,
): RecurringRunReportRow["tradeThesis"] {
  const tradeThesis = readJsonObject(scorePayload["tradeThesis"]);
  if (tradeThesis === null) {
    return undefined;
  }

  const position = tradeThesis["position"];
  const outcomeName = readJsonString(tradeThesis["outcomeName"]);
  const edgeCents = readJsonNumber(tradeThesis["edgeCents"]);
  const contractPriceCents = readJsonNumber(tradeThesis["contractPriceCents"]);
  const fairValueCents = readJsonNumber(tradeThesis["fairValueCents"]);
  const conviction = tradeThesis["conviction"];
  const mispricingSummary = readJsonString(tradeThesis["mispricingSummary"]);
  const counterpartySummary = readJsonString(
    tradeThesis["counterpartySummary"],
  );

  if (
    (position !== "bet_yes" && position !== "bet_no" && position !== "hold") ||
    outcomeName === null ||
    edgeCents === null ||
    contractPriceCents === null ||
    fairValueCents === null ||
    (conviction !== "fragile" &&
      conviction !== "tradable" &&
      conviction !== "strong") ||
    mispricingSummary === null ||
    counterpartySummary === null
  ) {
    return undefined;
  }

  return {
    position,
    outcomeName,
    edgeCents,
    contractPriceCents,
    fairValueCents,
    conviction,
    mispricingSummary,
    counterpartySummary,
  };
}

function readJsonObject(value: unknown): JsonObject | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  return value as JsonObject;
}

function readJsonString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readJsonNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function toCanonicalCheckpoint(
  match: CanonicalMatch,
  checkpointState: CheckpointStateRecord,
): CanonicalCheckpoint {
  return {
    checkpointType: checkpointState.checkpointType,
    match,
    state: {
      matchSlug: checkpointState.matchSlug,
      checkpointType: checkpointState.checkpointType,
      snapshotTime: checkpointState.snapshotTime,
      stateVersion: checkpointState.stateVersion,
      sourceMarketSnapshotId: checkpointState.sourceMarketSnapshotId,
      sourceCricketSnapshotId: checkpointState.sourceCricketSnapshotId,
      inningsNumber: normalizeNullableInteger(checkpointState.inningsNumber),
      battingTeamName: checkpointState.battingTeamName,
      bowlingTeamName: checkpointState.bowlingTeamName,
      runs: normalizeNullableInteger(checkpointState.runs),
      wickets: normalizeNullableInteger(checkpointState.wickets),
      overs: normalizeNullableNumber(checkpointState.overs),
      targetRuns: normalizeNullableInteger(checkpointState.targetRuns),
      currentRunRate: normalizeNullableNumber(checkpointState.currentRunRate),
      requiredRunRate: normalizeNullableNumber(checkpointState.requiredRunRate),
      statePayload: checkpointState.statePayload,
    } as CanonicalCheckpoint["state"],
  };
}

function selectLatestCheckpointState(
  checkpointStates: readonly CheckpointStateRecord[],
  checkpointType: CheckpointType,
): CheckpointStateRecord | null {
  const matchingStates = checkpointStates
    .filter((state) => state.checkpointType === checkpointType)
    .sort((left, right) => {
      const snapshotCompare =
        Date.parse(right.snapshotTime) - Date.parse(left.snapshotTime);
      if (snapshotCompare !== 0) {
        return snapshotCompare;
      }

      return right.stateVersion - left.stateVersion;
    });

  return matchingStates[0] ?? null;
}

async function loadActiveModelRegistry(
  executor: SqlExecutor,
  checkpointType: CheckpointType,
): Promise<ModelRegistryRecord> {
  const result = await executor.query<{
    id: string | number;
    model_key: string;
    checkpoint_type: CheckpointType;
    model_family: string;
    version: string;
    training_window: string | null;
    is_active: boolean;
    metadata: JsonObject;
    created_at: Date;
  }>(
    `
      select
        id,
        model_key,
        checkpoint_type,
        model_family,
        version,
        training_window,
        is_active,
        metadata,
        created_at
      from model_registry
      where checkpoint_type = $1
        and is_active = true
      order by created_at desc, id desc
      limit 1
    `,
    [checkpointType],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new Error(
      `No active model registry found for checkpoint ${checkpointType}.`,
    );
  }

  return {
    id: Number(row.id),
    modelKey: row.model_key,
    checkpointType: row.checkpoint_type,
    modelFamily: row.model_family,
    version: row.version,
    trainingWindow: row.training_window,
    isActive: row.is_active,
    metadata: row.metadata,
    createdAt: row.created_at.toISOString(),
  };
}

async function loadMarketSnapshotById(
  executor: SqlExecutor,
  sourceMarketSnapshotId: number,
): Promise<MarketSnapshot> {
  const result = await executor.query<{
    competition: "IPL";
    source_market_id: string;
    market_slug: string;
    event_slug: string | null;
    snapshot_time: Date;
    market_status: string | null;
    yes_outcome_name: string | null;
    no_outcome_name: string | null;
    outcome_probabilities: Record<string, number>;
    last_traded_price: string | number | null;
    liquidity: string | number | null;
    payload: JsonObject;
  }>(
    `
      select
        competition,
        source_market_id,
        market_slug,
        event_slug,
        snapshot_time,
        market_status,
        yes_outcome_name,
        no_outcome_name,
        outcome_probabilities,
        last_traded_price,
        liquidity,
        payload
      from raw_market_snapshots
      where id = $1
    `,
    [sourceMarketSnapshotId],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new Error(
      `Raw market snapshot ${sourceMarketSnapshotId} was not found.`,
    );
  }

  return parseMarketSnapshot({
    competition: row.competition,
    sourceMarketId: row.source_market_id,
    marketSlug: row.market_slug,
    eventSlug: row.event_slug,
    snapshotTime: row.snapshot_time.toISOString(),
    marketStatus: row.market_status,
    yesOutcomeName: row.yes_outcome_name,
    noOutcomeName: row.no_outcome_name,
    outcomeProbabilities: row.outcome_probabilities,
    lastTradedPrice:
      row.last_traded_price === null ? null : Number(row.last_traded_price),
    liquidity: row.liquidity === null ? null : Number(row.liquidity),
    payload: row.payload,
  });
}

async function claimRecurringRun(
  executor: SqlExecutor,
  input: {
    runKey: string;
    checkpointType: CheckpointType;
    triggeredBy: string;
    startedAt: string;
    metadata: JsonObject;
  },
): Promise<void> {
  const inserted = await executor.query(
    `
      insert into scoring_runs (
        run_key,
        checkpoint_type,
        run_status,
        triggered_by,
        started_at,
        completed_at,
        input_snapshot_time,
        notes,
        metadata
      ) values ($1, $2, 'running', $3, $4, null, null, null, $5)
      on conflict (run_key) do nothing
      returning id
    `,
    [
      input.runKey,
      input.checkpointType,
      input.triggeredBy,
      input.startedAt,
      input.metadata,
    ],
  );

  if (inserted.rowCount === 1) {
    return;
  }

  const existing = await executor.query<{
    run_status: string;
  }>(
    `
      select run_status
      from scoring_runs
      where run_key = $1
    `,
    [input.runKey],
  );

  const row = existing.rows[0];
  if (row === undefined) {
    throw new Error(`Unable to claim recurring run ${input.runKey}.`);
  }

  throw new Error(
    `Recurring run ${input.runKey} is already recorded with status ${row.run_status}.`,
  );
}

async function completeRecurringRun(
  executor: SqlExecutor,
  input: {
    runKey: string;
    completedAt: string;
    metadata: JsonObject;
  },
): Promise<void> {
  await executor.query(
    `
      update scoring_runs
      set run_status = 'succeeded',
          completed_at = $2,
          metadata = $3
      where run_key = $1
    `,
    [input.runKey, input.completedAt, input.metadata],
  );
}

function createTransactionalRepositorySet(executor: SqlExecutor) {
  return {
    executor,
    raw: createRawSnapshotRepository(executor),
    normalized: createNormalizedRepository(executor),
    read: createReadModelRepository(executor),
    modeling: createModelingRepository(executor),
    matching: createMatchingRepository(executor),
  };
}

function buildRecurringRunKey(
  checkpointType: CheckpointType,
  startedAt: string,
): string {
  return `recurring-${checkpointType}-${startedAt.replace(/[:.]/g, "-")}`;
}

function readTeamAMarketImpliedProbability(
  marketSnapshot: MarketSnapshot,
  teamAName: string,
  teamBName: string,
): number {
  const yesOutcomeName = requireString(marketSnapshot.yesOutcomeName, {
    field: "marketSnapshot.yesOutcomeName",
  });
  const noOutcomeName = requireString(marketSnapshot.noOutcomeName, {
    field: "marketSnapshot.noOutcomeName",
  });

  const yesOutcomeSide = resolveOutcomeSide({
    outcomeTeamName: yesOutcomeName,
    teamAName,
    teamBName,
    field: "marketSnapshot.yesOutcomeName",
  });

  const teamAProbability =
    yesOutcomeSide === "team_a"
      ? readRequiredMarketProbability(
          marketSnapshot.outcomeProbabilities,
          "yes",
        )
      : yesOutcomeSide === "team_b"
        ? readRequiredMarketProbability(
            marketSnapshot.outcomeProbabilities,
            "no",
          )
        : null;

  if (teamAProbability === null) {
    throw new Error(
      `Unable to align market probability to team A for ${teamAName} vs ${teamBName}.`,
    );
  }

  const noOutcomeSide = resolveOutcomeSide({
    outcomeTeamName: noOutcomeName,
    teamAName,
    teamBName,
    field: "marketSnapshot.noOutcomeName",
  });

  if (
    (yesOutcomeSide === "team_a" && noOutcomeSide !== "team_b") ||
    (yesOutcomeSide === "team_b" && noOutcomeSide !== "team_a")
  ) {
    throw new Error(
      `Market outcomes do not cleanly map to team A / team B for ${teamAName} vs ${teamBName}.`,
    );
  }

  return Number(teamAProbability.toFixed(6));
}

function readRequiredMarketProbability(
  outcomeProbabilities: Record<string, number>,
  outcomeKey: string,
): number {
  const probability = outcomeProbabilities[outcomeKey];
  if (
    typeof probability !== "number" ||
    !Number.isFinite(probability) ||
    probability < 0 ||
    probability > 1
  ) {
    throw new Error(
      `marketSnapshot.outcomeProbabilities["${outcomeKey}"] must be in [0, 1].`,
    );
  }

  return probability;
}

function requireString(
  value: string | null,
  context: { field: string },
): string {
  if (value === null) {
    throw new Error(`${context.field} is required.`);
  }

  return value;
}

function resolveOutcomeSide(input: {
  outcomeTeamName: string;
  teamAName: string;
  teamBName: string;
  field: string;
}): "team_a" | "team_b" {
  if (matchesTeam(input.outcomeTeamName, input.teamAName)) {
    return "team_a";
  }

  if (matchesTeam(input.outcomeTeamName, input.teamBName)) {
    return "team_b";
  }

  throw new Error(
    `${input.field} does not match either team in the checkpoint match.`,
  );
}

function matchesTeam(left: string, right: string): boolean {
  return normalizeTeamName(left) === normalizeTeamName(right);
}

function normalizeTeamName(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, " ")
    .trim();
}

function normalizeNullableNumber(value: number | string | null): number | null {
  if (value === null) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeNullableInteger(
  value: number | string | null,
): number | null {
  if (value === null) {
    return null;
  }

  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function assertSupportedCheckpointType(
  checkpointType: CheckpointType,
): asserts checkpointType is CheckpointType {
  if (!SUPPORTED_CHECKPOINT_TYPES.includes(checkpointType)) {
    throw new Error(`Unsupported checkpoint type: ${checkpointType}`);
  }
}
