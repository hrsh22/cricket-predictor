import type { CheckpointType, ModelScore } from "../domain/checkpoint.js";
import type { JsonObject, ValidationIssue } from "../domain/primitives.js";
import { DomainValidationError } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";
import type { CheckpointStateRow } from "./normalized.js";

export type ScoringRunStatus = "running" | "succeeded" | "failed";

export interface ModelRegistryInsert {
  modelKey: string;
  checkpointType: CheckpointType;
  modelFamily: string;
  version: string;
  trainingWindow: string | null;
  isActive: boolean;
  metadata: JsonObject;
  createdAt: string;
}

export interface ModelRegistryRecord extends ModelRegistryInsert {
  id: number;
}

export interface ScoringRunInsert {
  runKey: string;
  checkpointType: CheckpointType;
  runStatus: ScoringRunStatus;
  triggeredBy: string;
  startedAt: string;
  completedAt: string | null;
  inputSnapshotTime: string | null;
  notes: string | null;
  metadata: JsonObject;
}

export interface ScoringRunRecord extends ScoringRunInsert {
  id: number;
}

export interface ModelScoreInsert extends Omit<ModelScore, "matchSlug"> {
  checkpointStateId: number;
}

export interface ModelScoreRecord {
  id: number;
  scoringRunId: number;
  scoringRunKey: string;
  modelRegistryId: number;
  modelKey: string;
  canonicalMatchId: number;
  matchSlug: string;
  checkpointStateId: number;
  checkpointType: CheckpointType;
  sourceMarketSnapshotId: number | null;
  sourceCricketSnapshotId: number | null;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  edge: number | null;
  scoredAt: string;
  scorePayload: JsonObject;
}

export interface BacktestInsert {
  runKey: string;
  modelKey: string;
  checkpointType: CheckpointType;
  runStatus: ScoringRunStatus;
  seasonFrom: number;
  seasonTo: number;
  sampleSize: number | null;
  logLoss: number | null;
  brierScore: number | null;
  calibrationError: number | null;
  startedAt: string;
  completedAt: string | null;
  summary: JsonObject;
  metadata: JsonObject;
}

export interface BacktestRecord extends BacktestInsert {
  id: number;
  modelRegistryId: number;
}

export interface ModelingRepository {
  saveModelRegistry(
    registry: ModelRegistryInsert,
  ): Promise<ModelRegistryRecord>;
  saveScoringRun(run: ScoringRunInsert): Promise<ScoringRunRecord>;
  saveModelScore(score: ModelScoreInsert): Promise<ModelScoreRecord>;
  saveBacktest(backtest: BacktestInsert): Promise<BacktestRecord>;
}

interface ModelRegistryRow {
  id: string | number;
  model_key: string;
  checkpoint_type: CheckpointType;
  model_family: string;
  version: string;
  training_window: string | null;
  is_active: boolean;
  metadata: JsonObject;
  created_at: Date;
}

interface ScoringRunRow {
  id: string | number;
  run_key: string;
  checkpoint_type: CheckpointType;
  run_status: ScoringRunStatus;
  triggered_by: string;
  started_at: Date;
  completed_at: Date | null;
  input_snapshot_time: Date | null;
  notes: string | null;
  metadata: JsonObject;
}

interface ModelScoreRow {
  id: string | number;
  scoring_run_id: string | number;
  scoring_run_key: string;
  model_registry_id: string | number;
  model_key: string;
  canonical_match_id: string | number;
  match_slug: string;
  checkpoint_state_id: string | number;
  checkpoint_type: CheckpointType;
  source_market_snapshot_id: string | number | null;
  source_cricket_snapshot_id: string | number | null;
  fair_win_probability: string | number;
  market_implied_probability: string | number | null;
  edge: string | number | null;
  scored_at: Date;
  score_payload: JsonObject;
}

interface BacktestRow {
  id: string | number;
  run_key: string;
  model_registry_id: string | number;
  model_key: string;
  checkpoint_type: CheckpointType;
  run_status: ScoringRunStatus;
  season_from: number;
  season_to: number;
  sample_size: number | null;
  log_loss: string | number | null;
  brier_score: string | number | null;
  calibration_error: string | number | null;
  started_at: Date;
  completed_at: Date | null;
  summary: JsonObject;
  metadata: JsonObject;
}

interface ResolvedModelRegistryRow {
  id: string | number;
  model_key: string;
  checkpoint_type: CheckpointType;
}

interface ResolvedScoringRunRow {
  id: string | number;
  run_key: string;
  checkpoint_type: CheckpointType;
}

interface ResolvedCheckpointStateRow extends CheckpointStateRow {
  match_slug: string;
}

export function createModelingRepository(
  executor: SqlExecutor,
): ModelingRepository {
  return {
    async saveModelRegistry(
      registry: ModelRegistryInsert,
    ): Promise<ModelRegistryRecord> {
      const result = await executor.query<ModelRegistryRow>(
        `
          insert into model_registry (
            model_key,
            checkpoint_type,
            model_family,
            version,
            training_window,
            is_active,
            metadata,
            created_at
          ) values ($1, $2, $3, $4, $5, $6, $7, $8)
          on conflict (checkpoint_type, model_family, version) do update set
            training_window = excluded.training_window,
            is_active = excluded.is_active,
            metadata = excluded.metadata,
            created_at = excluded.created_at
          returning
            id,
            model_key,
            checkpoint_type,
            model_family,
            version,
            training_window,
            is_active,
            metadata,
            created_at
        `,
        [
          registry.modelKey,
          registry.checkpointType,
          registry.modelFamily,
          registry.version,
          registry.trainingWindow,
          registry.isActive,
          registry.metadata,
          registry.createdAt,
        ],
      );

      return mapModelRegistryRow(result.rows[0] as ModelRegistryRow);
    },

    async saveScoringRun(run: ScoringRunInsert): Promise<ScoringRunRecord> {
      const result = await executor.query<ScoringRunRow>(
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
          ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          on conflict (run_key) do update set
            checkpoint_type = excluded.checkpoint_type,
            run_status = excluded.run_status,
            triggered_by = excluded.triggered_by,
            started_at = excluded.started_at,
            completed_at = excluded.completed_at,
            input_snapshot_time = excluded.input_snapshot_time,
            notes = excluded.notes,
            metadata = excluded.metadata
          returning
            id,
            run_key,
            checkpoint_type,
            run_status,
            triggered_by,
            started_at,
            completed_at,
            input_snapshot_time,
            notes,
            metadata
        `,
        [
          run.runKey,
          run.checkpointType,
          run.runStatus,
          run.triggeredBy,
          run.startedAt,
          run.completedAt,
          run.inputSnapshotTime,
          run.notes,
          run.metadata,
        ],
      );

      return mapScoringRunRow(result.rows[0] as ScoringRunRow);
    },

    async saveModelScore(score: ModelScoreInsert): Promise<ModelScoreRecord> {
      const modelRegistry = await loadModelRegistryByKey(
        executor,
        score.modelKey,
      );
      const scoringRun = await loadScoringRunByKey(
        executor,
        score.scoringRunKey,
      );
      const checkpointState = await loadCheckpointStateById(
        executor,
        score.checkpointStateId,
      );

      const issues: ValidationIssue[] = [];
      if (modelRegistry.checkpoint_type !== score.checkpointType) {
        issues.push({
          path: "modelScore.checkpointType",
          message: "must match model registry checkpoint type",
        });
      }
      if (scoringRun.checkpoint_type !== score.checkpointType) {
        issues.push({
          path: "modelScore.checkpointType",
          message: "must match scoring run checkpoint type",
        });
      }
      if (checkpointState.checkpoint_type !== score.checkpointType) {
        issues.push({
          path: "modelScore.checkpointType",
          message: "must match checkpoint state checkpoint type",
        });
      }

      if (issues.length > 0) {
        throw new DomainValidationError(issues);
      }

      const result = await executor.query<ModelScoreRow>(
        `
          with upserted as (
            insert into model_scores (
              scoring_run_id,
              canonical_match_id,
              checkpoint_state_id,
              model_registry_id,
              fair_win_probability,
              market_implied_probability,
              edge,
              score_payload,
              scored_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            on conflict (scoring_run_id, canonical_match_id, model_registry_id) do update set
              checkpoint_state_id = excluded.checkpoint_state_id,
              fair_win_probability = excluded.fair_win_probability,
              market_implied_probability = excluded.market_implied_probability,
              edge = excluded.edge,
              score_payload = excluded.score_payload,
              scored_at = excluded.scored_at
            returning
              id,
              scoring_run_id,
              model_registry_id,
              canonical_match_id,
              checkpoint_state_id,
              fair_win_probability,
              market_implied_probability,
              edge,
              scored_at,
              score_payload
          )
          select
            upserted.id,
            upserted.scoring_run_id,
            sr.run_key as scoring_run_key,
            upserted.model_registry_id,
            mr.model_key,
            upserted.canonical_match_id,
            cm.match_slug,
            upserted.checkpoint_state_id,
            cs.checkpoint_type,
            cs.source_market_snapshot_id,
            cs.source_cricket_snapshot_id,
            upserted.fair_win_probability,
            upserted.market_implied_probability,
            upserted.edge,
            upserted.scored_at,
            upserted.score_payload
          from upserted
          join scoring_runs sr on sr.id = upserted.scoring_run_id
          join model_registry mr on mr.id = upserted.model_registry_id
          join checkpoint_states cs on cs.id = upserted.checkpoint_state_id
          join canonical_matches cm on cm.id = upserted.canonical_match_id
        `,
        [
          scoringRun.id,
          checkpointState.canonical_match_id,
          checkpointState.id,
          modelRegistry.id,
          score.fairWinProbability,
          score.marketImpliedProbability,
          score.edge,
          score.scorePayload,
          score.scoredAt,
        ],
      );

      return mapModelScoreRow(
        result.rows[0] as ModelScoreRow,
        checkpointState.match_slug,
      );
    },

    async saveBacktest(backtest: BacktestInsert): Promise<BacktestRecord> {
      const modelRegistry = await loadModelRegistryByKey(
        executor,
        backtest.modelKey,
      );
      if (modelRegistry.checkpoint_type !== backtest.checkpointType) {
        throw new DomainValidationError([
          {
            path: "backtest.checkpointType",
            message: "must match model registry checkpoint type",
          },
        ]);
      }

      const result = await executor.query<BacktestRow>(
        `
          with upserted as (
            insert into backtests (
              run_key,
              model_registry_id,
              checkpoint_type,
              run_status,
              season_from,
              season_to,
              sample_size,
              log_loss,
              brier_score,
              calibration_error,
              started_at,
              completed_at,
              summary,
              metadata
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            on conflict (run_key) do update set
              model_registry_id = excluded.model_registry_id,
              checkpoint_type = excluded.checkpoint_type,
              run_status = excluded.run_status,
              season_from = excluded.season_from,
              season_to = excluded.season_to,
              sample_size = excluded.sample_size,
              log_loss = excluded.log_loss,
              brier_score = excluded.brier_score,
              calibration_error = excluded.calibration_error,
              started_at = excluded.started_at,
              completed_at = excluded.completed_at,
              summary = excluded.summary,
              metadata = excluded.metadata
            returning
              id,
              run_key,
              model_registry_id,
              checkpoint_type,
              run_status,
              season_from,
              season_to,
              sample_size,
              log_loss,
              brier_score,
              calibration_error,
              started_at,
              completed_at,
              summary,
              metadata
          )
          select
            upserted.id,
            upserted.run_key,
            upserted.model_registry_id,
            mr.model_key,
            upserted.checkpoint_type,
            upserted.run_status,
            upserted.season_from,
            upserted.season_to,
            upserted.sample_size,
            upserted.log_loss,
            upserted.brier_score,
            upserted.calibration_error,
            upserted.started_at,
            upserted.completed_at,
            upserted.summary,
            upserted.metadata
          from upserted
          join model_registry mr on mr.id = upserted.model_registry_id
        `,
        [
          backtest.runKey,
          modelRegistry.id,
          backtest.checkpointType,
          backtest.runStatus,
          backtest.seasonFrom,
          backtest.seasonTo,
          backtest.sampleSize,
          backtest.logLoss,
          backtest.brierScore,
          backtest.calibrationError,
          backtest.startedAt,
          backtest.completedAt,
          backtest.summary,
          backtest.metadata,
        ],
      );

      return mapBacktestRow(result.rows[0] as BacktestRow);
    },
  };
}

async function loadModelRegistryByKey(
  executor: SqlExecutor,
  modelKey: string,
): Promise<ResolvedModelRegistryRow> {
  const result = await executor.query<ResolvedModelRegistryRow>(
    `
      select id, model_key, checkpoint_type
      from model_registry
      where model_key = $1
    `,
    [modelKey],
  );

  const row = result.rows[0] as ResolvedModelRegistryRow | undefined;
  if (row === undefined) {
    throw new DomainValidationError([
      { path: "modelScore.modelKey", message: "model registry not found" },
    ]);
  }

  return row;
}

async function loadScoringRunByKey(
  executor: SqlExecutor,
  runKey: string,
): Promise<ResolvedScoringRunRow> {
  const result = await executor.query<ResolvedScoringRunRow>(
    `
      select id, run_key, checkpoint_type
      from scoring_runs
      where run_key = $1
    `,
    [runKey],
  );

  const row = result.rows[0] as ResolvedScoringRunRow | undefined;
  if (row === undefined) {
    throw new DomainValidationError([
      { path: "modelScore.scoringRunKey", message: "scoring run not found" },
    ]);
  }

  return row;
}

async function loadCheckpointStateById(
  executor: SqlExecutor,
  checkpointStateId: number,
): Promise<ResolvedCheckpointStateRow> {
  const result = await executor.query<CheckpointStateRow>(
    `
      select
        cs.id,
        cs.canonical_match_id,
        cm.match_slug,
        cs.checkpoint_type,
        cs.snapshot_time,
        cs.state_version,
        cs.source_market_snapshot_id,
        cs.source_cricket_snapshot_id,
        cs.innings_number,
        cs.batting_team_name,
        cs.bowling_team_name,
        cs.runs,
        cs.wickets,
        cs.overs,
        cs.target_runs,
        cs.current_run_rate,
        cs.required_run_rate,
        cs.state_payload,
        cs.created_at
      from checkpoint_states cs
      join canonical_matches cm on cm.id = cs.canonical_match_id
      where cs.id = $1
    `,
    [checkpointStateId],
  );

  const row = result.rows[0] as ResolvedCheckpointStateRow | undefined;
  if (row === undefined) {
    throw new DomainValidationError([
      {
        path: "modelScore.checkpointStateId",
        message: "checkpoint state not found",
      },
    ]);
  }

  return row;
}

function mapModelRegistryRow(row: ModelRegistryRow): ModelRegistryRecord {
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

function mapScoringRunRow(row: ScoringRunRow): ScoringRunRecord {
  return {
    id: Number(row.id),
    runKey: row.run_key,
    checkpointType: row.checkpoint_type,
    runStatus: row.run_status,
    triggeredBy: row.triggered_by,
    startedAt: row.started_at.toISOString(),
    completedAt:
      row.completed_at === null ? null : row.completed_at.toISOString(),
    inputSnapshotTime:
      row.input_snapshot_time === null
        ? null
        : row.input_snapshot_time.toISOString(),
    notes: row.notes,
    metadata: row.metadata,
  };
}

function mapModelScoreRow(
  row: ModelScoreRow,
  matchSlug: string,
): ModelScoreRecord {
  return {
    id: Number(row.id),
    scoringRunId: Number(row.scoring_run_id),
    scoringRunKey: row.scoring_run_key,
    modelRegistryId: Number(row.model_registry_id),
    modelKey: row.model_key,
    canonicalMatchId: Number(row.canonical_match_id),
    matchSlug,
    checkpointStateId: Number(row.checkpoint_state_id),
    checkpointType: row.checkpoint_type,
    sourceMarketSnapshotId:
      row.source_market_snapshot_id === null
        ? null
        : Number(row.source_market_snapshot_id),
    sourceCricketSnapshotId:
      row.source_cricket_snapshot_id === null
        ? null
        : Number(row.source_cricket_snapshot_id),
    fairWinProbability: Number(row.fair_win_probability),
    marketImpliedProbability:
      row.market_implied_probability === null
        ? null
        : Number(row.market_implied_probability),
    edge: row.edge === null ? null : Number(row.edge),
    scoredAt: row.scored_at.toISOString(),
    scorePayload: row.score_payload,
  };
}

function mapBacktestRow(row: BacktestRow): BacktestRecord {
  return {
    id: Number(row.id),
    runKey: row.run_key,
    modelRegistryId: Number(row.model_registry_id),
    modelKey: row.model_key,
    checkpointType: row.checkpoint_type,
    runStatus: row.run_status,
    seasonFrom: row.season_from,
    seasonTo: row.season_to,
    sampleSize: row.sample_size,
    logLoss: row.log_loss === null ? null : Number(row.log_loss),
    brierScore: row.brier_score === null ? null : Number(row.brier_score),
    calibrationError:
      row.calibration_error === null ? null : Number(row.calibration_error),
    startedAt: row.started_at.toISOString(),
    completedAt:
      row.completed_at === null ? null : row.completed_at.toISOString(),
    summary: row.summary,
    metadata: row.metadata,
  };
}
