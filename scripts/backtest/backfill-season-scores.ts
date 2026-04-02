import { fileURLToPath } from "node:url";

import { loadAppConfig } from "../../src/config/index.js";
import {
  parseCanonicalCheckpoint,
  type CheckpointType,
} from "../../src/domain/checkpoint.js";
import type { CanonicalMatch } from "../../src/domain/match.js";
import { parseMarketSnapshot } from "../../src/domain/market.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/pre-match.js";
import { createModelingRepository } from "../../src/repositories/modeling.js";
import { createNormalizedRepository } from "../../src/repositories/normalized.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";
import { createRawSnapshotRepository } from "../../src/repositories/raw.js";
import { scoreAndPersistPreMatchValuation } from "../../src/scoring/pre-match.js";
import { scorePostTossValuation } from "../../src/scoring/post-toss.js";
import type { MarketMatchMappingRecord } from "../../src/repositories/matching.js";
import type { JsonObject } from "../../src/domain/primitives.js";

interface CliOptions {
  season: number;
  preModelKey: string;
  postModelKey: string;
  runKeyPrefix: string;
}

interface CanonicalMatchRow {
  id: number;
  match_slug: string;
  source_match_id: string | null;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  status: string;
  toss_winner_team_name: string | null;
  toss_decision: "bat" | "bowl" | null;
  winning_team_name: string | null;
  result_type: string | null;
}

interface CheckpointStateInsert {
  canonicalMatchId: number;
  checkpointType: CheckpointType;
  snapshotTime: string;
  stateVersion: number;
  sourceMarketSnapshotId: number | null;
  sourceCricketSnapshotId: number | null;
  inningsNumber: number | null;
  battingTeamName: string | null;
  bowlingTeamName: string | null;
  runs: number | null;
  wickets: number | null;
  overs: number | null;
  targetRuns: number | null;
  currentRunRate: number | null;
  requiredRunRate: number | null;
  statePayload: JsonObject;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const modeling = createModelingRepository(pool);
    const normalized = createNormalizedRepository(pool);
    const raw = createRawSnapshotRepository(pool);

    const preModel = await loadModelMetadata(
      pool,
      options.preModelKey,
      "pre_match",
    );
    const postModel = await loadModelMetadata(
      pool,
      options.postModelKey,
      "post_toss",
    );

    const matches = await loadCompletedMatchesForSeason(pool, options.season);
    if (matches.length === 0) {
      throw new Error(
        `No completed win matches found for season ${options.season}.`,
      );
    }

    const startedAt = new Date().toISOString();
    const preRunKey = `${options.runKeyPrefix}-pre_match-${options.season}`;
    const postRunKey = `${options.runKeyPrefix}-post_toss-${options.season}`;

    await modeling.saveScoringRun({
      runKey: preRunKey,
      checkpointType: "pre_match",
      runStatus: "running",
      triggeredBy: "historical-backfill",
      startedAt,
      completedAt: null,
      inputSnapshotTime: null,
      notes: `historical backfill pre_match season ${options.season}`,
      metadata: { source: "backfill-season-scores" },
    });

    await modeling.saveScoringRun({
      runKey: postRunKey,
      checkpointType: "post_toss",
      runStatus: "running",
      triggeredBy: "historical-backfill",
      startedAt,
      completedAt: null,
      inputSnapshotTime: null,
      notes: `historical backfill post_toss season ${options.season}`,
      metadata: { source: "backfill-season-scores" },
    });

    let preScored = 0;
    let postScored = 0;
    let postSkippedMissingToss = 0;

    try {
      for (const matchRow of matches) {
        const scheduledStart = matchRow.scheduled_start;
        const preSnapshotTime = new Date(
          scheduledStart.getTime() - 30 * 60 * 1000,
        ).toISOString();
        const postSnapshotTime = new Date(
          scheduledStart.getTime() - 5 * 60 * 1000,
        ).toISOString();

        const sourceMarketId = `hist-${matchRow.match_slug}-winner`;
        const marketSnapshot = await raw.saveMarketSnapshot(
          parseMarketSnapshot({
            competition: "IPL",
            sourceMarketId,
            marketSlug: `${matchRow.match_slug}-winner`,
            eventSlug: matchRow.match_slug,
            snapshotTime: preSnapshotTime,
            marketStatus: "closed",
            yesOutcomeName: matchRow.team_a_name,
            noOutcomeName: matchRow.team_b_name,
            outcomeProbabilities: {
              yes: 0.5,
              no: 0.5,
            },
            lastTradedPrice: 0.5,
            liquidity: 0,
            payload: {
              source: "historical_backfill",
              season: options.season,
            },
          }),
        );

        const canonicalForPre = buildCheckpointMatch(matchRow, "pre_match");
        const preCheckpoint = parseCanonicalCheckpoint({
          checkpointType: "pre_match",
          match: canonicalForPre,
          state: {
            matchSlug: canonicalForPre.matchSlug,
            checkpointType: "pre_match",
            snapshotTime: preSnapshotTime,
            stateVersion: 1,
            sourceMarketSnapshotId: marketSnapshot.id,
            sourceCricketSnapshotId: null,
            inningsNumber: null,
            battingTeamName: null,
            bowlingTeamName: null,
            runs: null,
            wickets: null,
            overs: null,
            targetRuns: null,
            currentRunRate: null,
            requiredRunRate: null,
            statePayload: {
              source: "historical_backfill",
            },
          },
        });

        const preStateId = await saveCheckpointState(pool, {
          canonicalMatchId: matchRow.id,
          checkpointType: "pre_match",
          snapshotTime: preCheckpoint.state.snapshotTime,
          stateVersion: preCheckpoint.state.stateVersion,
          sourceMarketSnapshotId: preCheckpoint.state.sourceMarketSnapshotId,
          sourceCricketSnapshotId: preCheckpoint.state.sourceCricketSnapshotId,
          inningsNumber: preCheckpoint.state.inningsNumber,
          battingTeamName: preCheckpoint.state.battingTeamName,
          bowlingTeamName: preCheckpoint.state.bowlingTeamName,
          runs: preCheckpoint.state.runs,
          wickets: preCheckpoint.state.wickets,
          overs: preCheckpoint.state.overs,
          targetRuns: preCheckpoint.state.targetRuns,
          currentRunRate: preCheckpoint.state.currentRunRate,
          requiredRunRate: preCheckpoint.state.requiredRunRate,
          statePayload: preCheckpoint.state.statePayload,
        });
        const preContext = await buildFeatureContextFromHistory(
          pool,
          new Date(matchRow.scheduled_start.toISOString()),
        );
        const preFeatureRow = buildBaselinePreMatchFeatureRow(
          preCheckpoint,
          preContext,
        );
        await normalized.saveFeatureRow(preStateId, preFeatureRow);

        const mapping = buildResolvedMapping({
          sourceMarketId,
          sourceMarketSnapshotId: marketSnapshot.id,
          canonicalMatchId: matchRow.id,
          matchSlug: matchRow.match_slug,
        });

        await scoreAndPersistPreMatchValuation({
          modelingRepository: modeling,
          mapping,
          marketSnapshot,
          featureRow: preFeatureRow,
          checkpointStateId: preStateId,
          scoringRunKey: preRunKey,
          modelKey: options.preModelKey,
          modelMetadata: preModel.metadata,
          scoredAt: preFeatureRow.generatedAt,
        });
        preScored += 1;

        if (
          matchRow.toss_winner_team_name === null ||
          matchRow.toss_decision === null
        ) {
          postSkippedMissingToss += 1;
          continue;
        }

        const postMarketSnapshot = await raw.saveMarketSnapshot(
          parseMarketSnapshot({
            competition: "IPL",
            sourceMarketId,
            marketSlug: `${matchRow.match_slug}-winner`,
            eventSlug: matchRow.match_slug,
            snapshotTime: postSnapshotTime,
            marketStatus: "closed",
            yesOutcomeName: matchRow.team_a_name,
            noOutcomeName: matchRow.team_b_name,
            outcomeProbabilities: {
              yes: 0.5,
              no: 0.5,
            },
            lastTradedPrice: 0.5,
            liquidity: 0,
            payload: {
              source: "historical_backfill",
              season: options.season,
              stage: "post_toss",
            },
          }),
        );

        const canonicalForPost = buildCheckpointMatch(matchRow, "post_toss");
        const postCheckpoint = parseCanonicalCheckpoint({
          checkpointType: "post_toss",
          match: canonicalForPost,
          state: {
            matchSlug: canonicalForPost.matchSlug,
            checkpointType: "post_toss",
            snapshotTime: postSnapshotTime,
            stateVersion: 1,
            sourceMarketSnapshotId: postMarketSnapshot.id,
            sourceCricketSnapshotId: null,
            inningsNumber: null,
            battingTeamName: null,
            bowlingTeamName: null,
            runs: null,
            wickets: null,
            overs: null,
            targetRuns: null,
            currentRunRate: null,
            requiredRunRate: null,
            statePayload: {
              source: "historical_backfill",
            },
          },
        });

        const postStateId = await saveCheckpointState(pool, {
          canonicalMatchId: matchRow.id,
          checkpointType: "post_toss",
          snapshotTime: postCheckpoint.state.snapshotTime,
          stateVersion: postCheckpoint.state.stateVersion,
          sourceMarketSnapshotId: postCheckpoint.state.sourceMarketSnapshotId,
          sourceCricketSnapshotId: postCheckpoint.state.sourceCricketSnapshotId,
          inningsNumber: postCheckpoint.state.inningsNumber,
          battingTeamName: postCheckpoint.state.battingTeamName,
          bowlingTeamName: postCheckpoint.state.bowlingTeamName,
          runs: postCheckpoint.state.runs,
          wickets: postCheckpoint.state.wickets,
          overs: postCheckpoint.state.overs,
          targetRuns: postCheckpoint.state.targetRuns,
          currentRunRate: postCheckpoint.state.currentRunRate,
          requiredRunRate: postCheckpoint.state.requiredRunRate,
          statePayload: postCheckpoint.state.statePayload,
        });
        const postResult = scorePostTossValuation({
          checkpoint: postCheckpoint,
          featureContext: preContext,
          marketImpliedProbability: 0.5,
          evaluatedAt: postSnapshotTime,
          modelMetadata: postModel.metadata,
        });
        await normalized.saveFeatureRow(postStateId, postResult.featureRow);

        await modeling.saveModelScore({
          checkpointStateId: postStateId,
          checkpointType: "post_toss",
          scoringRunKey: postRunKey,
          modelKey: options.postModelKey,
          fairWinProbability: postResult.valuation.fairWinProbability,
          marketImpliedProbability:
            postResult.valuation.marketImpliedProbability,
          edge: postResult.valuation.edge,
          scoredAt: postResult.valuation.evaluatedAt,
          scorePayload: postResult.valuation.valuationPayload,
        });
        postScored += 1;
      }

      const completedAt = new Date().toISOString();
      await modeling.saveScoringRun({
        runKey: preRunKey,
        checkpointType: "pre_match",
        runStatus: "succeeded",
        triggeredBy: "historical-backfill",
        startedAt,
        completedAt,
        inputSnapshotTime: null,
        notes: `historical backfill pre_match season ${options.season}`,
        metadata: {
          source: "backfill-season-scores",
          scoredRows: preScored,
        },
      });

      await modeling.saveScoringRun({
        runKey: postRunKey,
        checkpointType: "post_toss",
        runStatus: "succeeded",
        triggeredBy: "historical-backfill",
        startedAt,
        completedAt,
        inputSnapshotTime: null,
        notes: `historical backfill post_toss season ${options.season}`,
        metadata: {
          source: "backfill-season-scores",
          scoredRows: postScored,
          skippedMissingToss: postSkippedMissingToss,
        },
      });

      process.stdout.write(
        `${JSON.stringify(
          {
            season: options.season,
            preModelKey: options.preModelKey,
            postModelKey: options.postModelKey,
            preScored,
            postScored,
            postSkippedMissingToss,
            preRunKey,
            postRunKey,
          },
          null,
          2,
        )}\n`,
      );
    } catch (error) {
      const failedAt = new Date().toISOString();
      await modeling.saveScoringRun({
        runKey: preRunKey,
        checkpointType: "pre_match",
        runStatus: "failed",
        triggeredBy: "historical-backfill",
        startedAt,
        completedAt: failedAt,
        inputSnapshotTime: null,
        notes: `historical backfill pre_match season ${options.season} failed`,
        metadata: {
          source: "backfill-season-scores",
          scoredRows: preScored,
          failed: true,
        },
      });
      await modeling.saveScoringRun({
        runKey: postRunKey,
        checkpointType: "post_toss",
        runStatus: "failed",
        triggeredBy: "historical-backfill",
        startedAt,
        completedAt: failedAt,
        inputSnapshotTime: null,
        notes: `historical backfill post_toss season ${options.season} failed`,
        metadata: {
          source: "backfill-season-scores",
          scoredRows: postScored,
          skippedMissingToss: postSkippedMissingToss,
          failed: true,
        },
      });
      throw error;
    }
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season = 2025;
  let preModelKey = "baseline-pre-match-runtime-calibrated";
  let postModelKey = "baseline-post-toss-v1";
  let runKeyPrefix = "backfill-historical-scores";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--season") {
      season = parseIntegerArg("--season", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--pre-model-key") {
      preModelKey = readStringArg("--pre-model-key", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--post-model-key") {
      postModelKey = readStringArg("--post-model-key", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--run-key-prefix") {
      runKeyPrefix = readStringArg("--run-key-prefix", argv[index + 1]);
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season, --pre-model-key, --post-model-key, --run-key-prefix.`,
    );
  }

  return {
    season,
    preModelKey,
    postModelKey,
    runKeyPrefix,
  };
}

async function loadModelMetadata(
  pool: ReturnType<typeof createPgPool>,
  modelKey: string,
  checkpointType: CheckpointType,
): Promise<{ metadata: JsonObject }> {
  const result = await pool.query<{ metadata: JsonObject }>(
    `
      select metadata
      from model_registry
      where model_key = $1
        and checkpoint_type = $2
      order by created_at desc, id desc
      limit 1
    `,
    [modelKey, checkpointType],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new Error(
      `Model key ${modelKey} for checkpoint ${checkpointType} was not found in model_registry.`,
    );
  }

  return {
    metadata: row.metadata,
  };
}

async function loadCompletedMatchesForSeason(
  pool: ReturnType<typeof createPgPool>,
  season: number,
): Promise<CanonicalMatchRow[]> {
  const result = await pool.query<CanonicalMatchRow>(
    `
      select
        id,
        match_slug,
        source_match_id,
        season,
        scheduled_start,
        team_a_name,
        team_b_name,
        venue_name,
        status,
        toss_winner_team_name,
        toss_decision,
        winning_team_name,
        result_type
      from canonical_matches
      where season = $1
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
      order by scheduled_start asc, id asc
    `,
    [season],
  );

  return result.rows;
}

function buildCheckpointMatch(
  row: CanonicalMatchRow,
  checkpointType: "pre_match" | "post_toss",
): CanonicalMatch {
  if (checkpointType === "pre_match") {
    return {
      competition: "IPL",
      matchSlug: row.match_slug,
      sourceMatchId: row.source_match_id,
      season: row.season,
      scheduledStart: row.scheduled_start.toISOString(),
      teamAName: row.team_a_name,
      teamBName: row.team_b_name,
      venueName: row.venue_name,
      status: "scheduled",
      tossWinnerTeamName: null,
      tossDecision: null,
      winningTeamName: null,
      resultType: null,
    };
  }

  return {
    competition: "IPL",
    matchSlug: row.match_slug,
    sourceMatchId: row.source_match_id,
    season: row.season,
    scheduledStart: row.scheduled_start.toISOString(),
    teamAName: row.team_a_name,
    teamBName: row.team_b_name,
    venueName: row.venue_name,
    status: "in_progress",
    tossWinnerTeamName: row.toss_winner_team_name,
    tossDecision: row.toss_decision,
    winningTeamName: null,
    resultType: null,
  };
}

function buildResolvedMapping(input: {
  sourceMarketId: string;
  sourceMarketSnapshotId: number;
  canonicalMatchId: number;
  matchSlug: string;
}): MarketMatchMappingRecord {
  const now = new Date().toISOString();
  return {
    id: -1,
    competition: "IPL",
    sourceMarketId: input.sourceMarketId,
    sourceMarketSnapshotId: input.sourceMarketSnapshotId,
    canonicalMatchId: input.canonicalMatchId,
    matchSlug: input.matchSlug,
    mappingStatus: "resolved",
    confidence: 1,
    resolverVersion: "historical-backfill-v1",
    reason: "direct_match_slug_mapping",
    payload: {
      source: "backfill-season-scores",
    },
    createdAt: now,
    updatedAt: now,
  };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

function readStringArg(flag: string, value: string | undefined): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${flag} requires a non-empty value.`);
  }

  return value.trim();
}

async function saveCheckpointState(
  pool: ReturnType<typeof createPgPool>,
  input: CheckpointStateInsert,
): Promise<number> {
  const result = await pool.query<{ id: number }>(
    `
      insert into checkpoint_states (
        canonical_match_id,
        checkpoint_type,
        snapshot_time,
        state_version,
        source_market_snapshot_id,
        source_cricket_snapshot_id,
        innings_number,
        batting_team_name,
        bowling_team_name,
        runs,
        wickets,
        overs,
        target_runs,
        current_run_rate,
        required_run_rate,
        state_payload
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
      on conflict (canonical_match_id, checkpoint_type, snapshot_time, state_version) do update set
        source_market_snapshot_id = excluded.source_market_snapshot_id,
        source_cricket_snapshot_id = excluded.source_cricket_snapshot_id,
        innings_number = excluded.innings_number,
        batting_team_name = excluded.batting_team_name,
        bowling_team_name = excluded.bowling_team_name,
        runs = excluded.runs,
        wickets = excluded.wickets,
        overs = excluded.overs,
        target_runs = excluded.target_runs,
        current_run_rate = excluded.current_run_rate,
        required_run_rate = excluded.required_run_rate,
        state_payload = excluded.state_payload
      returning id
    `,
    [
      input.canonicalMatchId,
      input.checkpointType,
      input.snapshotTime,
      input.stateVersion,
      input.sourceMarketSnapshotId,
      input.sourceCricketSnapshotId,
      input.inningsNumber,
      input.battingTeamName,
      input.bowlingTeamName,
      input.runs,
      input.wickets,
      input.overs,
      input.targetRuns,
      input.currentRunRate,
      input.requiredRunRate,
      input.statePayload,
    ],
  );

  const id = result.rows[0]?.id;
  if (id === undefined) {
    throw new Error("Failed to persist checkpoint state row.");
  }

  return id;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
