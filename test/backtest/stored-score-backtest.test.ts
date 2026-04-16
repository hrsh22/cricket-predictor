import { randomUUID } from "node:crypto";

import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { migrateDatabase } from "../../database/migration-runner.js";
import { loadAppConfig } from "../../src/config/index.js";
import { runStoredScoreBacktest } from "../../src/backtest/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import {
  createRepositorySet,
  createPgPool,
} from "../../src/repositories/index.js";

interface BacktestSummaryRow {
  run_key: string;
  summary: unknown;
}

describe("stored score historical backtest", () => {
  const config = loadAppConfig();
  const repositories = createRepositorySet(config);
  const cleanupPool = createPgPool(config.databaseUrl);
  const createdSuffixes = new Set<string>();

  beforeAll(async () => {
    await migrateDatabase(config.databaseUrl);
  });

  afterEach(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    createdSuffixes.clear();
  });

  afterAll(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    await cleanupPool.end();
    await repositories.close();
  });

  it("loads persisted score rows, skips unsupported outcomes, and stores social regression summaries", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const modelKey = `baseline-pre-match-social-${suffix}`;
    const runKey = `backtest-${suffix}`;

    await repositories.modeling.saveModelRegistry({
      modelKey,
      checkpointType: "pre_match",
      modelFamily: "baseline-rating",
      version: `task17-${suffix}`,
      trainingWindow: "2019-2024",
      isActive: true,
      metadata: { source: "task-17-test" },
      createdAt: "2026-03-29T18:00:00.000Z",
    });

    await repositories.modeling.saveScoringRun({
      runKey: `run-2024-${suffix}`,
      checkpointType: "pre_match",
      runStatus: "succeeded",
      triggeredBy: "integration-test",
      startedAt: "2026-03-29T18:01:00.000Z",
      completedAt: "2026-03-29T18:01:30.000Z",
      inputSnapshotTime: "2024-04-01T12:00:00.000Z",
      notes: "task 17 2024 backfill",
      metadata: { season: 2024 },
    });

    await repositories.modeling.saveScoringRun({
      runKey: `run-2025-${suffix}`,
      checkpointType: "pre_match",
      runStatus: "succeeded",
      triggeredBy: "integration-test",
      startedAt: "2026-03-29T18:02:00.000Z",
      completedAt: "2026-03-29T18:02:30.000Z",
      inputSnapshotTime: "2025-04-01T12:00:00.000Z",
      notes: "task 17 2025 backfill",
      metadata: { season: 2025 },
    });

    await seedPreMatchScoreRow({
      repositories,
      suffix,
      modelKey,
      scoringRunKey: `run-2024-${suffix}`,
      matchLabel: "a",
      season: 2024,
      snapshotTime: "2024-04-01T12:00:00.000Z",
      scheduledStart: "2024-04-01T14:00:00.000Z",
      teamAName: "Chennai Super Kings",
      teamBName: "Mumbai Indians",
      winningTeamName: "Chennai Super Kings",
      resultType: "win",
      fairWinProbability: 0.74,
      structuredFairProbability: 0.7,
    });

    await seedPreMatchScoreRow({
      repositories,
      suffix,
      modelKey,
      scoringRunKey: `run-2024-${suffix}`,
      matchLabel: "b",
      season: 2024,
      snapshotTime: "2024-04-03T12:00:00.000Z",
      scheduledStart: "2024-04-03T14:00:00.000Z",
      teamAName: "Rajasthan Royals",
      teamBName: "Sunrisers Hyderabad",
      winningTeamName: "Sunrisers Hyderabad",
      resultType: "win",
      fairWinProbability: 0.34,
      structuredFairProbability: 0.3,
    });

    await seedPreMatchScoreRow({
      repositories,
      suffix,
      modelKey,
      scoringRunKey: `run-2025-${suffix}`,
      matchLabel: "c",
      season: 2025,
      snapshotTime: "2025-04-02T12:00:00.000Z",
      scheduledStart: "2025-04-02T14:00:00.000Z",
      teamAName: "Royal Challengers Bengaluru",
      teamBName: "Punjab Kings",
      winningTeamName: "Royal Challengers Bengaluru",
      resultType: "win",
      fairWinProbability: 0.56,
      structuredFairProbability: 0.74,
    });

    await seedPreMatchScoreRow({
      repositories,
      suffix,
      modelKey,
      scoringRunKey: `run-2025-${suffix}`,
      matchLabel: "d",
      season: 2025,
      snapshotTime: "2025-04-04T12:00:00.000Z",
      scheduledStart: "2025-04-04T14:00:00.000Z",
      teamAName: "Lucknow Super Giants",
      teamBName: "Gujarat Titans",
      winningTeamName: "Gujarat Titans",
      resultType: "win",
      fairWinProbability: 0.64,
      structuredFairProbability: 0.28,
    });

    await seedPreMatchScoreRow({
      repositories,
      suffix,
      modelKey,
      scoringRunKey: `run-2025-${suffix}`,
      matchLabel: "e",
      season: 2025,
      snapshotTime: "2025-04-06T12:00:00.000Z",
      scheduledStart: "2025-04-06T14:00:00.000Z",
      teamAName: "Delhi Capitals",
      teamBName: "Kolkata Knight Riders",
      winningTeamName: null,
      resultType: "no_result",
      fairWinProbability: 0.51,
      structuredFairProbability: 0.49,
      completedStatus: "completed",
    });

    const result = await runStoredScoreBacktest({
      executor: cleanupPool,
      modelingRepository: repositories.modeling,
      options: {
        modelKey,
        checkpointType: "pre_match",
        evaluationSeasonFrom: 2024,
        evaluationSeasonTo: 2025,
        calibrationBinCount: 4,
      },
      runKey,
      triggeredBy: "integration-test",
    });

    expect(result.folds).toHaveLength(1);
    expect(result.socialComparison.regressionDetected).toBe(true);
    expect(result.socialComparison.recommendation).toBe(
      "disable_social_by_default",
    );
    expect(result.trading).not.toBeNull();
    expect(result.trading?.marketSampleSize).toBe(2);
    expect(
      result.skippedRows.some(
        (row) => row.reason === "unsupported_match_result",
      ),
    ).toBe(true);

    const backtestResult = await cleanupPool.query<BacktestSummaryRow>(
      `
        select run_key, summary
        from backtests
        where run_key = $1
      `,
      [runKey],
    );

    expect(backtestResult.rows).toHaveLength(1);
    const summary = backtestResult.rows[0]?.summary;
    expect(summary).not.toBeNull();
    const summaryRecord = summary as Record<string, unknown>;
    const socialComparison = summaryRecord["socialComparison"] as Record<
      string,
      unknown
    >;
    const trading = summaryRecord["trading"] as Record<string, unknown>;
    expect(socialComparison["recommendation"]).toBe(
      "disable_social_by_default",
    );
    expect(socialComparison["regressionDetected"]).toBe(true);
    expect(trading["marketSampleSize"]).toBe(2);
  });

  async function cleanupSyntheticRows(
    suffixes: readonly string[],
  ): Promise<void> {
    if (suffixes.length === 0) {
      return;
    }

    await cleanupPool.query(
      `
        delete from model_scores ms
        using canonical_matches cm
        where ms.canonical_match_id = cm.id
          and right(cm.match_slug, 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from backtests
        where right(run_key, 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from scoring_runs
        where right(run_key, 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from model_registry
        where right(model_key, 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from canonical_matches
        where right(match_slug, 8) = any($1::text[])
           or right(coalesce(source_match_id, ''), 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from raw_market_snapshots
        where right(source_market_id, 8) = any($1::text[])
      `,
      [suffixes],
    );
  }
});

async function seedPreMatchScoreRow(input: {
  repositories: ReturnType<typeof createRepositorySet>;
  suffix: string;
  modelKey: string;
  scoringRunKey: string;
  matchLabel: string;
  season: number;
  snapshotTime: string;
  scheduledStart: string;
  teamAName: string;
  teamBName: string;
  winningTeamName: string | null;
  resultType: "win" | "no_result";
  fairWinProbability: number;
  structuredFairProbability: number;
  completedStatus?: "completed";
}): Promise<void> {
  const sourceMarketId = `market-${input.matchLabel}-${input.suffix}`;
  const sourceMatchId = `match-${input.matchLabel}-${input.suffix}`;
  const matchSlug = `ipl-${input.season}-${input.matchLabel}-${input.suffix}`;

  const marketSnapshot = await input.repositories.raw.saveMarketSnapshot({
    competition: "IPL",
    sourceMarketId,
    marketSlug: `${matchSlug}-winner`,
    eventSlug: matchSlug,
    snapshotTime: input.snapshotTime,
    marketStatus: "open",
    yesOutcomeName: input.teamAName,
    noOutcomeName: input.teamBName,
    outcomeProbabilities: {
      yes: 0.5,
      no: 0.5,
    },
    lastTradedPrice: 0.5,
    liquidity: 10000,
    payload: {
      source: "task-17-test",
    },
  });

  const checkpoint = await input.repositories.normalized.saveCheckpoint(
    parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: {
        competition: "IPL",
        matchSlug,
        sourceMatchId,
        season: input.season,
        scheduledStart: input.scheduledStart,
        teamAName: input.teamAName,
        teamBName: input.teamBName,
        venueName: "Test Venue",
        status: "scheduled",
        tossWinnerTeamName: null,
        tossDecision: null,
        winningTeamName: null,
        resultType: null,
      },
      state: {
        matchSlug,
        checkpointType: "pre_match",
        snapshotTime: input.snapshotTime,
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
          provider: "task-17-test",
        },
      },
    }),
  );

  await input.repositories.normalized.saveCanonicalMatch({
    competition: "IPL",
    matchSlug,
    sourceMatchId,
    season: input.season,
    scheduledStart: input.scheduledStart,
    teamAName: input.teamAName,
    teamBName: input.teamBName,
    venueName: "Test Venue",
    status: input.completedStatus ?? "completed",
    tossWinnerTeamName: null,
    tossDecision: null,
    winningTeamName: input.winningTeamName,
    resultType: input.resultType,
  });

  await input.repositories.modeling.saveModelScore({
    checkpointStateId: checkpoint.id,
    checkpointType: "pre_match",
    scoringRunKey: input.scoringRunKey,
    modelKey: input.modelKey,
    fairWinProbability: input.fairWinProbability,
    marketImpliedProbability: 0.5,
    edge: Number((input.fairWinProbability - 0.5).toFixed(6)),
    scoredAt: input.snapshotTime,
    scorePayload: {
      valuation: {
        structuredFairProbability: input.structuredFairProbability,
        fairWinProbability: input.fairWinProbability,
      },
      socialAdjustment: {
        mode: "enabled",
        boundedAdjustment: Number(
          (input.fairWinProbability - input.structuredFairProbability).toFixed(
            6,
          ),
        ),
      },
    },
  });
}
