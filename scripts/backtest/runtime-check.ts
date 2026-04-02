import { execFileSync } from "node:child_process";

import { migrateDatabase } from "../../database/migration-runner.js";
import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import {
  createRepositorySet,
  createPgPool,
} from "../../src/repositories/index.js";

async function main(): Promise<void> {
  const config = loadAppConfig();
  const repositories = createRepositorySet(config);
  const cleanupPool = createPgPool(config.databaseUrl);
  const suffix = "rtck1726";
  const modelKey = `baseline-pre-match-social-${suffix}`;
  const runKey = `backtest-${suffix}`;

  await migrateDatabase(config.databaseUrl);
  await cleanupSyntheticRows(cleanupPool, suffix);

  try {
    await repositories.modeling.saveModelRegistry({
      modelKey,
      checkpointType: "pre_match",
      modelFamily: "baseline-rating",
      version: `runtime-${suffix}`,
      trainingWindow: "2019-2024",
      isActive: true,
      metadata: { source: "runtime-check" },
      createdAt: "2026-03-29T19:00:00.000Z",
    });

    await repositories.modeling.saveScoringRun({
      runKey: `run-2024-${suffix}`,
      checkpointType: "pre_match",
      runStatus: "succeeded",
      triggeredBy: "runtime-check",
      startedAt: "2026-03-29T19:01:00.000Z",
      completedAt: "2026-03-29T19:01:30.000Z",
      inputSnapshotTime: "2024-04-01T12:00:00.000Z",
      notes: "runtime check 2024",
      metadata: { season: 2024 },
    });

    await repositories.modeling.saveScoringRun({
      runKey: `run-2025-${suffix}`,
      checkpointType: "pre_match",
      runStatus: "succeeded",
      triggeredBy: "runtime-check",
      startedAt: "2026-03-29T19:02:00.000Z",
      completedAt: "2026-03-29T19:02:30.000Z",
      inputSnapshotTime: "2025-04-01T12:00:00.000Z",
      notes: "runtime check 2025",
      metadata: { season: 2025 },
    });

    await seedRow({
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
      fairWinProbability: 0.74,
      structuredFairProbability: 0.7,
    });

    await seedRow({
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
      fairWinProbability: 0.34,
      structuredFairProbability: 0.3,
    });

    await seedRow({
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
      fairWinProbability: 0.56,
      structuredFairProbability: 0.74,
    });

    await seedRow({
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
      fairWinProbability: 0.64,
      structuredFairProbability: 0.28,
    });

    const output = execFileSync(
      "pnpm",
      [
        "tsx",
        "scripts/backtest/run.ts",
        "--model-key",
        modelKey,
        "--checkpoint",
        "pre_match",
        "--season-from",
        "2024",
        "--season-to",
        "2025",
        "--bin-count",
        "4",
        "--run-key",
        runKey,
      ],
      {
        cwd: process.cwd(),
        encoding: "utf8",
      },
    );

    process.stdout.write(output);
  } finally {
    await cleanupSyntheticRows(cleanupPool, suffix);
    await repositories.close();
    await cleanupPool.end();
  }
}

async function cleanupSyntheticRows(
  pool: ReturnType<typeof createPgPool>,
  suffix: string,
): Promise<void> {
  await pool.query(
    `
      delete from model_scores ms
      using canonical_matches cm
      where ms.canonical_match_id = cm.id
        and right(cm.match_slug, 8) = $1
    `,
    [suffix],
  );

  await pool.query(
    `
      delete from backtests
      where right(run_key, 8) = $1
    `,
    [suffix],
  );

  await pool.query(
    `
      delete from scoring_runs
      where right(run_key, 8) = $1
    `,
    [suffix],
  );

  await pool.query(
    `
      delete from model_registry
      where right(model_key, 8) = $1
    `,
    [suffix],
  );

  await pool.query(
    `
      delete from canonical_matches
      where right(match_slug, 8) = $1
         or right(coalesce(source_match_id, ''), 8) = $1
    `,
    [suffix],
  );

  await pool.query(
    `
      delete from raw_market_snapshots
      where right(source_market_id, 8) = $1
    `,
    [suffix],
  );
}

async function seedRow(input: {
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
  winningTeamName: string;
  fairWinProbability: number;
  structuredFairProbability: number;
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
      source: "runtime-check",
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
        venueName: "Runtime Check Venue",
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
          source: "runtime-check",
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
    venueName: "Runtime Check Venue",
    status: "completed",
    tossWinnerTeamName: null,
    tossDecision: null,
    winningTeamName: input.winningTeamName,
    resultType: "win",
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

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Backtest runtime check failed: ${message}`);
  process.exitCode = 1;
});
