import { randomUUID } from "node:crypto";

import { describe, expect, it, beforeAll, afterAll, afterEach } from "vitest";

import { migrateDatabase } from "../../database/migration-runner.js";
import { loadAppConfig } from "../../src/config/index.js";
import {
  createPgPool,
  createRepositorySet,
} from "../../src/repositories/index.js";

describe("postgres repositories", () => {
  const config = loadAppConfig();
  const repositories = createRepositorySet(config);
  const cleanupPool = createPgPool(config.databaseUrl);
  const createdSuffixes = new Set<string>();

  beforeAll(async () => {
    await migrateDatabase(config.databaseUrl);
  });

  afterAll(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    await cleanupPool.end();
    await repositories.close();
  });

  afterEach(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    createdSuffixes.clear();
  });

  it("round-trips raw snapshots, normalized writes, and read models", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const marketSnapshot = await repositories.raw.saveMarketSnapshot({
      competition: "IPL",
      sourceMarketId: `market-${suffix}`,
      marketSlug: `ipl-2026-roundtrip-${suffix}`,
      eventSlug: `ipl-2026-event-${suffix}`,
      snapshotTime: "2026-03-29T13:20:00.000Z",
      marketStatus: "open",
      yesOutcomeName: "Chennai Super Kings",
      noOutcomeName: "Mumbai Indians",
      outcomeProbabilities: { yes: 0.54, no: 0.46 },
      lastTradedPrice: 0.53,
      liquidity: 42000,
      payload: { tokenId: `token-${suffix}` },
    });

    const cricketSnapshot = await repositories.raw.saveCricketSnapshot({
      provider: "espncricinfo",
      sourceMatchId: `match-${suffix}`,
      snapshotTime: "2026-03-29T13:21:00.000Z",
      matchStatus: "in_progress",
      inningsNumber: 1,
      overNumber: 3.2,
      payload: { score: "24/1" },
    });

    expect(marketSnapshot.sourceMarketId).toBe(`market-${suffix}`);
    expect(cricketSnapshot.provider).toBe("espncricinfo");

    const canonicalMatch = await repositories.normalized.saveCanonicalMatch({
      competition: "IPL",
      matchSlug: `ipl-2026-match-${suffix}`,
      sourceMatchId: cricketSnapshot.sourceMatchId,
      season: 2026,
      scheduledStart: "2026-03-29T13:00:00.000Z",
      teamAName: "Chennai Super Kings",
      teamBName: "Mumbai Indians",
      venueName: "Chepauk",
      status: "in_progress",
      tossWinnerTeamName: "Chennai Super Kings",
      tossDecision: "bat",
      winningTeamName: null,
      resultType: null,
    });

    const checkpoint = await repositories.normalized.saveCheckpoint({
      checkpointType: "post_toss",
      match: {
        competition: "IPL",
        matchSlug: canonicalMatch.matchSlug,
        sourceMatchId: canonicalMatch.sourceMatchId,
        season: canonicalMatch.season,
        scheduledStart: canonicalMatch.scheduledStart,
        teamAName: canonicalMatch.teamAName,
        teamBName: canonicalMatch.teamBName,
        venueName: canonicalMatch.venueName,
        status: canonicalMatch.status,
        tossWinnerTeamName: canonicalMatch.tossWinnerTeamName,
        tossDecision: canonicalMatch.tossDecision,
        winningTeamName: canonicalMatch.winningTeamName,
        resultType: canonicalMatch.resultType,
      },
      state: {
        matchSlug: canonicalMatch.matchSlug,
        checkpointType: "post_toss",
        snapshotTime: "2026-03-29T13:25:00.000Z",
        stateVersion: 1,
        sourceMarketSnapshotId: marketSnapshot.id,
        sourceCricketSnapshotId: cricketSnapshot.id,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload: { toss: "bat" },
      },
    });

    const featureRow = await repositories.normalized.saveFeatureRow(
      checkpoint.id,
      {
        matchSlug: canonicalMatch.matchSlug,
        checkpointType: "post_toss",
        featureSetVersion: "v1",
        generatedAt: "2026-03-29T13:26:00.000Z",
        features: { baselineRating: 0.61 },
      },
    );

    const readModel = await repositories.read.getMatchReadModel(
      canonicalMatch.matchSlug,
    );

    expect(readModel).not.toBeNull();
    expect(readModel?.match.matchSlug).toBe(canonicalMatch.matchSlug);
    expect(readModel?.checkpointStates).toHaveLength(1);
    expect(readModel?.checkpointStates[0]?.statePayload).toEqual({
      toss: "bat",
    });
    expect(readModel?.featureRows).toHaveLength(1);
    expect(readModel?.featureRows[0]?.features).toEqual({
      baselineRating: 0.61,
    });
    expect(featureRow.checkpointStateId).toBe(checkpoint.id);
  });

  it("persists model lineage through registry, runs, scores, and backtests", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const marketSnapshot = await repositories.raw.saveMarketSnapshot({
      competition: "IPL",
      sourceMarketId: `market-lineage-${suffix}`,
      marketSlug: `ipl-2026-lineage-${suffix}`,
      eventSlug: `ipl-2026-event-lineage-${suffix}`,
      snapshotTime: "2026-03-29T14:00:00.000Z",
      marketStatus: "open",
      yesOutcomeName: "Royal Challengers Bengaluru",
      noOutcomeName: "Kolkata Knight Riders",
      outcomeProbabilities: { yes: 0.47, no: 0.53 },
      lastTradedPrice: 0.48,
      liquidity: 21000,
      payload: { tokenId: `token-lineage-${suffix}` },
    });

    const cricketSnapshot = await repositories.raw.saveCricketSnapshot({
      provider: "espncricinfo",
      sourceMatchId: `match-lineage-${suffix}`,
      snapshotTime: "2026-03-29T14:01:00.000Z",
      matchStatus: "in_progress",
      inningsNumber: 1,
      overNumber: 7.4,
      payload: { score: "58/2" },
    });

    const canonicalMatch = await repositories.normalized.saveCanonicalMatch({
      competition: "IPL",
      matchSlug: `ipl-2026-lineage-match-${suffix}`,
      sourceMatchId: cricketSnapshot.sourceMatchId,
      season: 2026,
      scheduledStart: "2026-03-29T13:30:00.000Z",
      teamAName: "Royal Challengers Bengaluru",
      teamBName: "Kolkata Knight Riders",
      venueName: "Chinnaswamy",
      status: "in_progress",
      tossWinnerTeamName: "Royal Challengers Bengaluru",
      tossDecision: "bat",
      winningTeamName: null,
      resultType: null,
    });

    const checkpoint = await repositories.normalized.saveCheckpoint({
      checkpointType: "innings_break",
      match: {
        competition: "IPL",
        matchSlug: canonicalMatch.matchSlug,
        sourceMatchId: canonicalMatch.sourceMatchId,
        season: canonicalMatch.season,
        scheduledStart: canonicalMatch.scheduledStart,
        teamAName: canonicalMatch.teamAName,
        teamBName: canonicalMatch.teamBName,
        venueName: canonicalMatch.venueName,
        status: canonicalMatch.status,
        tossWinnerTeamName: canonicalMatch.tossWinnerTeamName,
        tossDecision: canonicalMatch.tossDecision,
        winningTeamName: canonicalMatch.winningTeamName,
        resultType: canonicalMatch.resultType,
      },
      state: {
        matchSlug: canonicalMatch.matchSlug,
        checkpointType: "innings_break",
        snapshotTime: "2026-03-29T14:10:00.000Z",
        stateVersion: 1,
        sourceMarketSnapshotId: marketSnapshot.id,
        sourceCricketSnapshotId: cricketSnapshot.id,
        inningsNumber: 1,
        battingTeamName: "Royal Challengers Bengaluru",
        bowlingTeamName: "Kolkata Knight Riders",
        runs: 58,
        wickets: 2,
        overs: 7.4,
        targetRuns: 0,
        currentRunRate: 7.84,
        requiredRunRate: 0,
        statePayload: { phase: "innings_break" },
      },
    });

    const registry = await repositories.modeling.saveModelRegistry({
      modelKey: `baseline-pre-match-${suffix}`,
      checkpointType: "innings_break",
      modelFamily: "baseline-rating",
      version: "v1",
      trainingWindow: "2022-2025",
      isActive: true,
      metadata: { source: "task-13" },
      createdAt: "2026-03-29T14:12:00.000Z",
    });

    const scoringRun = await repositories.modeling.saveScoringRun({
      runKey: `run-${suffix}`,
      checkpointType: "innings_break",
      runStatus: "running",
      triggeredBy: "manual",
      startedAt: "2026-03-29T14:12:00.000Z",
      completedAt: null,
      inputSnapshotTime: "2026-03-29T14:10:00.000Z",
      notes: "lineage smoke test",
      metadata: { checkpointStateId: checkpoint.id },
    });

    const score = await repositories.modeling.saveModelScore({
      checkpointStateId: checkpoint.id,
      checkpointType: "innings_break",
      scoringRunKey: scoringRun.runKey,
      modelKey: registry.modelKey,
      fairWinProbability: 0.64,
      marketImpliedProbability: 0.53,
      edge: 0.11,
      scoredAt: "2026-03-29T14:13:00.000Z",
      scorePayload: {
        modelVersion: registry.version,
        source: "integration-test",
      },
    });

    const backtest = await repositories.modeling.saveBacktest({
      runKey: `backtest-${suffix}`,
      modelKey: registry.modelKey,
      checkpointType: "innings_break",
      runStatus: "succeeded",
      seasonFrom: 2022,
      seasonTo: 2025,
      sampleSize: 184,
      logLoss: 0.4721,
      brierScore: 0.1688,
      calibrationError: 0.031,
      startedAt: "2026-03-29T14:14:00.000Z",
      completedAt: "2026-03-29T14:15:00.000Z",
      summary: { folds: 4 },
      metadata: { datasetSlice: "ipl-2022-2025" },
    });

    expect(registry.createdAt).toBe("2026-03-29T14:12:00.000Z");
    expect(scoringRun.id).toBeGreaterThan(0);
    expect(score.modelKey).toBe(registry.modelKey);
    expect(score.scoringRunKey).toBe(scoringRun.runKey);
    expect(score.matchSlug).toBe(canonicalMatch.matchSlug);
    expect(score.sourceMarketSnapshotId).toBe(marketSnapshot.id);
    expect(score.sourceCricketSnapshotId).toBe(cricketSnapshot.id);
    expect(score.checkpointStateId).toBe(checkpoint.id);
    expect(backtest.modelKey).toBe(registry.modelKey);
    expect(backtest.modelRegistryId).toBe(registry.id);
  });

  it("rejects score writes without a known model registry", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const cricketSnapshot = await repositories.raw.saveCricketSnapshot({
      provider: "espncricinfo",
      sourceMatchId: `match-invalid-${suffix}`,
      snapshotTime: "2026-03-29T15:00:00.000Z",
      matchStatus: "in_progress",
      inningsNumber: 1,
      overNumber: 9.1,
      payload: { score: "72/3" },
    });

    const canonicalMatch = await repositories.normalized.saveCanonicalMatch({
      competition: "IPL",
      matchSlug: `ipl-2026-invalid-match-${suffix}`,
      sourceMatchId: cricketSnapshot.sourceMatchId,
      season: 2026,
      scheduledStart: "2026-03-29T14:30:00.000Z",
      teamAName: "Mumbai Indians",
      teamBName: "Punjab Kings",
      venueName: "Wankhede",
      status: "in_progress",
      tossWinnerTeamName: "Mumbai Indians",
      tossDecision: "bat",
      winningTeamName: null,
      resultType: null,
    });

    const checkpoint = await repositories.normalized.saveCheckpoint({
      checkpointType: "post_toss",
      match: {
        competition: "IPL",
        matchSlug: canonicalMatch.matchSlug,
        sourceMatchId: canonicalMatch.sourceMatchId,
        season: canonicalMatch.season,
        scheduledStart: canonicalMatch.scheduledStart,
        teamAName: canonicalMatch.teamAName,
        teamBName: canonicalMatch.teamBName,
        venueName: canonicalMatch.venueName,
        status: canonicalMatch.status,
        tossWinnerTeamName: canonicalMatch.tossWinnerTeamName,
        tossDecision: canonicalMatch.tossDecision,
        winningTeamName: canonicalMatch.winningTeamName,
        resultType: canonicalMatch.resultType,
      },
      state: {
        matchSlug: canonicalMatch.matchSlug,
        checkpointType: "post_toss",
        snapshotTime: "2026-03-29T14:05:00.000Z",
        stateVersion: 1,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: cricketSnapshot.id,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload: { toss: "bat" },
      },
    });

    await expect(
      repositories.modeling.saveModelScore({
        checkpointStateId: checkpoint.id,
        checkpointType: "post_toss",
        scoringRunKey: `missing-run-${suffix}`,
        modelKey: `missing-model-${suffix}`,
        fairWinProbability: 0.61,
        marketImpliedProbability: 0.52,
        edge: 0.09,
        scoredAt: "2026-03-29T14:06:00.000Z",
        scorePayload: { reason: "should fail" },
      }),
    ).rejects.toThrow();
  });

  it("persists mapping status/confidence and exposes resolved-only scorer mappings", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const marketSnapshot = await repositories.raw.saveMarketSnapshot({
      competition: "IPL",
      sourceMarketId: `market-mapping-${suffix}`,
      marketSlug: `ipl-2027-mapping-${suffix}-winner`,
      eventSlug: `ipl-2027-mapping-${suffix}`,
      snapshotTime: "2027-01-20T12:00:00.000Z",
      marketStatus: "open",
      yesOutcomeName: "Lucknow Super Giants",
      noOutcomeName: "Gujarat Titans",
      outcomeProbabilities: { yes: 0.57, no: 0.43 },
      lastTradedPrice: 0.56,
      liquidity: 26000,
      payload: { source: "mapping-integration" },
    });

    const canonicalMatch = await repositories.normalized.saveCanonicalMatch({
      competition: "IPL",
      matchSlug: `ipl-2027-mapping-match-${suffix}`,
      sourceMatchId: `match-mapping-${suffix}`,
      season: 2027,
      scheduledStart: "2027-01-20T14:00:00.000Z",
      teamAName: "Lucknow Super Giants",
      teamBName: "Gujarat Titans",
      venueName: "Lucknow",
      status: "scheduled",
      tossWinnerTeamName: null,
      tossDecision: null,
      winningTeamName: null,
      resultType: null,
    });

    const resolved = await repositories.matching.saveMarketMatchMapping({
      sourceMarketId: marketSnapshot.sourceMarketId,
      sourceMarketSnapshotId: marketSnapshot.id,
      canonicalMatchId: canonicalMatch.id,
      mappingStatus: "resolved",
      confidence: 0.93,
      resolverVersion: "task10-v1",
      reason: "high_confidence_match",
      payload: {
        candidates: [
          {
            canonicalMatchId: canonicalMatch.id,
            confidence: 0.93,
          },
        ],
      },
    });

    const ambiguous = await repositories.matching.saveMarketMatchMapping({
      sourceMarketId: `market-mapping-ambiguous-${suffix}`,
      sourceMarketSnapshotId: marketSnapshot.id,
      canonicalMatchId: null,
      mappingStatus: "ambiguous",
      confidence: 0.79,
      resolverVersion: "task10-v1",
      reason: "multiple_close_candidates",
      payload: {
        candidates: [
          {
            canonicalMatchId: canonicalMatch.id,
            confidence: 0.79,
          },
        ],
      },
    });

    const scorerEligible =
      await repositories.matching.listScorerEligibleMappings({
        minimumConfidence: 0.85,
      });

    expect(resolved.mappingStatus).toBe("resolved");
    expect(resolved.matchSlug).toBe(canonicalMatch.matchSlug);
    expect(ambiguous.mappingStatus).toBe("ambiguous");
    expect(ambiguous.matchSlug).toBeNull();
    expect(
      scorerEligible.some(
        (mapping) => mapping.sourceMarketId === marketSnapshot.sourceMarketId,
      ),
    ).toBe(true);
    expect(
      scorerEligible.some(
        (mapping) => mapping.sourceMarketId === ambiguous.sourceMarketId,
      ),
    ).toBe(false);
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
        delete from model_scores ms
        using scoring_runs sr
        where ms.scoring_run_id = sr.id
          and right(sr.run_key, 8) = any($1::text[])
      `,
      [suffixes],
    );

    await cleanupPool.query(
      `
        delete from model_scores ms
        using model_registry mr
        where ms.model_registry_id = mr.id
          and right(mr.model_key, 8) = any($1::text[])
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
        delete from backtests b
        using model_registry mr
        where b.model_registry_id = mr.id
          and right(mr.model_key, 8) = any($1::text[])
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
        delete from raw_cricket_snapshots
        where right(source_match_id, 8) = any($1::text[])
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

    await cleanupPool.query(
      `
        delete from market_match_mappings
        where right(source_market_id, 8) = any($1::text[])
      `,
      [suffixes],
    );
  }
});
