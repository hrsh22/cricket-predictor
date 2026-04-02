import { randomUUID } from "node:crypto";

import { beforeAll, afterAll, afterEach, describe, expect, it } from "vitest";

import { resetDatabase } from "../../database/migration-runner.js";
import { loadAppConfig } from "../../src/config/index.js";
import {
  runRecurringPipeline,
  scopeEligibleMappingsToRun,
} from "../../src/orchestration/index.js";
import {
  createPgPool,
  createRepositorySet,
} from "../../src/repositories/index.js";
import {
  buildTestDatabaseUrl,
  ensureTestDatabaseExists,
} from "../helpers/postgres-test-db.js";
import {
  inningsBreakCricapiPayload,
  postTossCricapiPayload,
  preMatchCricapiPayload,
} from "../fixtures/cricket/cricapi.js";

describe("recurring orchestration", () => {
  const config = loadAppConfig({
    DATABASE_URL: buildTestDatabaseUrl("sports_predictor_orchestration_test"),
  });
  const orchestrationPool = createPgPool(config.databaseUrl);
  const cleanupPool = createPgPool(config.databaseUrl);
  const seedRepositories = createRepositorySet(config);
  const createdSuffixes = new Set<string>();

  beforeAll(async () => {
    await ensureTestDatabaseExists(config.databaseUrl);
    await resetDatabase(config.databaseUrl);
  });

  afterEach(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    createdSuffixes.clear();
  });

  afterAll(async () => {
    await cleanupSyntheticRows(Array.from(createdSuffixes));
    await cleanupPool.end();
    await seedRepositories.close();
    await orchestrationPool.end();
  });

  it.each([
    ["pre_match", preMatchCricapiPayload, true, 0.54],
    ["post_toss", postTossCricapiPayload, false, 0.38],
    ["innings_break", inningsBreakCricapiPayload, false, 0.38],
  ] as const)(
    "runs the %s checkpoint window end to end",
    async (
      checkpointType,
      cricketPayload,
      teamAIsYesOutcome,
      expectedTeamAProbability,
    ) => {
      const suffix = randomUUID().slice(0, 8);
      createdSuffixes.add(suffix);

      const market = cloneMarketFixture(suffix, teamAIsYesOutcome);
      const cricketSnapshot = cloneCricketSnapshot(cricketPayload, suffix);
      const runKey = `recurring-${checkpointType}-${suffix}`;

      await seedActiveModel(checkpointType, suffix);

      const summary = await runRecurringPipeline(orchestrationPool, {
        checkpointType,
        runKey,
        triggeredBy: "scheduled",
        marketIngestion: {
          trigger: "scheduled",
          gammaClient: {
            async listMarkets(): Promise<readonly unknown[]> {
              return [market];
            },
          },
        },
        cricketIngestion: {
          snapshots: [cricketSnapshot],
        },
      });

      expect(summary.runKey).toBe(runKey);
      expect(summary.checkpointType).toBe(checkpointType);
      expect(summary.ingest.marketsPersisted).toBe(1);
      expect(summary.ingest.cricketSnapshotsPersisted).toBe(1);
      expect(summary.map.resolvedCount).toBe(1);
      expect(summary.score.scoredCount).toBe(1);
      expect(summary.score.skippedCount).toBe(0);
      expect(summary.report.noData).toBe(false);
      expect(summary.report.rows).toHaveLength(1);
      expect(summary.report.rows[0]?.checkpointType).toBe(checkpointType);
      expect(summary.report.rows[0]?.sourceMarketId).toBe(market.id);
      expect(summary.report.rows[0]?.marketImpliedProbability).toBe(
        expectedTeamAProbability,
      );
      expect(summary.report.rows[0]?.note.length ?? 0).toBeGreaterThan(0);
    },
  );

  it("uses all eligible mappings when no new source market ids are supplied", () => {
    const eligibleMappings = [
      { sourceMarketId: "market-a", matchSlug: "match-a" },
      { sourceMarketId: "market-b", matchSlug: "match-b" },
    ] as const;

    const scoped = scopeEligibleMappingsToRun(eligibleMappings, []);

    expect(scoped).toEqual(eligibleMappings);
  });

  it("rejects duplicate recurring runs for the same run key", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const market = cloneMarketFixture(suffix, true);
    const cricketSnapshot = cloneCricketSnapshot(
      preMatchCricapiPayload,
      suffix,
    );
    const runKey = `recurring-pre_match-${suffix}`;

    await seedActiveModel("pre_match", suffix);

    const request = {
      checkpointType: "pre_match" as const,
      runKey,
      triggeredBy: "scheduled",
      marketIngestion: {
        trigger: "scheduled" as const,
        gammaClient: {
          async listMarkets(): Promise<readonly unknown[]> {
            return [market];
          },
        },
      },
      cricketIngestion: {
        snapshots: [cricketSnapshot],
      },
    };

    await runRecurringPipeline(orchestrationPool, request);
    await expect(
      runRecurringPipeline(orchestrationPool, request),
    ).rejects.toThrow(/already recorded/i);
  });

  it("rolls back partial work when the run cannot be scored", async () => {
    const suffix = randomUUID().slice(0, 8);
    createdSuffixes.add(suffix);

    const market = cloneMarketFixture(suffix, false);
    const cricketSnapshot = cloneCricketSnapshot(
      postTossCricapiPayload,
      suffix,
    );
    const runKey = `recurring-post_toss-${suffix}`;

    await cleanupPool.query(
      `update model_registry set is_active = false where checkpoint_type = 'post_toss'`,
    );

    await expect(
      runRecurringPipeline(orchestrationPool, {
        checkpointType: "post_toss",
        runKey,
        triggeredBy: "scheduled",
        marketIngestion: {
          trigger: "scheduled",
          gammaClient: {
            async listMarkets(): Promise<readonly unknown[]> {
              return [market];
            },
          },
        },
        cricketIngestion: {
          snapshots: [cricketSnapshot],
        },
      }),
    ).rejects.toThrow(/No active model registry/);

    const counts = await countSyntheticRows(suffix);
    expect(counts).toEqual({
      rawMarkets: 0,
      rawCricket: 0,
      canonicalMatches: 0,
      checkpointStates: 0,
      featureRows: 0,
      marketMappings: 0,
      scoringRuns: 0,
      modelScores: 0,
      modelRegistry: 0,
    });
  });

  async function seedActiveModel(
    checkpointType: "pre_match" | "post_toss" | "innings_break",
    suffix: string,
  ): Promise<void> {
    await seedRepositories.modeling.saveModelRegistry({
      modelKey: `baseline-${checkpointType}-${suffix}`,
      checkpointType,
      modelFamily: "baseline-rating",
      version: `v-${suffix}`,
      trainingWindow: "2022-2026",
      isActive: true,
      metadata: { source: "orchestration-test" },
      createdAt: `2026-03-29T18:0${checkpointType.length % 10}:00.000Z`,
    });
  }

  async function cleanupSyntheticRows(
    suffixes: readonly string[],
  ): Promise<void> {
    for (const suffix of suffixes) {
      await cleanupPool.query(
        `delete from scoring_runs where run_key like $1`,
        [`%${suffix}`],
      );
      await cleanupPool.query(
        `delete from model_registry where model_key like $1`,
        [`%${suffix}`],
      );
      await cleanupPool.query(
        `delete from market_match_mappings where source_market_id like $1`,
        [`%${suffix}`],
      );
      await cleanupPool.query(
        `delete from canonical_matches where match_slug like $1 or coalesce(source_match_id, '') like $1`,
        [`%${suffix}`],
      );
      await cleanupPool.query(
        `delete from raw_cricket_snapshots where source_match_id like $1`,
        [`%${suffix}`],
      );
      await cleanupPool.query(
        `delete from raw_market_snapshots where source_market_id like $1`,
        [`%${suffix}`],
      );
    }
  }

  async function countSyntheticRows(suffix: string): Promise<{
    rawMarkets: number;
    rawCricket: number;
    canonicalMatches: number;
    checkpointStates: number;
    featureRows: number;
    marketMappings: number;
    scoringRuns: number;
    modelScores: number;
    modelRegistry: number;
  }> {
    const [
      rawMarkets,
      rawCricket,
      canonicalMatches,
      checkpointStates,
      featureRows,
      marketMappings,
      scoringRuns,
      modelScores,
      modelRegistry,
    ] = await Promise.all([
      cleanupPool.query(
        `select count(*)::int as count from raw_market_snapshots where source_market_id like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from raw_cricket_snapshots where source_match_id like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from canonical_matches where match_slug like $1 or coalesce(source_match_id, '') like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from checkpoint_states cs join canonical_matches cm on cm.id = cs.canonical_match_id where cm.match_slug like $1 or coalesce(cm.source_match_id, '') like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from match_features mf join checkpoint_states cs on cs.id = mf.checkpoint_state_id join canonical_matches cm on cm.id = cs.canonical_match_id where cm.match_slug like $1 or coalesce(cm.source_match_id, '') like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from market_match_mappings where source_market_id like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from scoring_runs where run_key like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from model_scores ms join scoring_runs sr on sr.id = ms.scoring_run_id where sr.run_key like $1`,
        [`%${suffix}`],
      ),
      cleanupPool.query(
        `select count(*)::int as count from model_registry where model_key like $1`,
        [`%${suffix}`],
      ),
    ]);

    return {
      rawMarkets: rawMarkets.rows[0]?.count ?? 0,
      rawCricket: rawCricket.rows[0]?.count ?? 0,
      canonicalMatches: canonicalMatches.rows[0]?.count ?? 0,
      checkpointStates: checkpointStates.rows[0]?.count ?? 0,
      featureRows: featureRows.rows[0]?.count ?? 0,
      marketMappings: marketMappings.rows[0]?.count ?? 0,
      scoringRuns: scoringRuns.rows[0]?.count ?? 0,
      modelScores: modelScores.rows[0]?.count ?? 0,
      modelRegistry: modelRegistry.rows[0]?.count ?? 0,
    };
  }

  function cloneMarketFixture(suffix: string, teamAIsYesOutcome: boolean) {
    const teamA = `Alpha XI ${suffix}`;
    const teamB = `Beta XI ${suffix}`;
    const yesOutcomeName = teamAIsYesOutcome ? teamA : teamB;
    const noOutcomeName = teamAIsYesOutcome ? teamB : teamA;
    const yesProbability = teamAIsYesOutcome ? 0.54 : 0.62;
    const noProbability = teamAIsYesOutcome ? 0.46 : 0.38;

    return {
      id: `pm-${suffix}`,
      slug: `ipl-2026-${teamA.toLowerCase().replace(/\s+/g, "-")}-vs-${teamB.toLowerCase().replace(/\s+/g, "-")}-winner-${suffix}`,
      question: `Indian Premier League: ${teamA} vs ${teamB}`,
      market_type: "binary",
      sportsMarketType: "moneyline",
      active: true,
      closed: false,
      archived: false,
      endDate: "2026-03-29T14:00:00.000Z",
      updatedAt: "2026-03-29T13:10:00.000Z",
      outcomes: [yesOutcomeName, noOutcomeName],
      outcomePrices: [yesProbability, noProbability],
      liquidityClob: 42000,
      clobTokenIds: [`token-yes-${suffix}`, `token-no-${suffix}`],
      events: [
        {
          id: `event-${suffix}`,
          slug: `ipl-2026-${teamA.toLowerCase().replace(/\s+/g, "-")}-vs-${teamB.toLowerCase().replace(/\s+/g, "-")}-${suffix}`,
          title: `Indian Premier League: ${teamA} vs ${teamB}`,
          seriesSlug: "indian-premier-league",
        },
      ],
    };
  }

  function cloneCricketSnapshot(
    payload:
      | typeof preMatchCricapiPayload
      | typeof postTossCricapiPayload
      | typeof inningsBreakCricapiPayload,
    suffix: string,
  ) {
    const teamA = `Alpha XI ${suffix}`;
    const teamB = `Beta XI ${suffix}`;
    const clonedPayload = JSON.parse(
      JSON.stringify(payload)
        .replaceAll("Chennai Super Kings", teamA)
        .replaceAll("Mumbai Indians", teamB),
    ) as typeof payload;

    return {
      snapshotTime: payload.date,
      payload: {
        ...clonedPayload,
        id: `${payload.id}-${suffix}`,
      },
    };
  }
});
