import { randomUUID } from "node:crypto";

import { beforeAll, afterAll, afterEach, describe, expect, it } from "vitest";

import lifecycleSnapshots from "../fixtures/cricket/cricapi-lifecycle.snapshots.json" with { type: "json" };

import { resetDatabase } from "../../database/migration-runner.js";
import { loadAppConfig } from "../../src/config/index.js";
import {
  ingestCricketSnapshots,
  normalizeCricketSnapshot,
} from "../../src/ingest/cricket/index.js";
import {
  createPgPool,
  createRepositorySet,
} from "../../src/repositories/index.js";
import {
  buildTestDatabaseUrl,
  ensureTestDatabaseExists,
} from "../helpers/postgres-test-db.js";
import {
  dlsCricapiPayload,
  noResultCricapiPayload,
  superOverCricapiPayload,
} from "../fixtures/cricket/cricapi-edge-cases.js";
import {
  finalResultCricapiPayload,
  incompleteTossCricapiPayload,
  inningsBreakCricapiPayload,
  postTossCricapiPayload,
  preMatchCricapiPayload,
} from "../fixtures/cricket/cricapi.js";

describe("cricket normalization pipeline", () => {
  const config = loadAppConfig({
    DATABASE_URL: buildTestDatabaseUrl("sports_predictor_ingest_test"),
  });
  const repositories = createRepositorySet(config);
  const cleanupPool = createPgPool(config.databaseUrl);
  const createdSourceMatchIds = new Set<string>();

  beforeAll(async () => {
    await ensureTestDatabaseExists(config.databaseUrl);
    await resetDatabase(config.databaseUrl);
  });

  afterAll(async () => {
    await cleanupSyntheticRows(Array.from(createdSourceMatchIds));
    await cleanupPool.end();
    await repositories.close();
  });

  afterEach(async () => {
    await cleanupSyntheticRows(Array.from(createdSourceMatchIds));
    createdSourceMatchIds.clear();
  });

  it("normalizes pre-match, post-toss, innings-break, and final-result payloads without leakage", () => {
    const preMatch = normalizeCricketSnapshot({
      snapshotTime: "2026-03-29T13:00:00.000Z",
      payload: preMatchCricapiPayload,
    });
    const postToss = normalizeCricketSnapshot({
      snapshotTime: "2026-03-29T13:30:00.000Z",
      payload: postTossCricapiPayload,
    });
    const inningsBreak = normalizeCricketSnapshot({
      snapshotTime: "2026-03-29T15:20:00.000Z",
      payload: inningsBreakCricapiPayload,
    });
    const finalResult = normalizeCricketSnapshot({
      snapshotTime: "2026-03-29T17:35:00.000Z",
      payload: finalResultCricapiPayload,
    });

    expect(preMatch.status).toBe("normalized");
    expect(preMatch.lifecycle).toBe("pre_match");
    expect(preMatch.checkpoint?.checkpointType).toBe("pre_match");
    expect(
      (
        preMatch.checkpoint?.state.statePayload["result"] as Record<
          string,
          unknown
        >
      )["status"],
    ).toBe("unavailable");
    expect(
      (
        preMatch.checkpoint?.state.statePayload["pointInTimeMatch"] as Record<
          string,
          unknown
        >
      )["resultType"],
    ).toBeNull();

    expect(postToss.status).toBe("normalized");
    expect(postToss.lifecycle).toBe("post_toss");
    expect(postToss.checkpoint?.checkpointType).toBe("post_toss");
    expect(
      (
        postToss.checkpoint?.state.statePayload["toss"] as Record<
          string,
          unknown
        >
      )["status"],
    ).toBe("available");
    expect(postToss.checkpoint?.state.runs).toBeNull();

    expect(inningsBreak.status).toBe("normalized");
    expect(inningsBreak.lifecycle).toBe("innings_break");
    expect(inningsBreak.checkpoint?.checkpointType).toBe("innings_break");
    expect(inningsBreak.checkpoint?.state.runs).toBe(176);
    expect(inningsBreak.checkpoint?.state.targetRuns).toBe(177);

    expect(finalResult.status).toBe("normalized");
    expect(finalResult.lifecycle).toBe("final_result");
    expect(finalResult.checkpoint).toBeNull();
    expect(finalResult.canonicalMatch?.resultType).toBe("win");
    expect(finalResult.canonicalMatch?.winningTeamName).toBe("Mumbai Indians");
  });

  it("handles no-result and super-over finals explicitly while degrading DLS and incomplete toss payloads", () => {
    const noResult = normalizeCricketSnapshot({
      snapshotTime: "2026-04-01T13:20:00.000Z",
      payload: noResultCricapiPayload,
    });
    const superOver = normalizeCricketSnapshot({
      snapshotTime: "2026-04-03T17:40:00.000Z",
      payload: superOverCricapiPayload,
    });
    const dls = normalizeCricketSnapshot({
      snapshotTime: "2026-04-02T17:10:00.000Z",
      payload: dlsCricapiPayload,
    });
    const incompleteToss = normalizeCricketSnapshot({
      snapshotTime: "2026-04-04T13:25:00.000Z",
      payload: incompleteTossCricapiPayload,
    });

    expect(noResult.status).toBe("normalized");
    expect(noResult.lifecycle).toBe("final_result");
    expect(noResult.canonicalMatch?.status).toBe("no_result");
    expect(noResult.canonicalMatch?.resultType).toBe("no_result");
    expect(noResult.canonicalMatch?.winningTeamName).toBeNull();

    expect(superOver.status).toBe("normalized");
    expect(superOver.lifecycle).toBe("final_result");
    expect(superOver.canonicalMatch?.resultType).toBe("super_over");

    expect(dls.status).toBe("degraded");
    expect(dls.degradationReason).toBe("unsupported_provider_coverage");
    expect(dls.canonicalMatch).toBeNull();
    expect(dls.rawSnapshot.sourceMatchId).toBe("ipl-2026-003");

    expect(incompleteToss.status).toBe("degraded");
    expect(incompleteToss.degradationReason).toBe("lifecycle_degraded");
    expect(incompleteToss.canonicalMatch?.matchSlug).toContain("ipl-2026");
  });

  it("persists lifecycle checkpoints while keeping point-in-time match snapshots inside checkpoint payloads", async () => {
    const suffix = randomUUID().slice(0, 8);
    const sourceMatchId = `ipl-2026-001-${suffix}`;
    createdSourceMatchIds.add(sourceMatchId);
    const isolatedSnapshots = lifecycleSnapshots.map((snapshot) => ({
      snapshotTime: snapshot["snapshotTime"],
      payload: {
        ...(snapshot["payload"] as Record<string, unknown>),
        id: sourceMatchId,
      },
    }));

    const summary = await ingestCricketSnapshots(
      repositories,
      isolatedSnapshots,
    );
    const persistedMatchSlug =
      summary.results.find((result) => result.canonicalMatch !== null)
        ?.canonicalMatch?.matchSlug ?? null;
    const readModel =
      persistedMatchSlug === null
        ? null
        : await repositories.read.getMatchReadModel(persistedMatchSlug);

    expect(summary.totalSnapshots).toBe(4);
    expect(summary.normalizedSnapshots).toBe(4);
    expect(summary.degradedSnapshots).toBe(0);
    expect(summary.checkpointSnapshots).toBe(3);
    expect(summary.finalResultSnapshots).toBe(1);

    expect(readModel).not.toBeNull();
    expect(readModel?.match.sourceMatchId).toBe(sourceMatchId);
    expect(readModel?.match.resultType).toBe("win");
    expect(readModel?.match.winningTeamName).toBe("Mumbai Indians");
    expect(
      readModel?.checkpointStates.map((state) => state.checkpointType),
    ).toEqual(["pre_match", "post_toss", "innings_break"]);

    const preMatchPayload = readModel?.checkpointStates[0]
      ?.statePayload as Record<string, unknown>;
    const postTossPayload = readModel?.checkpointStates[1]
      ?.statePayload as Record<string, unknown>;

    expect(
      (preMatchPayload["pointInTimeMatch"] as Record<string, unknown>)[
        "winningTeamName"
      ],
    ).toBeNull();
    expect(
      (preMatchPayload["pointInTimeMatch"] as Record<string, unknown>)[
        "resultType"
      ],
    ).toBeNull();
    expect(
      (preMatchPayload["result"] as Record<string, unknown>)["status"],
    ).toBe("unavailable");
    expect((postTossPayload["toss"] as Record<string, unknown>)["status"]).toBe(
      "available",
    );
  });

  async function cleanupSyntheticRows(
    sourceMatchIds: readonly string[],
  ): Promise<void> {
    if (sourceMatchIds.length === 0) {
      return;
    }

    await cleanupPool.query(
      `
        delete from canonical_matches
        where source_match_id = any($1::text[])
      `,
      [sourceMatchIds],
    );

    await cleanupPool.query(
      `
        delete from raw_cricket_snapshots
        where source_match_id = any($1::text[])
      `,
      [sourceMatchIds],
    );
  }
});
