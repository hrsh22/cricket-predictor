import { describe, expect, it, vi } from "vitest";

import {
  scoreAndPersistPreMatchValuation,
  scorePreMatchValuation,
} from "../../src/scoring/index.js";
import type {
  BacktestInsert,
  BacktestRecord,
  ModelRegistryInsert,
  ModelRegistryRecord,
  ModelScoreInsert,
  ModelScoreRecord,
  ModelingRepository,
  ScoringRunInsert,
  ScoringRunRecord,
} from "../../src/repositories/modeling.js";
import type { MarketMatchMappingRecord } from "../../src/repositories/matching.js";
import { parseSocialSignalCandidate } from "../../src/social/index.js";
import {
  baselinePreMatchCheckpoint,
  baselinePreMatchFeatureContext,
} from "../fixtures/features/pre-match.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/index.js";
import { parseMarketSnapshot } from "../../src/domain/index.js";

describe("pre-match scorer valuation", () => {
  it("emits fair %, market %, spread/edge, checkpoint tag, run/model metadata, and social note", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const result = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 501,
      scoringRunKey: "run-pre-match-001",
      modelKey: "baseline-pre-match-v1",
      socialCandidate: createPreMatchSocialCandidate(),
    });

    expect(result.checkpointType).toBe("pre_match");
    expect(result.checkpointTag).toBe("pre_match");
    expect(result.modelKey).toBe("baseline-pre-match-v1");
    expect(result.scoringRunKey).toBe("run-pre-match-001");
    expect(result.marketImpliedProbability).toBe(0.54);
    expect(result.edge).toBe(result.spread);
    expect(result.socialMode).toBe("enabled");
    expect(result.socialAdjustmentNote).toContain("yes outcome");
    expect(result.fairWinProbability).toBeGreaterThan(
      result.structuredFairProbability,
    );

    const socialAdjustment = result.scorePayload["socialAdjustment"] as Record<
      string,
      unknown
    >;
    expect(socialAdjustment["requestedAdjustment"]).toBe(0.08);
    expect(socialAdjustment["boundedAdjustment"]).toBe(0.05);
    expect(socialAdjustment["mode"]).toBe("enabled");
  });

  it("keeps base probability unchanged when social is explicitly disabled", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const result = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 502,
      scoringRunKey: "run-pre-match-002",
      modelKey: "baseline-pre-match-v1",
      socialCandidate: createPreMatchSocialCandidate(),
      socialPolicyOverrides: {
        enabled: false,
      },
    });

    expect(result.socialMode).toBe("disabled");
    expect(result.fairWinProbability).toBe(result.structuredFairProbability);
    expect(result.socialAdjustmentNote).toContain("disabled");
  });

  it("rejects unresolved mappings so scorer runs only on resolved mapped markets", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const unresolvedMapping: MarketMatchMappingRecord = {
      ...createResolvedMapping(featureRow.matchSlug),
      mappingStatus: "ambiguous",
      canonicalMatchId: null,
      matchSlug: null,
    };

    expect(() =>
      scorePreMatchValuation({
        mapping: unresolvedMapping,
        marketSnapshot: createMarketSnapshot(),
        featureRow,
        checkpointStateId: 503,
        scoringRunKey: "run-pre-match-003",
        modelKey: "baseline-pre-match-v1",
      }),
    ).toThrow(/resolved market mapping/);
  });

  it("persists valuation output to model_scores with lineage metadata", async () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );
    const saveModelScore = vi.fn(
      async (insert: ModelScoreInsert): Promise<ModelScoreRecord> => ({
        id: 77,
        scoringRunId: 44,
        scoringRunKey: insert.scoringRunKey,
        modelRegistryId: 22,
        modelKey: insert.modelKey,
        canonicalMatchId: 11,
        matchSlug: featureRow.matchSlug,
        checkpointStateId: insert.checkpointStateId,
        checkpointType: insert.checkpointType,
        sourceMarketSnapshotId: 9001,
        sourceCricketSnapshotId: 7001,
        fairWinProbability: insert.fairWinProbability,
        marketImpliedProbability: insert.marketImpliedProbability,
        edge: insert.edge,
        scoredAt: insert.scoredAt,
        scorePayload: insert.scorePayload,
      }),
    );

    const modelingRepository = createModelingRepositoryStub(saveModelScore);

    const persisted = await scoreAndPersistPreMatchValuation({
      modelingRepository,
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 504,
      scoringRunKey: "run-pre-match-004",
      modelKey: "baseline-pre-match-v1",
      socialCandidate: createPreMatchSocialCandidate(),
      scoredAt: "2026-04-05T12:05:00.000Z",
    });

    expect(saveModelScore).toHaveBeenCalledTimes(1);
    expect(saveModelScore).toHaveBeenCalledWith(
      expect.objectContaining({
        checkpointStateId: 504,
        checkpointType: "pre_match",
        scoringRunKey: "run-pre-match-004",
        modelKey: "baseline-pre-match-v1",
      }),
    );

    expect(persisted.persistedScore.scoringRunKey).toBe("run-pre-match-004");
    expect(persisted.persistedScore.modelKey).toBe("baseline-pre-match-v1");
    expect(persisted.valuation.checkpointTag).toBe("pre_match");
  });

  it("applies platt calibration from model metadata when provided", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const withoutCalibration = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 505,
      scoringRunKey: "run-pre-match-005",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
    });

    const withCalibration = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 506,
      scoringRunKey: "run-pre-match-006",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
      modelMetadata: {
        preMatchModelOptions: {
          enabled: true,
          calibrationMethod: "platt",
          plattCalibration: {
            intercept: -0.1,
            slope: 0.9,
            converged: true,
            trainSampleSize: 120,
          },
        },
      },
    });

    expect(withCalibration.structuredFairProbability).not.toBe(
      withoutCalibration.structuredFairProbability,
    );
    const baselinePayload = withCalibration.scorePayload["baseline"] as Record<
      string,
      unknown
    >;
    const modelOptionsApplied = baselinePayload[
      "modelOptionsApplied"
    ] as Record<string, unknown>;
    expect(modelOptionsApplied["plattCalibrationApplied"]).toBe(1);
  });

  it("ignores calibration metadata unless explicitly enabled", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const withoutMetadata = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 507,
      scoringRunKey: "run-pre-match-007",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
    });

    const withDisabledMetadata = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 508,
      scoringRunKey: "run-pre-match-008",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
      modelMetadata: {
        preMatchModelOptions: {
          enabled: false,
          plattCalibration: {
            intercept: -0.1,
            slope: 0.9,
          },
        },
      },
    });

    expect(withDisabledMetadata.structuredFairProbability).toBe(
      withoutMetadata.structuredFairProbability,
    );
  });

  it("ignores enabled calibration metadata when platt safety checks fail", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const withoutMetadata = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 509,
      scoringRunKey: "run-pre-match-009",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
    });

    const withUnsafeCalibration = scorePreMatchValuation({
      mapping: createResolvedMapping(featureRow.matchSlug),
      marketSnapshot: createMarketSnapshot(),
      featureRow,
      checkpointStateId: 510,
      scoringRunKey: "run-pre-match-010",
      modelKey: "baseline-pre-match-v1",
      socialPolicyOverrides: {
        enabled: false,
      },
      modelMetadata: {
        preMatchModelOptions: {
          enabled: true,
          calibrationMethod: "platt",
          plattCalibration: {
            intercept: -0.1,
            slope: 0.9,
            converged: false,
            trainSampleSize: 10,
          },
        },
      },
    });

    expect(withUnsafeCalibration.structuredFairProbability).toBe(
      withoutMetadata.structuredFairProbability,
    );
  });
});

function createResolvedMapping(matchSlug: string): MarketMatchMappingRecord {
  return {
    id: 901,
    competition: "IPL",
    sourceMarketId: "market-ipl-rcb-kkr-001",
    sourceMarketSnapshotId: 9001,
    canonicalMatchId: 3001,
    matchSlug,
    mappingStatus: "resolved",
    confidence: 0.93,
    resolverVersion: "task10-v1",
    reason: "high_confidence_match",
    payload: {
      candidates: [
        {
          canonicalMatchId: 3001,
          confidence: 0.93,
        },
      ],
    },
    createdAt: "2026-04-05T12:00:00.000Z",
    updatedAt: "2026-04-05T12:00:00.000Z",
  };
}

function createMarketSnapshot() {
  return parseMarketSnapshot({
    competition: "IPL",
    sourceMarketId: "market-ipl-rcb-kkr-001",
    marketSlug: "ipl-2026-rcb-vs-kkr-winner",
    eventSlug: "ipl-2026-rcb-vs-kkr",
    snapshotTime: "2026-04-05T12:00:00.000Z",
    marketStatus: "open",
    yesOutcomeName: "Royal Challengers Bengaluru",
    noOutcomeName: "Kolkata Knight Riders",
    outcomeProbabilities: {
      yes: 0.54,
      no: 0.46,
    },
    lastTradedPrice: 0.53,
    liquidity: 32000,
    payload: {
      source: "test",
    },
  });
}

function createPreMatchSocialCandidate() {
  return parseSocialSignalCandidate({
    competition: "IPL",
    matchSlug: "ipl-2026-rcb-vs-kkr",
    checkpointType: "pre_match",
    targetTeamName: "Royal Challengers Bengaluru",
    source: {
      providerKey: "mirofish-inspired-manual",
      sourceType: "analyst_note",
      sourceId: "social-pre-match-001",
      sourceLabel: "Trusted pre-match analyst note",
      sourceQuality: "trusted",
      capturedAt: "2026-04-05T11:50:00.000Z",
      publishedAt: "2026-04-05T11:40:00.000Z",
      provenanceUrl: "https://example.com/pre-match-note",
    },
    summary: "Trusted analyst gives RCB a clear tactical edge before toss.",
    confidence: 0.77,
    requestedAdjustment: 0.08,
  });
}

function createModelingRepositoryStub(
  saveModelScore: (score: ModelScoreInsert) => Promise<ModelScoreRecord>,
): ModelingRepository {
  return {
    async saveModelRegistry(
      input: ModelRegistryInsert,
    ): Promise<ModelRegistryRecord> {
      void input;
      throw new Error("Not implemented in scorer unit test");
    },
    async saveScoringRun(input: ScoringRunInsert): Promise<ScoringRunRecord> {
      void input;
      throw new Error("Not implemented in scorer unit test");
    },
    async saveModelScore(score: ModelScoreInsert): Promise<ModelScoreRecord> {
      return saveModelScore(score);
    },
    async saveBacktest(input: BacktestInsert): Promise<BacktestRecord> {
      void input;
      throw new Error("Not implemented in scorer unit test");
    },
  };
}
