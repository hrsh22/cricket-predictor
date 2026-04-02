import { describe, expect, it } from "vitest";

import {
  DomainValidationError,
  parseCanonicalCheckpoint,
  parseFeatureRow,
  parseMarketSnapshot,
  parseModelScore,
  parseTeam,
  parseValuationResult,
} from "../../src/domain/index.js";
import {
  invalidInningsBreakCheckpoint,
  invalidPostTossCheckpoint,
  invalidPreMatchCheckpoint,
  validInningsBreakCheckpoint,
  validPostTossCheckpoint,
  validPreMatchCheckpoint,
} from "../fixtures/domain/checkpoint-payloads.js";

describe("domain schemas", () => {
  it("parses the canonical checkpoint shapes for all three checkpoints", () => {
    const preMatch = parseCanonicalCheckpoint(validPreMatchCheckpoint);
    const postToss = parseCanonicalCheckpoint(validPostTossCheckpoint);
    const inningsBreak = parseCanonicalCheckpoint(validInningsBreakCheckpoint);

    expect(preMatch.checkpointType).toBe("pre_match");
    expect(preMatch.state.inningsNumber).toBeNull();
    expect(postToss.match.tossDecision).toBe("bowl");
    expect(inningsBreak.state.runs).toBe(176);
  });

  it("accepts null source snapshot ids for a valid checkpoint", () => {
    const checkpoint = parseCanonicalCheckpoint({
      ...validPreMatchCheckpoint,
      state: {
        ...validPreMatchCheckpoint.state,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
      },
    });

    expect(checkpoint.state.sourceMarketSnapshotId).toBeNull();
    expect(checkpoint.state.sourceCricketSnapshotId).toBeNull();
  });

  it("rejects future-only innings data at pre-match", () => {
    try {
      parseCanonicalCheckpoint(invalidPreMatchCheckpoint);
      throw new Error("expected checkpoint validation to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(DomainValidationError);
      expect((error as DomainValidationError).issues.map((issue) => issue.path)).toEqual(["state"]);
    }
  });

  it("rejects innings leakage at post-toss", () => {
    expect(() => parseCanonicalCheckpoint(invalidPostTossCheckpoint)).toThrow(DomainValidationError);
  });

  it("rejects super-over and DLS leakage at innings break", () => {
    expect(() => parseCanonicalCheckpoint(invalidInningsBreakCheckpoint)).toThrow(DomainValidationError);
  });

  it("parses the shared domain row payloads", () => {
    expect(
      parseMarketSnapshot({
        competition: "IPL",
        sourceMarketId: "market-1",
        marketSlug: "ipl-2026-chennai-vs-mumbai-winner",
        eventSlug: "ipl-2026-chennai-vs-mumbai",
        snapshotTime: "2026-03-29T13:20:00.000Z",
        marketStatus: "open",
        yesOutcomeName: "Chennai Super Kings",
        noOutcomeName: "Mumbai Indians",
        outcomeProbabilities: { yes: 0.54, no: 0.46 },
        lastTradedPrice: 0.53,
        liquidity: 42000,
        payload: {},
      }).marketSlug,
    ).toBe("ipl-2026-chennai-vs-mumbai-winner");

    expect(parseTeam({ name: "Chennai Super Kings", shortName: "CSK" }).shortName).toBe("CSK");
    expect(
      parseFeatureRow({
        matchSlug: "ipl-2026-chennai-vs-mumbai",
        checkpointType: "pre_match",
        featureSetVersion: "v1",
        generatedAt: "2026-03-29T13:25:00.000Z",
        features: { baselineRating: 0.61 },
      }).featureSetVersion,
    ).toBe("v1");

    expect(
      parseModelScore({
        matchSlug: "ipl-2026-chennai-vs-mumbai",
        checkpointType: "innings_break",
        scoringRunKey: "score-001",
        modelKey: "innings-break-xgboost",
        fairWinProbability: 0.62,
        marketImpliedProbability: 0.55,
        edge: 0.07,
        scoredAt: "2026-03-29T15:16:00.000Z",
        scorePayload: {},
      }).modelKey,
    ).toBe("innings-break-xgboost");

    expect(
      parseValuationResult({
        matchSlug: "ipl-2026-chennai-vs-mumbai",
        checkpointType: "innings_break",
        fairWinProbability: 0.62,
        marketImpliedProbability: 0.55,
        edge: 0.07,
        evaluatedAt: "2026-03-29T15:16:30.000Z",
        valuationPayload: {},
      }).edge,
    ).toBe(0.07);
  });
});
