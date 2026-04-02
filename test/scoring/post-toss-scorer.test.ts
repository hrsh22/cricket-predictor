import { describe, expect, it } from "vitest";

import { scorePostTossValuation } from "../../src/scoring/index.js";
import {
  baselinePostTossCheckpoint,
  baselinePreMatchFeatureContext,
  tossAgainstTeamACheckpoint,
} from "../fixtures/features/post-toss.js";

describe("post-toss scorer", () => {
  it("emits a valid post_toss valuation row", () => {
    const result = scorePostTossValuation({
      checkpoint: baselinePostTossCheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
      evaluatedAt: "2026-04-05T12:01:00.000Z",
    });

    expect(result.valuation.checkpointType).toBe("post_toss");
    expect(result.valuation.fairWinProbability).toBeGreaterThanOrEqual(0);
    expect(result.valuation.fairWinProbability).toBeLessThanOrEqual(1);
    expect(result.valuation.marketImpliedProbability).toBe(0.54);
    expect(result.valuation.edge).toBe(
      Number((result.valuation.fairWinProbability - 0.54).toFixed(6)),
    );

    expect(result.valuation.valuationPayload["checkpoint"]).toBe("post_toss");
    expect(result.valuation.valuationPayload["socialAdjustmentApplied"]).toBe(
      false,
    );
    expect(result.valuation.valuationPayload["tossDecision"]).toBe("bowl");
  });

  it("changes probability by toss winner + toss decision only", () => {
    const teamAAfterToss = scorePostTossValuation({
      checkpoint: baselinePostTossCheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
    });

    const teamBAfterToss = scorePostTossValuation({
      checkpoint: tossAgainstTeamACheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
    });

    expect(teamAAfterToss.baseFairWinProbability).toBe(
      teamBAfterToss.baseFairWinProbability,
    );
    expect(teamAAfterToss.tossAdjustment).toBeGreaterThan(0);
    expect(teamBAfterToss.tossAdjustment).toBeLessThan(0);
    expect(teamAAfterToss.valuation.fairWinProbability).toBeGreaterThan(
      teamBAfterToss.valuation.fairWinProbability,
    );
  });

  it("rejects innings-state leakage in post_toss scoring", () => {
    const leaked = {
      ...baselinePostTossCheckpoint,
      state: {
        ...baselinePostTossCheckpoint.state,
        targetRuns: 180,
      },
    } as unknown as typeof baselinePostTossCheckpoint;

    expect(() =>
      scorePostTossValuation({
        checkpoint: leaked,
        featureContext: baselinePreMatchFeatureContext,
        marketImpliedProbability: 0.54,
      }),
    ).toThrow(/Innings-state leakage/);
  });

  it("supports metadata-driven toss strengths for post_toss scoring", () => {
    const baseline = scorePostTossValuation({
      checkpoint: baselinePostTossCheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
    });

    const tuned = scorePostTossValuation({
      checkpoint: baselinePostTossCheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
      modelMetadata: {
        postTossModelOptions: {
          enabled: true,
          bowlDecisionStrength: 0.03,
          batDecisionStrength: 0.01,
          venueTossStrength: 0.08,
        },
      },
    });

    expect(tuned.tossAdjustment).toBeGreaterThan(baseline.tossAdjustment);
    expect(tuned.valuation.fairWinProbability).toBeGreaterThan(
      baseline.valuation.fairWinProbability,
    );
  });

  it("applies venue toss decision feature in post-toss scorer", () => {
    const result = scorePostTossValuation({
      checkpoint: baselinePostTossCheckpoint,
      featureContext: baselinePreMatchFeatureContext,
      marketImpliedProbability: 0.54,
      modelMetadata: {
        postTossModelOptions: {
          enabled: true,
          bowlDecisionStrength: 0.01,
          batDecisionStrength: 0.01,
          venueTossStrength: 0.1,
        },
      },
    });

    const payload = result.valuation.valuationPayload as Record<
      string,
      unknown
    >;
    expect(payload["venueTossAdjustment"]).not.toBe(0);
  });
});
