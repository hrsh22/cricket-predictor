import { describe, expect, it } from "vitest";

import { scoreInningsBreakCheckpoint } from "../../src/scoring/index.js";
import type { SocialSignal } from "../../src/social/policy.js";
import {
  inningsBreakCheckpoint,
  withInningsBreakStatePatch,
} from "../fixtures/features/innings-break.js";

describe("innings-break scorer", () => {
  it("emits a valuation row with checkpoint=innings_break for complete state", () => {
    const result = scoreInningsBreakCheckpoint({
      checkpoint: inningsBreakCheckpoint,
      marketImpliedProbability: 0.55,
      evaluatedAt: "2026-03-29T15:20:10.000Z",
    });

    expect(result.status).toBe("scored");
    if (result.status !== "scored") {
      throw new Error("expected innings-break scorer to produce a valuation");
    }

    expect(result.valuation.checkpointType).toBe("innings_break");
    expect(result.valuation.matchSlug).toBe(
      inningsBreakCheckpoint.match.matchSlug,
    );
    expect(result.valuation.evaluatedAt).toBe("2026-03-29T15:20:10.000Z");
    expect(result.valuation.fairWinProbability).toBeGreaterThanOrEqual(0);
    expect(result.valuation.fairWinProbability).toBeLessThanOrEqual(1);
    expect(result.valuation.edge).toBe(
      Number((result.valuation.fairWinProbability - 0.55).toFixed(6)),
    );
  });

  it("applies bounded social adjustment in an auditable way", () => {
    const socialSignal: SocialSignal = {
      competition: "IPL",
      matchSlug: inningsBreakCheckpoint.match.matchSlug,
      checkpointType: "innings_break",
      targetTeamName: inningsBreakCheckpoint.match.teamAName,
      source: {
        providerKey: "mirofish-inspired-manual",
        sourceType: "analyst_note",
        sourceId: "ib-social-001",
        sourceLabel: "Manual analyst note",
        sourceQuality: "trusted",
        capturedAt: "2026-03-29T15:18:00.000Z",
        publishedAt: null,
        provenanceUrl: null,
      },
      summary: "Positive sentiment for team A batting depth.",
      confidence: 0.7,
      requestedAdjustment: 0.08,
      boundedAdjustment: 0.05,
      adjustmentCap: 0.05,
      status: "applied",
      reason: "clamped_to_cap",
    };

    const withoutSocial = scoreInningsBreakCheckpoint({
      checkpoint: inningsBreakCheckpoint,
      marketImpliedProbability: null,
      evaluatedAt: "2026-03-29T15:20:10.000Z",
    });
    const withSocial = scoreInningsBreakCheckpoint({
      checkpoint: inningsBreakCheckpoint,
      marketImpliedProbability: null,
      evaluatedAt: "2026-03-29T15:20:10.000Z",
      socialSignal,
    });

    if (withoutSocial.status !== "scored" || withSocial.status !== "scored") {
      throw new Error("expected innings-break scorer to score both scenarios");
    }

    expect(withSocial.valuation.fairWinProbability).toBe(
      Number((withoutSocial.valuation.fairWinProbability + 0.05).toFixed(6)),
    );
    expect(withSocial.valuation.valuationPayload["socialApplied"]).toBe(true);
  });

  it("skips incomplete innings-break state explicitly", () => {
    const incompleteCheckpoint = withInningsBreakStatePatch({
      statePayload: {
        toss: {
          status: "unavailable",
          reason: "toss_not_reported",
        },
      },
    });

    const result = scoreInningsBreakCheckpoint({
      checkpoint: incompleteCheckpoint,
      marketImpliedProbability: 0.52,
    });

    expect(result).toEqual({
      status: "skipped",
      reason: "incomplete_state",
      detail:
        "innings-break scoring requires complete toss and innings payload state",
    });
  });

  it("skips unsupported innings-number states explicitly", () => {
    const unsupportedCheckpoint = withInningsBreakStatePatch({
      inningsNumber: 2,
    });

    const result = scoreInningsBreakCheckpoint({
      checkpoint: unsupportedCheckpoint,
      marketImpliedProbability: 0.5,
    });

    expect(result).toEqual({
      status: "skipped",
      reason: "unsupported_innings_number",
      detail:
        "innings-break scoring is supported only for completed first-innings state",
    });
  });
});
