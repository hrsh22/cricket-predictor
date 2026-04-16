import { describe, expect, it } from "vitest";

import {
  buildNextMatchPredictionView,
  formatNextMatchPrediction,
} from "../../scripts/predict-next-match.js";
import type { RecurringRunSummary } from "../../src/orchestration/index.js";

describe("predict next match script", () => {
  it("builds and formats a concise next-match prediction", () => {
    const summary: RecurringRunSummary = {
      runKey: "next-match-run",
      checkpointType: "pre_match",
      triggeredBy: "manual_next_match",
      ingest: {
        marketsPersisted: 1,
        cricketSnapshotsPersisted: 1,
        marketSourceIds: ["m1"],
      },
      normalize: {
        normalizedSnapshots: 1,
        checkpointSnapshots: 1,
        featureRows: 1,
      },
      map: {
        totalMarkets: 1,
        resolvedCount: 1,
        ambiguousCount: 0,
        unresolvedCount: 0,
      },
      score: {
        scoredCount: 1,
        skippedCount: 0,
        skipped: [],
      },
      report: {
        noData: false,
        rows: [
          {
            checkpointType: "pre_match",
            matchSlug: "ipl-2026-csk-vs-mi",
            sourceMarketId: "m1",
            sourceMarketSnapshotId: 11,
            modelKey: "profit-first-pre-match-v1",
            modelVersion: "v1",
            teamAName: "Chennai Super Kings",
            teamBName: "Mumbai Indians",
            yesOutcomeName: "Chennai Super Kings",
            fairWinProbability: 0.53,
            marketImpliedProbability: 0.48,
            spread: 0.05,
            note: "Social adjustment not applied (missing_signal).",
            scoredAt: "2026-04-14T10:00:00.000Z",
            tradeThesis: {
              position: "bet_yes",
              outcomeName: "Chennai Super Kings",
              edgeCents: 5,
              contractPriceCents: 48,
              fairValueCents: 53,
              conviction: "tradable",
              mispricingSummary:
                "Rest differential and season strength lean Chennai Super Kings.",
              counterpartySummary:
                "Most likely the other side is thin-liquidity passive flow rather than a deeply informed sharp market.",
            },
          },
        ],
      },
    };

    const prediction = buildNextMatchPredictionView(summary);

    expect(prediction).not.toBeNull();
    expect(prediction?.recommendation).toBe("BET CHENNAI SUPER KINGS");
    const output = formatNextMatchPrediction(prediction!);
    expect(output).toContain("Next IPL match prediction");
    expect(output).toContain("Match: Chennai Super Kings vs Mumbai Indians");
    expect(output).toContain("Recommendation: BET CHENNAI SUPER KINGS");
    expect(output).toContain("Edge: 5.0c | Price: 48.0c | Fair value: 53.0c");
    expect(output).toContain("Why: Rest differential and season strength");
    expect(output).toContain(
      "Other side: Most likely the other side is thin-liquidity passive flow",
    );
  });

  it("returns null when there is no next match row", () => {
    const summary: RecurringRunSummary = {
      runKey: "next-match-empty",
      checkpointType: "pre_match",
      triggeredBy: "manual_next_match",
      ingest: {
        marketsPersisted: 0,
        cricketSnapshotsPersisted: 0,
        marketSourceIds: [],
      },
      normalize: {
        normalizedSnapshots: 0,
        checkpointSnapshots: 0,
        featureRows: 0,
      },
      map: {
        totalMarkets: 0,
        resolvedCount: 0,
        ambiguousCount: 0,
        unresolvedCount: 0,
      },
      score: {
        scoredCount: 0,
        skippedCount: 0,
        skipped: [],
      },
      report: {
        noData: true,
        rows: [],
      },
    };

    expect(buildNextMatchPredictionView(summary)).toBeNull();
  });
});
