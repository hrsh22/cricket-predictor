import { describe, expect, it } from "vitest";

import { formatRecurringRunSummaryReport } from "../../src/reporting/recurring-run-report.js";
import type { RecurringRunSummary } from "../../src/orchestration/index.js";

describe("recurring run summary report", () => {
  it("formats scored valuations with run metadata", () => {
    const summary: RecurringRunSummary = {
      runKey: "live-score-run-1",
      checkpointType: "pre_match",
      triggeredBy: "manual",
      ingest: {
        marketsPersisted: 2,
        cricketSnapshotsPersisted: 1,
        marketSourceIds: ["m1", "m2"],
      },
      normalize: {
        normalizedSnapshots: 1,
        checkpointSnapshots: 1,
        featureRows: 1,
      },
      map: {
        totalMarkets: 2,
        resolvedCount: 1,
        ambiguousCount: 1,
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
            modelKey: "baseline-pre_match-v1",
            modelVersion: "v1",
            fairWinProbability: 0.53,
            marketImpliedProbability: 0.48,
            spread: 0.05,
            note: "Social adjustment not applied (missing_signal).",
            scoredAt: "2026-03-29T13:00:00.000Z",
            teamAName: "Chennai Super Kings",
            teamBName: "Mumbai Indians",
            yesOutcomeName: "Chennai Super Kings",
          },
        ],
      },
    };

    const output = formatRecurringRunSummaryReport(summary);

    expect(output).toContain("Live IPL scoring run");
    expect(output).toContain("Run key: live-score-run-1");
    expect(output).toContain(
      "Mappings: 1 resolved / 1 ambiguous / 0 unresolved",
    );
    expect(output).toContain("CHENNAI SUPER KINGS vs MUMBAI INDIANS");
    expect(output).toContain("CSK");
    expect(output).toContain("MI");
    expect(output).toContain("Market");
    expect(output).toContain("Model");
    expect(output).toContain("53.0%");
    expect(output).toContain("48.0%");
    expect(output).toContain("+5.0pp");
    expect(output).toContain("RECOMMENDATION: BET CHENNAI SUPER KINGS");
    expect(output).toContain("Underpriced by 5.0pp");
  });

  it("renders a clear empty-state when no valuations are scored", () => {
    const summary: RecurringRunSummary = {
      runKey: "live-score-empty",
      checkpointType: "post_toss",
      triggeredBy: "manual",
      ingest: {
        marketsPersisted: 0,
        cricketSnapshotsPersisted: 1,
        marketSourceIds: [],
      },
      normalize: {
        normalizedSnapshots: 1,
        checkpointSnapshots: 1,
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
        skippedCount: 1,
        skipped: [
          {
            matchSlug: "ipl-2026-csk-vs-mi",
            reason: "missing_market",
            detail: "no matching market found",
          },
        ],
      },
      report: {
        noData: true,
        rows: [],
      },
    };

    const output = formatRecurringRunSummaryReport(summary);

    expect(output).toContain("No scored valuations produced for this run.");
    expect(output).toContain("Scores: 0 scored / 1 skipped");
  });
});
