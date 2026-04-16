import { describe, expect, it } from "vitest";

import {
  formatActiveIplValuationReport,
  type ActiveIplValuationReport,
} from "../../src/reporting/index.js";

describe("active IPL valuation reporting", () => {
  it("sorts rows by spread and keeps lineage / social notes in the payload", () => {
    const report = buildReport([
      createReportItem({
        matchSlug: "ipl-match-b",
        spread: 0.028,
        fairProbability: 0.58,
        marketProbability: 0.552,
        modelVersion: "v2",
        note: "Social adjustment applied toward yes outcome.",
        tradeThesis: {
          position: "bet_yes",
          outcomeName: "Team A",
          edgeCents: 2.8,
          contractPriceCents: 55.2,
          fairValueCents: 58,
          conviction: "tradable",
          mispricingSummary: "Rating edge leans Team A.",
          counterpartySummary:
            "Most likely the other side is ordinary balanced flow, which makes this edge fragile unless you get a better entry.",
        },
      }),
      createReportItem({
        matchSlug: "ipl-match-a",
        spread: 0.112,
        fairProbability: 0.64,
        marketProbability: 0.528,
        modelVersion: "v3",
        note: "Innings-break social adjustment +5.0%.",
        tradeThesis: {
          position: "bet_no",
          outcomeName: "Team B",
          edgeCents: 11.2,
          contractPriceCents: 47.2,
          fairValueCents: 58.4,
          conviction: "strong",
          mispricingSummary:
            "Team A looks slightly overbid in the mid-favorite band.",
          counterpartySummary:
            "Most likely the other side is favorite-leaning or narrative-driven buyers, with passive counterparties willing to sell them this price.",
        },
      }),
    ]);

    expect(report.items.map((item) => item.matchSlug)).toEqual([
      "ipl-match-a",
      "ipl-match-b",
    ]);
    expect(report.items[0]?.model.version).toBe("v3");
    expect(report.items[0]?.explanationNote).toContain("Innings-break");
    expect(report.items[1]?.social.status).toBe("applied");

    const rendered = formatActiveIplValuationReport(report);
    expect(rendered).toContain("Active IPL valuations");
    expect(rendered).toContain("spread: +11.2pp");
    expect(rendered).toContain("spread: +2.8pp");
    expect(rendered).toContain("edge: 11.2c");
    expect(rendered).toContain("why: Team A looks slightly overbid");
    expect(rendered).toContain(
      "other side: Most likely the other side is favorite-leaning",
    );
    expect(rendered.indexOf("ipl-match-a")).toBeLessThan(
      rendered.indexOf("ipl-match-b"),
    );
  });

  it("renders an explicit empty state when there are no active valuations", () => {
    const report = buildReport([]);

    expect(report.totalCount).toBe(0);
    expect(report.emptyMessage).toBe("No active IPL valuations found.");
    expect(formatActiveIplValuationReport(report)).toContain(
      "No active IPL valuations found.",
    );
  });
});

function buildReport(
  items: ActiveIplValuationReport["items"],
): ActiveIplValuationReport {
  const sortedItems = [...items].sort((left, right) => {
    const leftSpread = left.spread ?? Number.NEGATIVE_INFINITY;
    const rightSpread = right.spread ?? Number.NEGATIVE_INFINITY;

    if (leftSpread !== rightSpread) {
      return rightSpread - leftSpread;
    }

    return left.matchSlug.localeCompare(right.matchSlug);
  });

  return {
    generatedAt: "2026-03-29T15:00:00.000Z",
    totalCount: sortedItems.length,
    items: sortedItems,
    emptyMessage:
      sortedItems.length === 0 ? "No active IPL valuations found." : null,
  };
}

function createReportItem(input: {
  matchSlug: string;
  spread: number;
  fairProbability: number;
  marketProbability: number;
  modelVersion: string;
  note: string;
  tradeThesis: {
    position: "bet_yes" | "bet_no" | "hold";
    outcomeName: string;
    edgeCents: number;
    contractPriceCents: number;
    fairValueCents: number;
    conviction: "fragile" | "tradable" | "strong";
    mispricingSummary: string;
    counterpartySummary: string;
  };
}) {
  return {
    matchSlug: input.matchSlug,
    checkpoint: {
      type: "pre_match" as const,
      snapshotTime: "2026-03-29T14:00:00.000Z",
      checkpointStateId: 11,
    },
    model: {
      key: `model-${input.modelVersion}`,
      family: "baseline-rating",
      version: input.modelVersion,
      registryId: 22,
    },
    scoringRunKey: "run-001",
    scoredAt: "2026-03-29T14:05:00.000Z",
    fairProbability: input.fairProbability,
    marketProbability: input.marketProbability,
    spread: input.spread,
    social: {
      applied: true,
      mode: "enabled",
      status: "applied",
      note: input.note,
      sourceProviderKey: "manual",
      sourceId: "social-1",
    },
    tradeThesis: input.tradeThesis,
    explanationNote: input.note,
  };
}
