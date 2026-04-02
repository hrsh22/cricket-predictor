import { describe, expect, it } from "vitest";

import {
  INNINGS_BREAK_FEATURE_SET_VERSION,
  buildInningsBreakFeatureRow,
} from "../../src/features/index.js";
import {
  inningsBreakCheckpoint,
  withInningsBreakStatePatch,
} from "../fixtures/features/innings-break.js";

describe("innings-break feature generation", () => {
  it("builds innings-break features when first-innings state is complete", () => {
    const result = buildInningsBreakFeatureRow(inningsBreakCheckpoint);

    expect(result.status).toBe("ready");

    if (result.status !== "ready") {
      throw new Error("expected innings-break feature generation to be ready");
    }

    expect(result.featureRow.checkpointType).toBe("innings_break");
    expect(result.featureRow.featureSetVersion).toBe(
      INNINGS_BREAK_FEATURE_SET_VERSION,
    );
    expect(result.featureRow.generatedAt).toBe(
      inningsBreakCheckpoint.state.snapshotTime,
    );
    expect(result.featureRow.features["firstInningsRuns"]).toBe(176);
    expect(result.featureRow.features["firstInningsWickets"]).toBe(6);
    expect(result.featureRow.features["firstInningsOvers"]).toBe(20);
    expect(result.featureRow.features["targetRuns"]).toBe(177);
  });

  it("skips when innings payload is incomplete", () => {
    const incompleteCheckpoint = withInningsBreakStatePatch({
      statePayload: {
        innings: {
          status: "degraded",
          issues: [{ path: "cricapi.score", message: "missing overs" }],
        },
      },
    });

    const result = buildInningsBreakFeatureRow(incompleteCheckpoint);

    expect(result).toEqual({
      status: "skipped",
      reason: "incomplete_state",
      detail:
        "innings-break scoring requires complete toss and innings payload state",
    });
  });

  it("skips DLS/reduced-over payloads as unsupported provider coverage", () => {
    const dlsCheckpoint = withInningsBreakStatePatch({
      statePayload: {
        coverage: {
          dlsApplied: true,
          noResult: false,
          superOver: false,
          reducedOvers: true,
          incomplete: false,
        },
      },
    });

    const result = buildInningsBreakFeatureRow(dlsCheckpoint);

    expect(result.status).toBe("skipped");
    if (result.status !== "skipped") {
      throw new Error("expected innings-break feature generation to skip");
    }

    expect(result.reason).toBe("unsupported_provider_coverage");
  });

  it("skips abbreviated non all-out innings states", () => {
    const abbreviatedCheckpoint = withInningsBreakStatePatch({
      overs: 17,
      wickets: 6,
    });

    const result = buildInningsBreakFeatureRow(abbreviatedCheckpoint);

    expect(result.status).toBe("skipped");
    if (result.status !== "skipped") {
      throw new Error("expected innings-break feature generation to skip");
    }

    expect(result.reason).toBe("abbreviated_innings_not_supported");
  });
});
