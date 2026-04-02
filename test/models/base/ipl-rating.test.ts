import { describe, expect, it } from "vitest";

import { parseFeatureRow } from "../../../src/domain/checkpoint.js";
import { buildBaselinePreMatchFeatureRow } from "../../../src/features/index.js";
import {
  assertCoherentTwoOutcomeProbabilities,
  scoreBaselineIplPreMatch,
} from "../../../src/models/base/index.js";
import {
  baselinePreMatchCheckpoint,
  baselinePreMatchFeatureContext,
} from "../../fixtures/features/pre-match.js";

describe("baseline IPL rating model", () => {
  it("produces coherent two-outcome probabilities in [0, 1]", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const score = scoreBaselineIplPreMatch(featureRow);

    expect(score.teamAWinProbability).toBeGreaterThanOrEqual(0);
    expect(score.teamAWinProbability).toBeLessThanOrEqual(1);
    expect(score.teamBWinProbability).toBeGreaterThanOrEqual(0);
    expect(score.teamBWinProbability).toBeLessThanOrEqual(1);
    expect(score.teamAWinProbability + score.teamBWinProbability).toBe(1);

    expect(score.teamAWinProbability).toBeGreaterThan(0.5);
    expect(score.generatedAt).toBe(featureRow.generatedAt);
    expect(score.checkpointType).toBe("pre_match");
  });

  it("keeps probability output deterministic for identical feature rows", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const scoreA = scoreBaselineIplPreMatch(featureRow);
    const scoreB = scoreBaselineIplPreMatch(featureRow);

    expect(scoreA).toEqual(scoreB);
  });

  it("rejects feature rows that leak market odds", () => {
    const leakedFeatureRow = parseFeatureRow({
      matchSlug: "ipl-2026-rcb-vs-kkr",
      checkpointType: "pre_match",
      featureSetVersion: "baseline_pre_match_v2",
      generatedAt: "2026-04-05T12:00:00.000Z",
      features: {
        ratingDiff: 25,
        formDiff: 0.1,
        venueDiff: 0,
        headToHeadDiff: 0,
        restDiff: 2,
        congestionDiff: -1,
        marketOdds: 0.61,
      },
    });

    expect(() => scoreBaselineIplPreMatch(leakedFeatureRow)).toThrow(
      /Market-odds leakage/,
    );
  });

  it("rejects non-coherent two-outcome probabilities", () => {
    expect(() => assertCoherentTwoOutcomeProbabilities(0.8, 0.15)).toThrow(
      /must sum to 1/,
    );
  });

  it("applies optional platt calibration when provided", () => {
    const featureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    const uncalibrated = scoreBaselineIplPreMatch(featureRow);
    const calibrated = scoreBaselineIplPreMatch(featureRow, {
      plattCalibration: {
        intercept: -0.1,
        slope: 0.9,
      },
    });

    expect(calibrated.teamAWinProbability).not.toBe(
      uncalibrated.teamAWinProbability,
    );
    expect(
      (calibrated.scoreBreakdown as Record<string, unknown>)[
        "plattCalibrationApplied"
      ],
    ).toBe(1);
  });
});
