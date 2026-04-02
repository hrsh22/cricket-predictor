import { describe, expect, it } from "vitest";

import {
  assertNoMarketOddsFeatures,
  buildBaselinePreMatchFeatureRow,
  createVenueStrengthKey,
} from "../../src/features/index.js";
import {
  baselinePreMatchCheckpoint,
  baselinePreMatchFeatureContext,
} from "../fixtures/features/pre-match.js";

describe("baseline pre-match feature generation", () => {
  it("builds deterministic pre-match feature rows from cricket-only inputs", () => {
    const featureRowA = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );
    const featureRowB = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      baselinePreMatchFeatureContext,
    );

    expect(featureRowA).toEqual(featureRowB);
    expect(featureRowA.generatedAt).toBe(
      baselinePreMatchCheckpoint.state.snapshotTime,
    );
    expect(featureRowA.checkpointType).toBe("pre_match");

    expect(featureRowA.features["ratingDiff"]).toBe(45);
    expect(featureRowA.features["formDiff"]).toBe(0.4);
    expect(featureRowA.features["venueDiff"]).toBe(0.25);
    expect(featureRowA.features["restDiff"]).toBe(3);
    expect(featureRowA.features["congestionDiff"]).toBe(-1);
    expect(featureRowA.features["headToHeadDiff"]).toBe(0.24);
    expect(featureRowA.features["seasonWinRateDiff"]).toBe(0.3);
    expect(featureRowA.features["seasonMatchesPlayedDiffNormalized"]).toBe(0);
    expect(featureRowA.features["seasonWinStrengthDiff"]).toBe(0.12);

    expect(
      (featureRowA.features["source"] as Record<string, unknown>)[
        "cricketOnlyInputs"
      ],
    ).toBe(true);
  });

  it("falls back to neutral defaults for missing ratings/form/schedule/venue context", () => {
    const neutralFeatureRow = buildBaselinePreMatchFeatureRow(
      baselinePreMatchCheckpoint,
      {
        teamRatings: {},
        teamRatingDeviations: {},
        teamRecentForm: {},
        teamSchedule: {},
        teamSeasonContext: {},
        teamVenueStrength: {
          [createVenueStrengthKey("some-team", "some-venue")]: 0.2,
        },
        teamHeadToHeadStrength: {},
        venueTossDecisionWinRate: {},
        teamLineupContext: {},
        teamRoleCompositionContext: {},
      },
    );

    expect(neutralFeatureRow.features["teamARating"]).toBe(1500);
    expect(neutralFeatureRow.features["teamBRating"]).toBe(1500);
    expect(neutralFeatureRow.features["ratingDiff"]).toBe(0);
    expect(neutralFeatureRow.features["teamAFormWinRate"]).toBe(0.5);
    expect(neutralFeatureRow.features["teamBFormWinRate"]).toBe(0.5);
    expect(neutralFeatureRow.features["venueDiff"]).toBe(0);
    expect(neutralFeatureRow.features["restDiff"]).toBe(0);
    expect(neutralFeatureRow.features["headToHeadDiff"]).toBe(0);
    expect(neutralFeatureRow.features["teamASeasonMatchesPlayed"]).toBe(0);
    expect(neutralFeatureRow.features["teamBSeasonMatchesPlayed"]).toBe(0);
    expect(neutralFeatureRow.features["seasonWinRateDiff"]).toBe(0);
    expect(
      neutralFeatureRow.features["seasonMatchesPlayedDiffNormalized"],
    ).toBe(0);
    expect(neutralFeatureRow.features["seasonWinStrengthDiff"]).toBe(0);
  });

  it("rejects market-odds leakage keys from the structured feature payload", () => {
    expect(() =>
      assertNoMarketOddsFeatures({
        teamARating: 1520,
        nested: {
          marketImpliedProbability: 0.57,
        },
      }),
    ).toThrow(/Market-odds leakage/);
  });
});
