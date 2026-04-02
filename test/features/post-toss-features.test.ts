import { describe, expect, it } from "vitest";

import {
  buildBaselinePostTossFeatureRow,
  assertNoInningsLeakageInPostTossCheckpoint,
} from "../../src/features/index.js";
import {
  baselinePostTossCheckpoint,
  baselinePreMatchFeatureContext,
} from "../fixtures/features/post-toss.js";

describe("baseline post-toss feature generation", () => {
  it("builds a post_toss feature row with toss winner/decision signals", () => {
    const featureRow = buildBaselinePostTossFeatureRow(
      baselinePostTossCheckpoint,
      baselinePreMatchFeatureContext,
    );

    expect(featureRow.checkpointType).toBe("post_toss");
    expect(featureRow.generatedAt).toBe(
      baselinePostTossCheckpoint.state.snapshotTime,
    );
    expect(featureRow.features["tossWinnerTeamName"]).toBe(
      "Royal Challengers Bengaluru",
    );
    expect(featureRow.features["tossDecision"]).toBe("bowl");
    expect(featureRow.features["tossWinnerIsTeamA"]).toBe(1);
    expect(featureRow.features["tossDecisionIsBowl"]).toBe(1);
    expect(featureRow.features["tossWinnerDecisionPair"]).toBe("team_a_bowl");
  });

  it("rejects post_toss checkpoints that leak innings-state fields", () => {
    const leakedRunsCheckpoint = {
      ...baselinePostTossCheckpoint,
      state: {
        ...baselinePostTossCheckpoint.state,
        runs: 20,
      },
    } as unknown as typeof baselinePostTossCheckpoint;

    expect(() =>
      assertNoInningsLeakageInPostTossCheckpoint(leakedRunsCheckpoint),
    ).toThrow(/Innings-state leakage/);

    const leakedPayloadCheckpoint = {
      ...baselinePostTossCheckpoint,
      state: {
        ...baselinePostTossCheckpoint.state,
        statePayload: {
          ...baselinePostTossCheckpoint.state.statePayload,
          innings: {
            status: "available",
            value: {
              runs: 20,
            },
          },
        },
      },
    } as unknown as typeof baselinePostTossCheckpoint;

    expect(() =>
      buildBaselinePostTossFeatureRow(
        leakedPayloadCheckpoint,
        baselinePreMatchFeatureContext,
      ),
    ).toThrow(/Innings-state leakage/);
  });
});
