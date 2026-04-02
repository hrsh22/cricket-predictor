import {
  parseCanonicalCheckpoint,
  type CanonicalCheckpoint,
} from "../../../src/domain/checkpoint.js";
import {
  baselinePreMatchFeatureContext,
  baselinePreMatchCheckpoint,
} from "./pre-match.js";

export const baselinePostTossCheckpoint: CanonicalCheckpoint =
  parseCanonicalCheckpoint({
    checkpointType: "post_toss",
    match: {
      ...baselinePreMatchCheckpoint.match,
      status: "scheduled",
      tossWinnerTeamName: "Royal Challengers Bengaluru",
      tossDecision: "bowl",
    },
    state: {
      ...baselinePreMatchCheckpoint.state,
      checkpointType: "post_toss",
      sourceMarketSnapshotId: 3002,
      statePayload: {
        provider: "cricapi",
        sourceMatchId: "ipl-2026-101",
        toss: {
          status: "available",
          value: {
            tossWinnerTeamName: "Royal Challengers Bengaluru",
            tossDecision: "bowl",
          },
          issues: [],
        },
      },
    },
  });

export const tossAgainstTeamACheckpoint: CanonicalCheckpoint =
  parseCanonicalCheckpoint({
    checkpointType: "post_toss",
    match: {
      ...baselinePreMatchCheckpoint.match,
      status: "scheduled",
      tossWinnerTeamName: "Kolkata Knight Riders",
      tossDecision: "bat",
    },
    state: {
      ...baselinePreMatchCheckpoint.state,
      checkpointType: "post_toss",
      sourceMarketSnapshotId: 3003,
      statePayload: {
        provider: "cricapi",
        sourceMatchId: "ipl-2026-101",
        toss: {
          status: "available",
          value: {
            tossWinnerTeamName: "Kolkata Knight Riders",
            tossDecision: "bat",
          },
          issues: [],
        },
      },
    },
  });

export { baselinePreMatchFeatureContext };
