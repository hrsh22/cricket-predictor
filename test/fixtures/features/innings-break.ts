import {
  parseCanonicalCheckpoint,
  type CanonicalCheckpoint,
} from "../../../src/domain/index.js";

const baseInningsBreakCheckpointPayload = {
  checkpointType: "innings_break",
  match: {
    competition: "IPL",
    matchSlug: "ipl-2026-csk-vs-mi-innings-break",
    sourceMatchId: "ipl-2026-ib-001",
    season: 2026,
    scheduledStart: "2026-03-29T14:00:00.000Z",
    teamAName: "Chennai Super Kings",
    teamBName: "Mumbai Indians",
    venueName: "MA Chidambaram Stadium, Chennai",
    status: "in_progress",
    tossWinnerTeamName: "Mumbai Indians",
    tossDecision: "bowl",
    winningTeamName: null,
    resultType: null,
  },
  state: {
    matchSlug: "ipl-2026-csk-vs-mi-innings-break",
    checkpointType: "innings_break",
    snapshotTime: "2026-03-29T15:20:00.000Z",
    stateVersion: 1,
    sourceMarketSnapshotId: 4101,
    sourceCricketSnapshotId: 5101,
    inningsNumber: 1,
    battingTeamName: "Chennai Super Kings",
    bowlingTeamName: "Mumbai Indians",
    runs: 176,
    wickets: 6,
    overs: 20,
    targetRuns: 177,
    currentRunRate: 8.8,
    requiredRunRate: 8.85,
    statePayload: {
      provider: "cricapi",
      lifecycle: "innings_break",
      innings: {
        status: "available",
        value: {
          inningsNumber: 1,
          runs: 176,
          wickets: 6,
          overs: 20,
        },
      },
      toss: {
        status: "available",
        value: {
          tossWinnerTeamName: "Mumbai Indians",
          tossDecision: "bowl",
        },
      },
      coverage: {
        dlsApplied: false,
        noResult: false,
        superOver: false,
        reducedOvers: false,
        incomplete: false,
      },
    },
  },
} as const;

export const inningsBreakCheckpoint = parseCanonicalCheckpoint(
  baseInningsBreakCheckpointPayload,
);

export function withInningsBreakStatePatch(
  patch: Partial<CanonicalCheckpoint["state"]> & {
    statePayload?: Record<string, unknown>;
  },
): CanonicalCheckpoint {
  return parseCanonicalCheckpoint({
    ...baseInningsBreakCheckpointPayload,
    state: {
      ...baseInningsBreakCheckpointPayload.state,
      ...patch,
      statePayload: {
        ...baseInningsBreakCheckpointPayload.state.statePayload,
        ...(patch.statePayload ?? {}),
      },
    },
  });
}
