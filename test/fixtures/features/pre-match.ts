import {
  parseCanonicalCheckpoint,
  type CanonicalCheckpoint,
} from "../../../src/domain/checkpoint.js";
import {
  createVenueTossDecisionKey,
  createHeadToHeadKey,
  createTeamSeasonKey,
  createVenueStrengthKey,
  type PreMatchFeatureContext,
} from "../../../src/features/pre-match.js";

export const baselinePreMatchCheckpoint: CanonicalCheckpoint =
  parseCanonicalCheckpoint({
    checkpointType: "pre_match",
    match: {
      competition: "IPL",
      matchSlug: "ipl-2026-rcb-vs-kkr",
      sourceMatchId: "ipl-2026-101",
      season: 2026,
      scheduledStart: "2026-04-05T14:00:00.000Z",
      teamAName: "Royal Challengers Bengaluru",
      teamBName: "Kolkata Knight Riders",
      venueName: "M. Chinnaswamy Stadium",
      status: "scheduled",
      tossWinnerTeamName: null,
      tossDecision: null,
      winningTeamName: null,
      resultType: null,
    },
    state: {
      matchSlug: "ipl-2026-rcb-vs-kkr",
      checkpointType: "pre_match",
      snapshotTime: "2026-04-05T12:00:00.000Z",
      stateVersion: 1,
      sourceMarketSnapshotId: null,
      sourceCricketSnapshotId: 3001,
      inningsNumber: null,
      battingTeamName: null,
      bowlingTeamName: null,
      runs: null,
      wickets: null,
      overs: null,
      targetRuns: null,
      currentRunRate: null,
      requiredRunRate: null,
      statePayload: {
        provider: "cricapi",
        sourceMatchId: "ipl-2026-101",
      },
    },
  });

export const baselinePreMatchFeatureContext: PreMatchFeatureContext = {
  teamRatings: {
    "Royal Challengers Bengaluru": 1550,
    "Kolkata Knight Riders": 1505,
  },
  teamRatingDeviations: {
    "Royal Challengers Bengaluru": 300,
    "Kolkata Knight Riders": 300,
  },
  teamRecentForm: {
    "Royal Challengers Bengaluru": {
      wins: 4,
      matches: 5,
    },
    "Kolkata Knight Riders": {
      wins: 2,
      matches: 5,
    },
  },
  teamSchedule: {
    "Royal Challengers Bengaluru": {
      daysSincePreviousMatch: 6,
      matchesInPrevious7Days: 1,
    },
    "Kolkata Knight Riders": {
      daysSincePreviousMatch: 3,
      matchesInPrevious7Days: 2,
    },
  },
  teamSeasonContext: {
    [createTeamSeasonKey("Royal Challengers Bengaluru", 2026)]: {
      wins: 3,
      matchesPlayed: 4,
      weightedWinRate: 0.65,
    },
    [createTeamSeasonKey("Kolkata Knight Riders", 2026)]: {
      wins: 1,
      matchesPlayed: 4,
      weightedWinRate: 0.35,
    },
  },
  teamVenueStrength: {
    [createVenueStrengthKey(
      "Royal Challengers Bengaluru",
      "M. Chinnaswamy Stadium",
    )]: 0.17,
    [createVenueStrengthKey("Kolkata Knight Riders", "M. Chinnaswamy Stadium")]:
      -0.08,
  },
  teamHeadToHeadStrength: {
    [createHeadToHeadKey(
      "Royal Challengers Bengaluru",
      "Kolkata Knight Riders",
    )]: 0.12,
    [createHeadToHeadKey(
      "Kolkata Knight Riders",
      "Royal Challengers Bengaluru",
    )]: -0.12,
  },
  venueTossDecisionWinRate: {
    [createVenueTossDecisionKey("M. Chinnaswamy Stadium", "bowl")]: 0.56,
    [createVenueTossDecisionKey("M. Chinnaswamy Stadium", "bat")]: 0.49,
  },
  teamLineupContext: {
    "Royal Challengers Bengaluru": {
      matches: 5,
      stability: 0.78,
      continuity: 0.82,
      rotation: 0.18,
    },
    "Kolkata Knight Riders": {
      matches: 5,
      stability: 0.61,
      continuity: 0.66,
      rotation: 0.34,
    },
  },
  teamRoleCompositionContext: {
    "Royal Challengers Bengaluru": {
      matches: 5,
      bowlerShare: 0.36,
      allRounderShare: 0.27,
    },
    "Kolkata Knight Riders": {
      matches: 5,
      bowlerShare: 0.42,
      allRounderShare: 0.18,
    },
  },
  teamStyleCompositionContext: {
    "Royal Challengers Bengaluru": {
      players: 25,
      leftHandBatShare: 0.28,
      paceBowlerShare: 0.44,
      spinBowlerShare: 0.24,
    },
    "Kolkata Knight Riders": {
      players: 25,
      leftHandBatShare: 0.36,
      paceBowlerShare: 0.4,
      spinBowlerShare: 0.28,
    },
  },
};
