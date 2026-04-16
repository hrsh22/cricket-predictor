import {
  parseFeatureRow,
  type CanonicalCheckpoint,
  type FeatureRow,
} from "../domain/checkpoint.js";
import type { JsonObject, JsonValue } from "../domain/primitives.js";
import { computePitchConditionFeatures } from "./pitch-conditions.js";
import { computeVenueConditionsFeatures } from "./venue-conditions.js";

export const BASELINE_PRE_MATCH_FEATURE_SET_VERSION = "baseline_pre_match_v2";
export const DEFAULT_IPL_TEAM_RATING = 1500;
export const DEFAULT_IPL_TEAM_RD = 300;

const FORBIDDEN_MARKET_FEATURE_PATTERN =
  /(odds?|implied|marketimplied|market_probability|marketprobability|lasttradeprice|price|polymarket|book|vig|tokenid)/i;

export interface TeamRecentForm {
  wins: number;
  matches: number;
  weightedWinRate?: number;
}

export interface TeamScheduleContext {
  daysSincePreviousMatch: number | null;
  matchesInPrevious7Days: number;
}

export interface TeamSeasonContext {
  wins: number;
  matchesPlayed: number;
  weightedWinRate?: number;
}

export interface TeamLineupContext {
  matches: number;
  stability: number;
  continuity: number;
  rotation: number;
}

export interface TeamRoleCompositionContext {
  matches: number;
  bowlerShare: number;
  allRounderShare: number;
}

export interface TeamStyleCompositionContext {
  players: number;
  leftHandBatShare: number;
  paceBowlerShare: number;
  spinBowlerShare: number;
}

export interface PreMatchFeatureContext {
  teamRatings: Record<string, number>;
  teamRatingDeviations: Record<string, number>;
  teamRecentForm: Record<string, TeamRecentForm>;
  teamSchedule: Record<string, TeamScheduleContext>;
  teamSeasonContext?: Record<string, TeamSeasonContext>;
  teamVenueStrength: Record<string, number>;
  teamHeadToHeadStrength: Record<string, number>;
  venueTossDecisionWinRate: Record<string, number>;
  teamLineupContext: Record<string, TeamLineupContext>;
  teamRoleCompositionContext: Record<string, TeamRoleCompositionContext>;
  teamStyleCompositionContext: Record<string, TeamStyleCompositionContext>;
}

export function createVenueTossDecisionKey(
  venueName: string,
  tossDecision: "bat" | "bowl",
): string {
  return `${normalizeKeyToken(venueName)}::${tossDecision}`;
}

export function createVenueStrengthKey(
  teamName: string,
  venueName: string,
): string {
  return `${normalizeKeyToken(teamName)}::${normalizeKeyToken(venueName)}`;
}

export function createTeamSeasonKey(teamName: string, season: number): string {
  return `${normalizeKeyToken(teamName)}::${season}`;
}

export function buildBaselinePreMatchFeatureRow(
  checkpoint: CanonicalCheckpoint,
  context: PreMatchFeatureContext,
): FeatureRow {
  if (checkpoint.checkpointType !== "pre_match") {
    throw new Error(
      "Baseline pre-match feature generation requires a pre_match checkpoint.",
    );
  }

  const match = checkpoint.match;
  const venueName = match.venueName ?? "unknown";

  const teamARating = readTeamRating(context.teamRatings, match.teamAName);
  const teamBRating = readTeamRating(context.teamRatings, match.teamBName);
  const ratingDiff = roundTo(teamARating - teamBRating, 6);

  const teamARD = readTeamRD(context.teamRatingDeviations, match.teamAName);
  const teamBRD = readTeamRD(context.teamRatingDeviations, match.teamBName);
  const combinedRD = roundTo(
    Math.sqrt(teamARD * teamARD + teamBRD * teamBRD),
    6,
  );

  const teamAForm = readTeamForm(context.teamRecentForm, match.teamAName);
  const teamBForm = readTeamForm(context.teamRecentForm, match.teamBName);
  const teamAFormWinRate = computeWinRate(teamAForm);
  const teamBFormWinRate = computeWinRate(teamBForm);
  const formDiff = roundTo(teamAFormWinRate - teamBFormWinRate, 6);

  const teamAVenueStrength = readVenueStrength(
    context.teamVenueStrength,
    match.teamAName,
    venueName,
  );
  const teamBVenueStrength = readVenueStrength(
    context.teamVenueStrength,
    match.teamBName,
    venueName,
  );
  const venueDiff = roundTo(teamAVenueStrength - teamBVenueStrength, 6);

  const teamASchedule = readScheduleContext(
    context.teamSchedule,
    match.teamAName,
  );
  const teamBSchedule = readScheduleContext(
    context.teamSchedule,
    match.teamBName,
  );

  const teamARestDays = normalizeRestDays(teamASchedule.daysSincePreviousMatch);
  const teamBRestDays = normalizeRestDays(teamBSchedule.daysSincePreviousMatch);
  const restDiff = roundTo(teamARestDays - teamBRestDays, 6);

  const teamARecentMatchLoad = Math.max(
    0,
    teamASchedule.matchesInPrevious7Days,
  );
  const teamBRecentMatchLoad = Math.max(
    0,
    teamBSchedule.matchesInPrevious7Days,
  );
  const congestionDiff = roundTo(
    teamARecentMatchLoad - teamBRecentMatchLoad,
    6,
  );

  const teamAHeadToHeadStrength = readHeadToHeadStrength(
    context.teamHeadToHeadStrength,
    match.teamAName,
    match.teamBName,
  );
  const teamBHeadToHeadStrength = readHeadToHeadStrength(
    context.teamHeadToHeadStrength,
    match.teamBName,
    match.teamAName,
  );
  const headToHeadDiff = roundTo(
    teamAHeadToHeadStrength - teamBHeadToHeadStrength,
    6,
  );

  const teamASeason = readTeamSeasonContext(
    context.teamSeasonContext,
    match.teamAName,
    match.season,
  );
  const teamBSeason = readTeamSeasonContext(
    context.teamSeasonContext,
    match.teamBName,
    match.season,
  );
  const teamASeasonWinRate = computeSeasonWinRate(teamASeason);
  const teamBSeasonWinRate = computeSeasonWinRate(teamBSeason);
  const seasonWinRateDiff = roundTo(teamASeasonWinRate - teamBSeasonWinRate, 6);
  const seasonMatchesPlayedDiffNormalized = roundTo(
    normalizeSampleDiff(teamASeason.matchesPlayed, teamBSeason.matchesPlayed),
    6,
  );
  const teamASeasonWinStrength = computeSeasonWinStrength(
    teamASeasonWinRate,
    teamASeason.matchesPlayed,
  );
  const teamBSeasonWinStrength = computeSeasonWinStrength(
    teamBSeasonWinRate,
    teamBSeason.matchesPlayed,
  );
  const seasonWinStrengthDiff = roundTo(
    teamASeasonWinStrength - teamBSeasonWinStrength,
    6,
  );
  const teamALineup = readTeamLineupContext(
    context.teamLineupContext,
    match.teamAName,
  );
  const teamBLineup = readTeamLineupContext(
    context.teamLineupContext,
    match.teamBName,
  );
  const lineupStabilityDiff = roundTo(
    teamALineup.stability - teamBLineup.stability,
    6,
  );
  const lineupContinuityDiff = roundTo(
    teamALineup.continuity - teamBLineup.continuity,
    6,
  );
  const lineupRotationEdge = roundTo(
    teamBLineup.rotation - teamALineup.rotation,
    6,
  );
  const teamARoleComposition = readTeamRoleCompositionContext(
    context.teamRoleCompositionContext,
    match.teamAName,
  );
  const teamBRoleComposition = readTeamRoleCompositionContext(
    context.teamRoleCompositionContext,
    match.teamBName,
  );
  const bowlerShareDiff = roundTo(
    teamARoleComposition.bowlerShare - teamBRoleComposition.bowlerShare,
    6,
  );
  const allRounderShareDiff = roundTo(
    teamARoleComposition.allRounderShare - teamBRoleComposition.allRounderShare,
    6,
  );
  const teamAStyleComposition = readTeamStyleCompositionContext(
    context.teamStyleCompositionContext,
    match.teamAName,
  );
  const teamBStyleComposition = readTeamStyleCompositionContext(
    context.teamStyleCompositionContext,
    match.teamBName,
  );
  const leftHandBatShareDiff = roundTo(
    teamAStyleComposition.leftHandBatShare -
      teamBStyleComposition.leftHandBatShare,
    6,
  );
  const paceBowlerShareDiff = roundTo(
    teamAStyleComposition.paceBowlerShare -
      teamBStyleComposition.paceBowlerShare,
    6,
  );
  const spinBowlerShareDiff = roundTo(
    teamAStyleComposition.spinBowlerShare -
      teamBStyleComposition.spinBowlerShare,
    6,
  );
  const leftBatVsOppSpinDiff = roundTo(
    teamAStyleComposition.leftHandBatShare *
      teamBStyleComposition.spinBowlerShare -
      teamBStyleComposition.leftHandBatShare *
        teamAStyleComposition.spinBowlerShare,
    6,
  );
  const leftBatVsOppPaceDiff = roundTo(
    teamAStyleComposition.leftHandBatShare *
      teamBStyleComposition.paceBowlerShare -
      teamBStyleComposition.leftHandBatShare *
        teamAStyleComposition.paceBowlerShare,
    6,
  );

  const scheduledDate = new Date(match.scheduledStart);
  const scheduledHourUtc = scheduledDate.getUTCHours();
  const scheduledWeekdayUtc = scheduledDate.getUTCDay();
  const isWeekendUtc =
    scheduledWeekdayUtc === 0 || scheduledWeekdayUtc === 6 ? 1 : 0;

  const venueConditions = computeVenueConditionsFeatures(
    match.venueName,
    match.teamAName,
    match.teamBName,
    scheduledDate,
  );
  const pitchConditions = computePitchConditionFeatures(match.venueName);

  const features: JsonObject = {
    teamAName: match.teamAName,
    teamBName: match.teamBName,
    venueName,
    teamARating,
    teamBRating,
    ratingDiff,
    teamARD,
    teamBRD,
    combinedRD,
    teamAFormWinRate,
    teamBFormWinRate,
    formDiff,
    teamAFormMatches: teamAForm.matches,
    teamBFormMatches: teamBForm.matches,
    teamAVenueStrength,
    teamBVenueStrength,
    venueDiff,
    teamARestDays,
    teamBRestDays,
    restDiff,
    teamARecentMatchLoad,
    teamBRecentMatchLoad,
    congestionDiff,
    teamAHeadToHeadStrength,
    teamBHeadToHeadStrength,
    headToHeadDiff,
    teamASeasonMatchesPlayed: teamASeason.matchesPlayed,
    teamBSeasonMatchesPlayed: teamBSeason.matchesPlayed,
    seasonMatchesPlayedDiffNormalized,
    teamASeasonWinRate,
    teamBSeasonWinRate,
    seasonWinRateDiff,
    teamASeasonWinStrength,
    teamBSeasonWinStrength,
    seasonWinStrengthDiff,
    teamALineupMatchesKnown: teamALineup.matches,
    teamBLineupMatchesKnown: teamBLineup.matches,
    lineupStabilityDiff,
    lineupContinuityDiff,
    lineupRotationEdge,
    bowlerShareDiff,
    allRounderShareDiff,
    leftHandBatShareDiff,
    paceBowlerShareDiff,
    spinBowlerShareDiff,
    leftBatVsOppSpinDiff,
    leftBatVsOppPaceDiff,
    scheduledHourUtc,
    scheduledWeekdayUtc,
    isWeekendUtc,
    dewFactor: venueConditions.dewFactor,
    homeAdvantageDiff: venueConditions.homeAdvantageDiff,
    teamAHomeAdvantage: venueConditions.homeAdvantageTeamA,
    teamBHomeAdvantage: venueConditions.homeAdvantageTeamB,
    isEveningMatch: venueConditions.isEveningMatch ? 1 : 0,
    matchMonth: venueConditions.matchMonth,
    pitchType: pitchConditions.pitchType,
    pitchBattingIndex: pitchConditions.pitchBattingIndex,
    isSpinFriendlyPitch: pitchConditions.isSpinFriendly ? 1 : 0,
    isBattingFriendlyPitch: pitchConditions.isBattingFriendly ? 1 : 0,
    source: {
      featureScope: "cricket_only_structured",
      cricketOnlyInputs: true,
      ratingsSource: "elo_team_ratings",
      ratingDeviationsSource: "glicko2_rating_deviations",
      formSource: "provided_recent_form",
      scheduleSource: "provided_schedule_context",
      teamSeasonSource: "provided_team_season_context",
      teamLineupSource: "historical_team_lineup_context",
      teamRoleCompositionSource: "historical_team_role_composition_context",
      teamStyleCompositionSource: "historical_team_style_composition_context",
      venueSource: "provided_venue_strength",
      headToHeadSource: "provided_head_to_head_strength",
      venueConditionsSource: "computed_venue_conditions",
      pitchConditionsSource: "computed_pitch_conditions",
    },
  };

  assertNoMarketOddsFeatures(features);

  return parseFeatureRow({
    matchSlug: checkpoint.match.matchSlug,
    checkpointType: "pre_match",
    featureSetVersion: BASELINE_PRE_MATCH_FEATURE_SET_VERSION,
    generatedAt: checkpoint.state.snapshotTime,
    features,
  });
}

export function assertNoMarketOddsFeatures(
  features: JsonObject,
  path = "features",
): void {
  scanJsonValueForMarketOddsLeakage(features, path);
}

function scanJsonValueForMarketOddsLeakage(
  value: JsonValue,
  path: string,
): void {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => {
      scanJsonValueForMarketOddsLeakage(entry, `${path}[${index}]`);
    });
    return;
  }

  if (
    value === null ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return;
  }

  if (typeof value === "string") {
    if (FORBIDDEN_MARKET_FEATURE_PATTERN.test(value)) {
      throw new Error(
        `Market-odds leakage is not allowed in baseline features (${path}).`,
      );
    }
    return;
  }

  for (const [key, entry] of Object.entries(value)) {
    if (FORBIDDEN_MARKET_FEATURE_PATTERN.test(key)) {
      throw new Error(
        `Market-odds leakage is not allowed in baseline features (${path}.${key}).`,
      );
    }

    scanJsonValueForMarketOddsLeakage(entry, `${path}.${key}`);
  }
}

function readTeamRating(
  ratings: Record<string, number>,
  teamName: string,
): number {
  const rawRating = ratings[teamName];
  if (typeof rawRating !== "number" || !Number.isFinite(rawRating)) {
    return DEFAULT_IPL_TEAM_RATING;
  }

  return roundTo(rawRating, 6);
}

function readTeamRD(
  rds: Record<string, number> | undefined,
  teamName: string,
): number {
  if (!rds) {
    return DEFAULT_IPL_TEAM_RD;
  }

  const rawRD = rds[teamName];
  if (typeof rawRD !== "number" || !Number.isFinite(rawRD)) {
    return DEFAULT_IPL_TEAM_RD;
  }

  return roundTo(rawRD, 6);
}

function readTeamForm(
  forms: Record<string, TeamRecentForm>,
  teamName: string,
): TeamRecentForm {
  const rawForm = forms[teamName];
  if (rawForm === undefined) {
    return { wins: 0, matches: 0 };
  }

  return {
    wins: Math.max(0, Math.floor(rawForm.wins)),
    matches: Math.max(0, Math.floor(rawForm.matches)),
    ...(typeof rawForm.weightedWinRate === "number" &&
    Number.isFinite(rawForm.weightedWinRate)
      ? {
          weightedWinRate: Math.max(
            0,
            Math.min(1, roundTo(rawForm.weightedWinRate, 6)),
          ),
        }
      : {}),
  };
}

function readTeamSeasonContext(
  seasonByTeam: Record<string, TeamSeasonContext> | undefined,
  teamName: string,
  season: number,
): TeamSeasonContext {
  if (seasonByTeam === undefined || !Number.isInteger(season)) {
    return {
      wins: 0,
      matchesPlayed: 0,
      weightedWinRate: 0.5,
    };
  }

  const rawSeason = seasonByTeam[createTeamSeasonKey(teamName, season)];
  if (rawSeason === undefined) {
    return {
      wins: 0,
      matchesPlayed: 0,
      weightedWinRate: 0.5,
    };
  }

  const matchesPlayed = Math.max(0, Math.floor(rawSeason.matchesPlayed));
  const wins = Math.max(0, Math.min(matchesPlayed, Math.floor(rawSeason.wins)));

  const weightedWinRate =
    typeof rawSeason.weightedWinRate === "number" &&
    Number.isFinite(rawSeason.weightedWinRate)
      ? Math.max(0, Math.min(1, roundTo(rawSeason.weightedWinRate, 6)))
      : null;

  return {
    wins,
    matchesPlayed,
    ...(weightedWinRate === null ? {} : { weightedWinRate }),
  };
}

function readTeamLineupContext(
  lineupByTeam: Record<string, TeamLineupContext>,
  teamName: string,
): TeamLineupContext {
  const rawLineup = lineupByTeam[teamName];
  if (rawLineup === undefined) {
    return {
      matches: 0,
      stability: 0.5,
      continuity: 0.5,
      rotation: 0.5,
    };
  }

  return {
    matches: Math.max(0, Math.floor(rawLineup.matches)),
    stability: clampUnitInterval(rawLineup.stability),
    continuity: clampUnitInterval(rawLineup.continuity),
    rotation: clampUnitInterval(rawLineup.rotation),
  };
}

function readTeamRoleCompositionContext(
  roleByTeam: Record<string, TeamRoleCompositionContext>,
  teamName: string,
): TeamRoleCompositionContext {
  const rawRole = roleByTeam[teamName];
  if (rawRole === undefined) {
    return {
      matches: 0,
      bowlerShare: 0.35,
      allRounderShare: 0.25,
    };
  }

  return {
    matches: Math.max(0, Math.floor(rawRole.matches)),
    bowlerShare: clampUnitInterval(rawRole.bowlerShare),
    allRounderShare: clampUnitInterval(rawRole.allRounderShare),
  };
}

function readTeamStyleCompositionContext(
  styleByTeam: Record<string, TeamStyleCompositionContext>,
  teamName: string,
): TeamStyleCompositionContext {
  const rawStyle = styleByTeam[teamName];
  if (rawStyle === undefined) {
    return {
      players: 0,
      leftHandBatShare: 0.3,
      paceBowlerShare: 0.35,
      spinBowlerShare: 0.2,
    };
  }

  return {
    players: Math.max(0, Math.floor(rawStyle.players)),
    leftHandBatShare: clampUnitInterval(rawStyle.leftHandBatShare),
    paceBowlerShare: clampUnitInterval(rawStyle.paceBowlerShare),
    spinBowlerShare: clampUnitInterval(rawStyle.spinBowlerShare),
  };
}

function readScheduleContext(
  scheduleByTeam: Record<string, TeamScheduleContext>,
  teamName: string,
): TeamScheduleContext {
  const rawSchedule = scheduleByTeam[teamName];
  if (rawSchedule === undefined) {
    return {
      daysSincePreviousMatch: null,
      matchesInPrevious7Days: 0,
    };
  }

  return {
    daysSincePreviousMatch:
      rawSchedule.daysSincePreviousMatch === null
        ? null
        : Math.max(0, roundTo(rawSchedule.daysSincePreviousMatch, 6)),
    matchesInPrevious7Days: Math.max(
      0,
      Math.floor(rawSchedule.matchesInPrevious7Days),
    ),
  };
}

function readVenueStrength(
  venueStrengthByKey: Record<string, number>,
  teamName: string,
  venueName: string,
): number {
  const lookupKey = createVenueStrengthKey(teamName, venueName);
  const rawStrength = venueStrengthByKey[lookupKey];

  if (typeof rawStrength !== "number" || !Number.isFinite(rawStrength)) {
    return 0;
  }

  return roundTo(rawStrength, 6);
}

function readHeadToHeadStrength(
  headToHeadByKey: Record<string, number>,
  teamName: string,
  opponentName: string,
): number {
  const lookupKey = createHeadToHeadKey(teamName, opponentName);
  const rawStrength = headToHeadByKey[lookupKey];

  if (typeof rawStrength !== "number" || !Number.isFinite(rawStrength)) {
    return 0;
  }

  return roundTo(rawStrength, 6);
}

export function createHeadToHeadKey(
  teamName: string,
  opponentName: string,
): string {
  return `${normalizeKeyToken(teamName)}::${normalizeKeyToken(opponentName)}`;
}

function normalizeRestDays(value: number | null): number {
  if (value === null || !Number.isFinite(value)) {
    return 5;
  }

  const bounded = Math.max(0, Math.min(value, 14));
  return roundTo(bounded, 6);
}

function computeWinRate(form: TeamRecentForm): number {
  if (
    typeof form.weightedWinRate === "number" &&
    Number.isFinite(form.weightedWinRate)
  ) {
    return roundTo(Math.max(0, Math.min(1, form.weightedWinRate)), 6);
  }

  if (form.matches <= 0) {
    return 0.5;
  }

  const wins = Math.min(form.wins, form.matches);
  return roundTo(wins / form.matches, 6);
}

function computeSeasonWinRate(season: TeamSeasonContext): number {
  if (
    typeof season.weightedWinRate === "number" &&
    Number.isFinite(season.weightedWinRate)
  ) {
    return roundTo(Math.max(0, Math.min(1, season.weightedWinRate)), 6);
  }

  if (season.matchesPlayed <= 0) {
    return 0.5;
  }

  return roundTo(season.wins / season.matchesPlayed, 6);
}

function computeSeasonWinStrength(
  winRate: number,
  matchesPlayed: number,
): number {
  const boundedMatches = Math.max(0, Math.floor(matchesPlayed));
  const reliability = boundedMatches / (boundedMatches + 6);
  return roundTo((winRate - 0.5) * reliability, 6);
}

function clampUnitInterval(value: number | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0.5;
  }

  return Math.max(0, Math.min(1, roundTo(value, 6)));
}

function normalizeSampleDiff(
  teamASamples: number,
  teamBSamples: number,
): number {
  const safeA = Math.max(0, Math.floor(teamASamples));
  const safeB = Math.max(0, Math.floor(teamBSamples));
  const denominator = safeA + safeB + 4;

  if (denominator <= 0) {
    return 0;
  }

  return (safeA - safeB) / denominator;
}

function normalizeKeyToken(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
