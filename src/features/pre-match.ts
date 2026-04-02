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

export interface PreMatchFeatureContext {
  teamRatings: Record<string, number>;
  teamRatingDeviations: Record<string, number>;
  teamRecentForm: Record<string, TeamRecentForm>;
  teamSchedule: Record<string, TeamScheduleContext>;
  teamVenueStrength: Record<string, number>;
  teamHeadToHeadStrength: Record<string, number>;
  venueTossDecisionWinRate: Record<string, number>;
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

function normalizeKeyToken(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
