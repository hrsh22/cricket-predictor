const DEW_RISK_BY_CITY: Record<string, number> = {
  kolkata: 0.22,
  lucknow: 0.22,
  hyderabad: 0.19,
  delhi: 0.18,
  mumbai: 0.11,
  ahmedabad: 0.1,
  bengaluru: 0.05,
  bangalore: 0.05,
  chennai: 0.02,
  jaipur: 0.12,
  mohali: 0.15,
  chandigarh: 0.15,
  pune: 0.08,
  dharamsala: 0.06,
  guwahati: 0.14,
  visakhapatnam: 0.1,
  rajkot: 0.08,
  indore: 0.1,
  cuttack: 0.12,
  ranchi: 0.1,
  nagpur: 0.08,
  kanpur: 0.14,
};

const MONTH_DEW_MULTIPLIER: Record<number, number> = {
  3: 0.5,
  4: 0.75,
  5: 1.0,
  6: 0.6,
};

const HOME_ADVANTAGE_BY_TEAM: Record<string, number> = {
  "chennai super kings": 0.133,
  "gujarat titans": 0.095,
  "lucknow super giants": 0.097,
  "kolkata knight riders": 0.089,
  "sunrisers hyderabad": 0.089,
  "mumbai indians": 0.074,
  "rajasthan royals": 0.064,
  "delhi capitals": 0.036,
  "punjab kings": 0.035,
  "royal challengers bengaluru": 0.029,
  "royal challengers bangalore": 0.029,
};

const TEAM_HOME_VENUES: Record<string, string[]> = {
  "chennai super kings": ["chennai", "chepauk", "chidambaram"],
  "mumbai indians": ["mumbai", "wankhede"],
  "royal challengers bengaluru": ["bengaluru", "bangalore", "chinnaswamy"],
  "royal challengers bangalore": ["bengaluru", "bangalore", "chinnaswamy"],
  "kolkata knight riders": ["kolkata", "eden"],
  "delhi capitals": ["delhi", "kotla", "feroz"],
  "rajasthan royals": ["jaipur", "sawai"],
  "punjab kings": ["mohali", "chandigarh", "dharamsala", "mullanpur"],
  "sunrisers hyderabad": ["hyderabad", "rajiv gandhi", "uppal"],
  "gujarat titans": ["ahmedabad", "motera", "narendra modi"],
  "lucknow super giants": ["lucknow", "ekana", "bharat ratna"],
};

export interface VenueConditionsFeatures {
  dewFactor: number;
  homeAdvantageTeamA: number;
  homeAdvantageTeamB: number;
  homeAdvantageDiff: number;
  isEveningMatch: boolean;
  matchMonth: number;
}

export function computeVenueConditionsFeatures(
  venueName: string | null,
  teamAName: string,
  teamBName: string,
  scheduledStart: Date,
): VenueConditionsFeatures {
  const hour = scheduledStart.getUTCHours();
  const adjustedHour = (hour + 5.5) % 24;
  const isEveningMatch = adjustedHour >= 19 || adjustedHour < 2;

  const month = scheduledStart.getUTCMonth() + 1;

  const dewFactor = computeDewFactor(venueName, month, isEveningMatch);

  const homeAdvantageTeamA = computeHomeAdvantage(teamAName, venueName);
  const homeAdvantageTeamB = computeHomeAdvantage(teamBName, venueName);
  const homeAdvantageDiff = roundTo(homeAdvantageTeamA - homeAdvantageTeamB, 6);

  return {
    dewFactor,
    homeAdvantageTeamA,
    homeAdvantageTeamB,
    homeAdvantageDiff,
    isEveningMatch,
    matchMonth: month,
  };
}

function computeDewFactor(
  venueName: string | null,
  month: number,
  isEveningMatch: boolean,
): number {
  if (!isEveningMatch) {
    return 0;
  }

  const city = extractCityFromVenue(venueName);
  const baseDewRisk = DEW_RISK_BY_CITY[city] ?? 0.1;
  const monthMultiplier = MONTH_DEW_MULTIPLIER[month] ?? 0.75;

  return roundTo(baseDewRisk * monthMultiplier, 6);
}

function computeHomeAdvantage(
  teamName: string,
  venueName: string | null,
): number {
  const normalizedTeam = teamName.toLowerCase().trim();
  const homeAdvantage = HOME_ADVANTAGE_BY_TEAM[normalizedTeam] ?? 0.05;

  const homeVenues = TEAM_HOME_VENUES[normalizedTeam];
  if (!homeVenues || !venueName) {
    return 0;
  }

  const normalizedVenue = venueName.toLowerCase();
  const isHomeVenue = homeVenues.some((v) => normalizedVenue.includes(v));

  return isHomeVenue ? homeAdvantage : 0;
}

function extractCityFromVenue(venueName: string | null): string {
  if (!venueName) {
    return "unknown";
  }

  const normalized = venueName.toLowerCase();

  for (const city of Object.keys(DEW_RISK_BY_CITY)) {
    if (normalized.includes(city)) {
      return city;
    }
  }

  if (normalized.includes("eden")) return "kolkata";
  if (normalized.includes("wankhede")) return "mumbai";
  if (normalized.includes("chinnaswamy")) return "bengaluru";
  if (normalized.includes("chepauk") || normalized.includes("chidambaram"))
    return "chennai";
  if (normalized.includes("kotla") || normalized.includes("feroz"))
    return "delhi";
  if (normalized.includes("sawai") || normalized.includes("mansingh"))
    return "jaipur";
  if (normalized.includes("ekana") || normalized.includes("bharat ratna"))
    return "lucknow";
  if (normalized.includes("rajiv gandhi") || normalized.includes("uppal"))
    return "hyderabad";
  if (normalized.includes("motera") || normalized.includes("narendra modi"))
    return "ahmedabad";
  if (normalized.includes("mullanpur") || normalized.includes("is bindra"))
    return "mohali";

  return "unknown";
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
