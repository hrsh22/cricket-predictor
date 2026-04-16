import {
  DEFAULT_DATABASE_URL,
  getDatabaseName,
  getDatabaseUrl,
} from "../../database/config.js";

export interface SourceToggles {
  marketSnapshots: boolean;
  cricketSnapshots: boolean;
}

export interface CheckpointToggles {
  preMatch: boolean;
  postToss: boolean;
  inningsBreak: boolean;
}

export interface SocialFlags {
  enabled: boolean;
}

export interface CricketLiveConfig {
  provider: "espncricinfo" | "cricapi";
  apiKey: string | null;
  baseUrl: string;
}

export interface OpticOddsConfig {
  apiKey: string | null;
  baseUrl: string;
  sport: "cricket";
  leagueId: string;
  seasonYear: number;
  sportsbookIds: string[];
  marketIds: string[];
  oddsFormat: "DECIMAL";
  excludeFees: boolean;
  includeFixtureUpdates: boolean;
  assumedTossLeadMinutesBeforeStart: number;
  streamStartLeadMinutesBeforeToss: number;
  fixtureRefreshIntervalMs: number;
  liveResultsPollIntervalMs: number;
  reconnectDelayMs: number;
  streamInactivityTimeoutMs: number;
}

export interface AppConfig {
  databaseUrl: string;
  databaseName: string;
  sourceToggles: SourceToggles;
  checkpointToggles: CheckpointToggles;
  socialFlags: SocialFlags;
  cricketLive: CricketLiveConfig;
  opticOdds: OpticOddsConfig;
}

export const defaultSourceToggles: SourceToggles = {
  marketSnapshots: true,
  cricketSnapshots: true,
};

export const defaultCheckpointToggles: CheckpointToggles = {
  preMatch: true,
  postToss: true,
  inningsBreak: true,
};

export const defaultSocialFlags: SocialFlags = {
  enabled: true,
};

export const defaultCricketLiveConfig: CricketLiveConfig = {
  provider: "espncricinfo",
  apiKey: null,
  baseUrl: "https://www.espncricinfo.com",
};

export const defaultOpticOddsConfig: OpticOddsConfig = {
  apiKey: null,
  baseUrl: "https://api.opticodds.com/api/v3",
  sport: "cricket",
  leagueId: "india_-_ipl",
  seasonYear: new Date().getUTCFullYear(),
  sportsbookIds: [
    "polymarket",
    "1xbet",
    "dafabet",
    "parimatch_india_",
    "betfair_exchange",
  ],
  marketIds: ["moneyline"],
  oddsFormat: "DECIMAL",
  excludeFees: true,
  includeFixtureUpdates: true,
  assumedTossLeadMinutesBeforeStart: 30,
  streamStartLeadMinutesBeforeToss: 30,
  fixtureRefreshIntervalMs: 60_000,
  liveResultsPollIntervalMs: 15_000,
  reconnectDelayMs: 5_000,
  streamInactivityTimeoutMs: 90_000,
};

export function loadAppConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const databaseUrl = getDatabaseUrl(env);

  return {
    databaseUrl,
    databaseName: getDatabaseName(databaseUrl),
    sourceToggles: {
      marketSnapshots: parseBooleanEnv(
        env["ENABLE_MARKET_SNAPSHOTS"],
        "ENABLE_MARKET_SNAPSHOTS",
        defaultSourceToggles.marketSnapshots,
      ),
      cricketSnapshots: parseBooleanEnv(
        env["ENABLE_CRICKET_SNAPSHOTS"],
        "ENABLE_CRICKET_SNAPSHOTS",
        defaultSourceToggles.cricketSnapshots,
      ),
    },
    checkpointToggles: {
      preMatch: parseBooleanEnv(
        env["ENABLE_PRE_MATCH_CHECKPOINT"],
        "ENABLE_PRE_MATCH_CHECKPOINT",
        defaultCheckpointToggles.preMatch,
      ),
      postToss: parseBooleanEnv(
        env["ENABLE_POST_TOSS_CHECKPOINT"],
        "ENABLE_POST_TOSS_CHECKPOINT",
        defaultCheckpointToggles.postToss,
      ),
      inningsBreak: parseBooleanEnv(
        env["ENABLE_INNINGS_BREAK_CHECKPOINT"],
        "ENABLE_INNINGS_BREAK_CHECKPOINT",
        defaultCheckpointToggles.inningsBreak,
      ),
    },
    socialFlags: {
      enabled: parseBooleanEnv(
        env["ENABLE_SOCIAL_LAYER"],
        "ENABLE_SOCIAL_LAYER",
        defaultSocialFlags.enabled,
      ),
    },
    cricketLive: {
      provider: parseCricketLiveSourceEnv(env["CRICKET_LIVE_SOURCE"]),
      apiKey: parseOptionalStringEnv(env["CRICAPI_API_KEY"]),
      baseUrl:
        parseOptionalStringEnv(
          env["CRICKET_LIVE_BASE_URL"] ?? env["CRICAPI_BASE_URL"],
        ) ?? defaultCricketLiveConfig.baseUrl,
    },
    opticOdds: {
      apiKey: parseOptionalStringEnv(env["OPTIC_ODDS_API_KEY"]),
      baseUrl:
        parseOptionalStringEnv(env["OPTIC_ODDS_BASE_URL"]) ??
        defaultOpticOddsConfig.baseUrl,
      sport: "cricket",
      leagueId:
        parseOptionalStringEnv(env["OPTIC_ODDS_LEAGUE_ID"]) ??
        defaultOpticOddsConfig.leagueId,
      seasonYear: parsePositiveIntegerEnv(
        env["OPTIC_ODDS_SEASON_YEAR"],
        "OPTIC_ODDS_SEASON_YEAR",
        defaultOpticOddsConfig.seasonYear,
      ),
      sportsbookIds: parseCsvEnv(
        env["OPTIC_ODDS_SPORTSBOOKS"],
        defaultOpticOddsConfig.sportsbookIds,
      ),
      marketIds: parseCsvEnv(
        env["OPTIC_ODDS_MARKETS"],
        defaultOpticOddsConfig.marketIds,
      ),
      oddsFormat: "DECIMAL",
      excludeFees: parseBooleanEnv(
        env["OPTIC_ODDS_EXCLUDE_FEES"],
        "OPTIC_ODDS_EXCLUDE_FEES",
        defaultOpticOddsConfig.excludeFees,
      ),
      includeFixtureUpdates: parseBooleanEnv(
        env["OPTIC_ODDS_INCLUDE_FIXTURE_UPDATES"],
        "OPTIC_ODDS_INCLUDE_FIXTURE_UPDATES",
        defaultOpticOddsConfig.includeFixtureUpdates,
      ),
      assumedTossLeadMinutesBeforeStart: parsePositiveIntegerEnv(
        env["OPTIC_ODDS_ASSUMED_TOSS_LEAD_MINUTES_BEFORE_START"],
        "OPTIC_ODDS_ASSUMED_TOSS_LEAD_MINUTES_BEFORE_START",
        defaultOpticOddsConfig.assumedTossLeadMinutesBeforeStart,
      ),
      streamStartLeadMinutesBeforeToss: parsePositiveIntegerEnv(
        env["OPTIC_ODDS_STREAM_START_LEAD_MINUTES_BEFORE_TOSS"],
        "OPTIC_ODDS_STREAM_START_LEAD_MINUTES_BEFORE_TOSS",
        defaultOpticOddsConfig.streamStartLeadMinutesBeforeToss,
      ),
      fixtureRefreshIntervalMs:
        parsePositiveIntegerEnv(
          env["OPTIC_ODDS_FIXTURE_REFRESH_INTERVAL_SECONDS"],
          "OPTIC_ODDS_FIXTURE_REFRESH_INTERVAL_SECONDS",
          defaultOpticOddsConfig.fixtureRefreshIntervalMs / 1000,
        ) * 1000,
      liveResultsPollIntervalMs:
        parsePositiveIntegerEnv(
          env["OPTIC_ODDS_LIVE_RESULTS_POLL_INTERVAL_SECONDS"],
          "OPTIC_ODDS_LIVE_RESULTS_POLL_INTERVAL_SECONDS",
          defaultOpticOddsConfig.liveResultsPollIntervalMs / 1000,
        ) * 1000,
      reconnectDelayMs:
        parsePositiveIntegerEnv(
          env["OPTIC_ODDS_RECONNECT_DELAY_SECONDS"],
          "OPTIC_ODDS_RECONNECT_DELAY_SECONDS",
          defaultOpticOddsConfig.reconnectDelayMs / 1000,
        ) * 1000,
      streamInactivityTimeoutMs:
        parsePositiveIntegerEnv(
          env["OPTIC_ODDS_STREAM_INACTIVITY_TIMEOUT_SECONDS"],
          "OPTIC_ODDS_STREAM_INACTIVITY_TIMEOUT_SECONDS",
          defaultOpticOddsConfig.streamInactivityTimeoutMs / 1000,
        ) * 1000,
    },
  };
}

export function parseBooleanEnv(
  value: string | undefined,
  envName: string,
  defaultValue: boolean,
): boolean {
  if (value === undefined || value.trim().length === 0) {
    return defaultValue;
  }

  const normalized = value.trim().toLowerCase();

  if (
    normalized === "true" ||
    normalized === "1" ||
    normalized === "yes" ||
    normalized === "on"
  ) {
    return true;
  }

  if (
    normalized === "false" ||
    normalized === "0" ||
    normalized === "no" ||
    normalized === "off"
  ) {
    return false;
  }

  throw new Error(
    `Invalid ${envName} value "${value}". Expected one of true/false, 1/0, yes/no, or on/off.`,
  );
}

export function parseOptionalStringEnv(
  value: string | undefined,
): string | null {
  if (value === undefined) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

export function parseCsvEnv(
  value: string | undefined,
  defaultValue: readonly string[],
): string[] {
  const normalized = parseOptionalStringEnv(value);
  if (normalized === null) {
    return [...defaultValue];
  }

  return normalized
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

export function parsePositiveIntegerEnv(
  value: string | undefined,
  envName: string,
  defaultValue: number,
): number {
  const normalized = parseOptionalStringEnv(value);
  if (normalized === null) {
    return defaultValue;
  }

  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(
      `Invalid ${envName} value "${value}". Expected a positive integer.`,
    );
  }

  return parsed;
}

export function parseCricketLiveSourceEnv(
  value: string | undefined,
): CricketLiveConfig["provider"] {
  if (value === undefined || value.trim().length === 0) {
    return defaultCricketLiveConfig.provider;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === "espncricinfo" || normalized === "cricapi") {
    return normalized;
  }

  throw new Error(
    `Invalid CRICKET_LIVE_SOURCE value "${value}". Expected espncricinfo or cricapi.`,
  );
}

export function isCheckpointEnabled(
  config: Pick<AppConfig, "checkpointToggles">,
  checkpointType: "pre_match" | "post_toss" | "innings_break",
): boolean {
  if (checkpointType === "pre_match") {
    return config.checkpointToggles.preMatch;
  }

  if (checkpointType === "post_toss") {
    return config.checkpointToggles.postToss;
  }

  return config.checkpointToggles.inningsBreak;
}

export { DEFAULT_DATABASE_URL };
