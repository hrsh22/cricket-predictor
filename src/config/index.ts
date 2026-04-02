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

export interface AppConfig {
  databaseUrl: string;
  databaseName: string;
  sourceToggles: SourceToggles;
  checkpointToggles: CheckpointToggles;
  socialFlags: SocialFlags;
  cricketLive: CricketLiveConfig;
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
