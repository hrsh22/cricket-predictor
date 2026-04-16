import { describe, expect, it } from "vitest";

import { DEFAULT_DATABASE_URL } from "../../database/config.js";
import {
  defaultCheckpointToggles,
  defaultCricketLiveConfig,
  defaultLogLevel,
  defaultOpticOddsConfig,
  defaultSocialFlags,
  defaultSourceToggles,
  loadAppConfig,
} from "../../src/config/index.js";

describe("app config", () => {
  it("loads the local database URL and default feature toggles", () => {
    const config = loadAppConfig({});

    expect(config.databaseUrl).toBe(DEFAULT_DATABASE_URL);
    expect(config.logLevel).toBe(defaultLogLevel);
    expect(config.sourceToggles).toEqual(defaultSourceToggles);
    expect(config.checkpointToggles).toEqual(defaultCheckpointToggles);
    expect(config.socialFlags).toEqual(defaultSocialFlags);
    expect(config.cricketLive).toEqual(defaultCricketLiveConfig);
    expect(config.opticOdds).toEqual(defaultOpticOddsConfig);
    expect(config.opticOdds.sportsbookIds).toEqual([
      "polymarket",
      "1xbet",
      "dafabet",
      "parimatch_india_",
      "betfair_exchange",
    ]);
    expect(config.opticOdds.includeFixtureUpdates).toBe(true);
    expect(config.opticOdds.assumedTossLeadMinutesBeforeStart).toBe(30);
    expect(config.opticOdds.streamStartLeadMinutesBeforeToss).toBe(30);
    expect(config.opticOdds.streamInactivityTimeoutMs).toBe(90_000);
  });

  it("loads optional live cricket configuration", () => {
    const config = loadAppConfig({
      DATABASE_URL: DEFAULT_DATABASE_URL,
      CRICKET_LIVE_SOURCE: "cricapi",
      CRICAPI_API_KEY: "demo-key",
      CRICAPI_BASE_URL: "https://api.cricapi.com/v1",
    });

    expect(config.cricketLive.provider).toBe("cricapi");
    expect(config.cricketLive.apiKey).toBe("demo-key");
    expect(config.cricketLive.baseUrl).toBe("https://api.cricapi.com/v1");
  });

  it("loads optional log level configuration", () => {
    const config = loadAppConfig({
      DATABASE_URL: DEFAULT_DATABASE_URL,
      LOG_LEVEL: "debug",
    });

    expect(config.logLevel).toBe("debug");
  });

  it("loads optional OpticOdds configuration", () => {
    const config = loadAppConfig({
      DATABASE_URL: DEFAULT_DATABASE_URL,
      OPTIC_ODDS_API_KEY: "optic-demo",
      OPTIC_ODDS_BASE_URL: "https://api.opticodds.test/api/v3",
      OPTIC_ODDS_LEAGUE_ID: "india_-_ipl",
      OPTIC_ODDS_SEASON_YEAR: "2026",
      OPTIC_ODDS_SPORTSBOOKS: "polymarket,bet365,betfair",
      OPTIC_ODDS_MARKETS: "moneyline,spread",
      OPTIC_ODDS_EXCLUDE_FEES: "false",
      OPTIC_ODDS_INCLUDE_FIXTURE_UPDATES: "true",
      OPTIC_ODDS_ASSUMED_TOSS_LEAD_MINUTES_BEFORE_START: "25",
      OPTIC_ODDS_STREAM_START_LEAD_MINUTES_BEFORE_TOSS: "35",
      OPTIC_ODDS_FIXTURE_REFRESH_INTERVAL_SECONDS: "90",
      OPTIC_ODDS_LIVE_RESULTS_POLL_INTERVAL_SECONDS: "12",
      OPTIC_ODDS_RECONNECT_DELAY_SECONDS: "7",
      OPTIC_ODDS_STREAM_INACTIVITY_TIMEOUT_SECONDS: "75",
    });

    expect(config.opticOdds.apiKey).toBe("optic-demo");
    expect(config.opticOdds.baseUrl).toBe("https://api.opticodds.test/api/v3");
    expect(config.opticOdds.seasonYear).toBe(2026);
    expect(config.opticOdds.sportsbookIds).toEqual([
      "polymarket",
      "bet365",
      "betfair",
    ]);
    expect(config.opticOdds.marketIds).toEqual(["moneyline", "spread"]);
    expect(config.opticOdds.excludeFees).toBe(false);
    expect(config.opticOdds.includeFixtureUpdates).toBe(true);
    expect(config.opticOdds.assumedTossLeadMinutesBeforeStart).toBe(25);
    expect(config.opticOdds.streamStartLeadMinutesBeforeToss).toBe(35);
    expect(config.opticOdds.fixtureRefreshIntervalMs).toBe(90_000);
    expect(config.opticOdds.liveResultsPollIntervalMs).toBe(12_000);
    expect(config.opticOdds.reconnectDelayMs).toBe(7_000);
    expect(config.opticOdds.streamInactivityTimeoutMs).toBe(75_000);
  });

  it("fails fast on invalid boolean env values", () => {
    expect(() =>
      loadAppConfig({
        DATABASE_URL: DEFAULT_DATABASE_URL,
        ENABLE_SOCIAL_LAYER: "sometimes",
      }),
    ).toThrow(/ENABLE_SOCIAL_LAYER/);
  });

  it("fails fast on invalid database URLs", () => {
    expect(() =>
      loadAppConfig({
        DATABASE_URL: DEFAULT_DATABASE_URL.replace(
          /sports_predictor_mvp$/u,
          "",
        ),
      }),
    ).toThrow(/DATABASE_URL/);
  });
});
