import { describe, expect, it } from "vitest";

import { DEFAULT_DATABASE_URL } from "../../database/config.js";
import {
  defaultCheckpointToggles,
  defaultCricketLiveConfig,
  defaultSocialFlags,
  defaultSourceToggles,
  loadAppConfig,
} from "../../src/config/index.js";

describe("app config", () => {
  it("loads the local database URL and default feature toggles", () => {
    const config = loadAppConfig({});

    expect(config.databaseUrl).toBe(DEFAULT_DATABASE_URL);
    expect(config.sourceToggles).toEqual(defaultSourceToggles);
    expect(config.checkpointToggles).toEqual(defaultCheckpointToggles);
    expect(config.socialFlags).toEqual(defaultSocialFlags);
    expect(config.cricketLive).toEqual(defaultCricketLiveConfig);
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
        DATABASE_URL: "postgresql://localhost:5432/",
      }),
    ).toThrow(/DATABASE_URL/);
  });
});
