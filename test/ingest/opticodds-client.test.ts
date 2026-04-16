import { describe, expect, it, vi } from "vitest";

import { createOpticOddsApiClient } from "../../src/ingest/opticodds/client.js";

describe("opticodds client", () => {
  it("serializes array query params as repeated values", async () => {
    const fetchImpl = vi.fn(async (input: string) => ({
      ok: true,
      status: 200,
      headers: {
        get: () => null,
      },
      async json() {
        return { data: [] };
      },
      async text() {
        return "";
      },
    }));

    const client = createOpticOddsApiClient({
      apiKey: "optic-test",
      baseUrl: "https://api.opticodds.test/api/v3",
      fetchImpl,
    });

    await client.getFixtureOdds({
      fixtureId: "fixture-1",
      sportsbookIds: ["polymarket", "1xbet"],
      marketIds: ["moneyline"],
      oddsFormat: "DECIMAL",
      excludeFees: true,
    });

    const requestUrl = new URL(fetchImpl.mock.calls[0]?.[0] ?? "");
    expect(requestUrl.searchParams.getAll("sportsbook")).toEqual([
      "polymarket",
      "1xbet",
    ]);
    expect(requestUrl.searchParams.get("sportsbook")).toBe("polymarket");
    expect(requestUrl.searchParams.getAll("market")).toEqual(["moneyline"]);
  });

  it("builds stream URLs with repeated sportsbook params", () => {
    const client = createOpticOddsApiClient({
      apiKey: "optic-test",
      baseUrl: "https://api.opticodds.test/api/v3",
    });

    const url = new URL(
      client.buildOddsStreamUrl({
        sportsbookIds: ["polymarket", "1xbet"],
        fixtureIds: ["fixture-1"],
        marketIds: ["moneyline"],
        oddsFormat: "DECIMAL",
        excludeFees: true,
        includeFixtureUpdates: true,
      }),
    );

    expect(url.searchParams.getAll("sportsbook")).toEqual([
      "polymarket",
      "1xbet",
    ]);
    expect(url.searchParams.getAll("fixture_id")).toEqual(["fixture-1"]);
    expect(url.searchParams.getAll("market")).toEqual(["moneyline"]);
  });
});
