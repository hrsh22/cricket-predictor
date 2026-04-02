import { describe, expect, it, vi } from "vitest";

import { isRecord } from "../../src/domain/primitives.js";
import {
  fetchLiveCricketSnapshots,
  toLiveCricketSnapshot,
} from "../../src/ingest/cricket/live.js";

describe("live cricket fetcher", () => {
  it("filters IPL matches and returns CricAPI-shaped snapshots", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockImplementation(async (url) => {
      const stringUrl = String(url);
      if (stringUrl.includes("currentMatches")) {
        return buildJsonResponse({
          data: [
            {
              id: "ipl-2026-001",
              name: "Indian Premier League: Chennai Super Kings vs Mumbai Indians",
              matchType: "t20",
              status: "Match not started",
              venue: "MA Chidambaram Stadium, Chennai",
              date: "2026-03-29T14:00:00.000Z",
              teams: ["Chennai Super Kings", "Mumbai Indians"],
              teamInfo: [
                { name: "Chennai Super Kings", shortname: "CSK" },
                { name: "Mumbai Indians", shortname: "MI" },
              ],
              score: [],
              tossWinner: null,
              tossChoice: null,
              matchWinner: null,
            },
          ],
        });
      }

      return buildJsonResponse({
        data: [
          {
            id: "other-2026-001",
            name: "County Championship: Team A vs Team B",
            matchType: "test",
            status: "Match not started",
            venue: "Some Ground",
            date: "2026-03-29T14:00:00.000Z",
            teams: ["Team A", "Team B"],
            score: [],
          },
        ],
      });
    });

    const snapshots = await fetchLiveCricketSnapshots({
      config: { provider: "cricapi", apiKey: "demo-key" },
      fetchedAt: "2026-03-29T13:00:00.000Z",
      fetchImpl,
    });

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.snapshotTime).toBe("2026-03-29T13:00:00.000Z");
    expect(snapshots[0]?.payload).toMatchObject({
      id: "ipl-2026-001",
      teams: ["Chennai Super Kings", "Mumbai Indians"],
    });
    expect(fetchImpl).toHaveBeenNthCalledWith(
      1,
      "https://api.cricapi.com/v1/currentMatches?apikey=demo-key&offset=0",
      { headers: { accept: "application/json" } },
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      2,
      "https://api.cricapi.com/v1/matches?apikey=demo-key&offset=0",
      { headers: { accept: "application/json" } },
    );
  });

  it("falls back to shallow paginated matches when no IPL series id is discovered", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockImplementation(async (url) => {
      const stringUrl = String(url);

      if (stringUrl.includes("currentMatches")) {
        return buildJsonResponse({ data: [] });
      }

      if (stringUrl.endsWith("offset=25")) {
        return buildJsonResponse({
          data: [
            {
              id: "ipl-2026-late-page",
              name: "Mumbai Indians vs Rajasthan Royals, 69th Match, Indian Premier League 2026",
              matchType: "t20",
              status: "Match starts at May 24, 10:00 GMT",
              venue: "Wankhede Stadium, Mumbai",
              date: "2026-05-24",
              dateTimeGMT: "2026-05-24T10:00:00",
              teams: ["Mumbai Indians", "Rajasthan Royals"],
              score: [],
            },
          ],
        });
      }

      return buildJsonResponse({
        data: new Array(25).fill(null).map((_, index) => ({
          id: `other-${index}`,
          name: `Other competition ${index}`,
          matchType: "t20",
          status: "Match not started",
          venue: "Some Ground",
          date: "2026-03-29T14:00:00.000Z",
          teams: ["Team A", "Team B"],
          score: [],
        })),
      });
    });

    const snapshots = await fetchLiveCricketSnapshots({
      config: { provider: "cricapi", apiKey: "demo-key" },
      fetchedAt: "2026-03-29T13:00:00.000Z",
      fetchImpl,
    });

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.payload).toMatchObject({
      id: "ipl-2026-late-page",
      teams: ["Mumbai Indians", "Rajasthan Royals"],
    });
  });

  it("uses series_info when an IPL series id is discovered", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockImplementation(async (url) => {
      const stringUrl = String(url);

      if (stringUrl.includes("currentMatches")) {
        return buildJsonResponse({ data: [] });
      }

      if (stringUrl.includes("series_info")) {
        return buildJsonResponse({
          data: {
            info: {
              id: "ipl-series-2026",
              name: "Indian Premier League 2026",
            },
            matchList: [
              {
                id: "ipl-series-match-1",
                name: "Delhi Capitals vs Mumbai Indians, 8th Match, Indian Premier League 2026",
                matchType: "t20",
                status: "Match starts at Apr 04, 10:00 GMT",
                venue: "Arun Jaitley Stadium, Delhi",
                date: "2026-04-04",
                dateTimeGMT: "2026-04-04T10:00:00",
                teams: ["Delhi Capitals", "Mumbai Indians"],
                score: [],
              },
            ],
          },
        });
      }

      return buildJsonResponse({
        data: [
          {
            id: "ipl-discovery-1",
            name: "Mumbai Indians vs Kolkata Knight Riders, 2nd Match, Indian Premier League 2026",
            matchType: "t20",
            status: "Mumbai Indians won by 6 wkts",
            venue: "Wankhede Stadium, Mumbai",
            date: "2026-03-29",
            dateTimeGMT: "2026-03-29T14:00:00",
            teams: ["Mumbai Indians", "Kolkata Knight Riders"],
            score: [],
            series_id: "ipl-series-2026",
          },
        ],
      });
    });

    const snapshots = await fetchLiveCricketSnapshots({
      config: { provider: "cricapi", apiKey: "demo-key" },
      fetchedAt: "2026-03-29T13:00:00.000Z",
      fetchImpl,
    });

    expect(
      snapshots.some(
        (snapshot) =>
          isRecord(snapshot.payload) &&
          snapshot.payload["id"] === "ipl-series-match-1",
      ),
    ).toBe(true);
    expect(fetchImpl).toHaveBeenCalledWith(
      "https://api.cricapi.com/v1/series_info?apikey=demo-key&id=ipl-series-2026",
      { headers: { accept: "application/json" } },
    );
  });

  it("converts alternate live payload keys into the existing adapter shape", () => {
    const snapshot = toLiveCricketSnapshot(
      {
        id: "ipl-2026-002",
        dateTimeGMT: "2026-03-29T14:00:00.000Z",
        teams: ["Mumbai Indians", "Kolkata Knight Riders"],
        tossWinner: "Kolkata Knight Riders",
        tossDecision: "bowling",
        score: [
          {
            runs: 181,
            wickets: 7,
            overs: 20,
            inningName: "Mumbai Indians Inning 1",
          },
        ],
      },
      "2026-03-29T15:20:00.000Z",
    );

    expect(snapshot).not.toBeNull();
    expect(snapshot?.payload).toMatchObject({
      id: "ipl-2026-002",
      date: "2026-03-29T14:00:00.000Z",
      tossChoice: "bowl",
      status: "Innings break",
    });
  });

  it("fails clearly when the API key is missing", async () => {
    await expect(() =>
      fetchLiveCricketSnapshots({
        config: { provider: "cricapi", apiKey: "" },
      }),
    ).rejects.toThrow(/CRICAPI_API_KEY/);
  });

  it("surfaces provider quota failures clearly", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(
      buildJsonResponse({
        status: "failure",
        reason: "hits today exceeded hits limit",
      }),
    );

    await expect(() =>
      fetchLiveCricketSnapshots({
        config: { provider: "cricapi", apiKey: "demo-key" },
        fetchImpl,
      }),
    ).rejects.toThrow(/hits today exceeded hits limit/i);
  });
});

function buildJsonResponse(body: unknown): Response {
  return {
    ok: true,
    status: 200,
    json: async () => body,
  } as Response;
}
