import { describe, expect, it, vi } from "vitest";

import { fetchLiveEspncricinfoSnapshots } from "../../src/ingest/cricket/live-espncricinfo.js";

describe("live espncricinfo cricket fetcher", () => {
  it("loads IPL fixture ids and converts match json into cricket snapshots", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockImplementation(async (url) => {
      const stringUrl = String(url);

      if (stringUrl.includes("view=fixtures")) {
        return buildTextResponse(`
          <html><body>
            <a href="/series/8048/game/1527676/rajasthan-royals-vs-chennai-super-kings-3rd-match-indian-premier-league-2026">Scorecard</a>
            <a href="/series/8048/scorecard/1527675/mumbai-indians-vs-kolkata-knight-riders-2nd-match-indian-premier-league-2026">Scorecard</a>
          </body></html>
        `);
      }

      if (stringUrl.includes("1527676.json")) {
        return buildJsonResponse({
          match: {
            object_id: "1527676",
            team1_name: "Rajasthan Royals",
            team2_name: "Chennai Super Kings",
            team1_id: "4345",
            team2_id: "4343",
            team1_abbreviation: "RR",
            team2_abbreviation: "CSK",
            ground_name: "Barsapara Cricket Stadium, Guwahati",
            start_datetime_gmt_raw: "2026-03-30 14:00:00",
            winner_team_id: null,
            toss_winner_team_id: null,
          },
          live: {
            status: "Match scheduled to begin at 19:30 local time (14:00 GMT)",
          },
          innings: [],
          series: [{ object_id: 1510719 }],
        });
      }

      return buildJsonResponse({
        match: {
          object_id: "1527675",
          team1_name: "Mumbai Indians",
          team2_name: "Kolkata Knight Riders",
          team1_id: "4346",
          team2_id: "4341",
          team1_abbreviation: "MI",
          team2_abbreviation: "KKR",
          ground_name: "Wankhede Stadium, Mumbai",
          start_datetime_gmt_raw: "2026-03-29 14:00:00",
          winner_team_id: "4346",
          toss_winner_team_id: null,
        },
        live: {
          status: "Mumbai Indians won by 6 wickets (with 5 balls remaining)",
        },
        innings: [
          {
            batting_team_id: "4341",
            innings_number: 1,
            runs: 220,
            wickets: 4,
            overs: 20,
          },
        ],
        series: [{ object_id: 1510719 }],
      });
    });

    const snapshots = await fetchLiveEspncricinfoSnapshots({
      fetchedAt: "2026-03-30T11:00:00.000Z",
      fetchImpl,
    });

    expect(snapshots).toHaveLength(2);
    expect(snapshots[0]?.payload).toMatchObject({
      id: "1527675",
      teams: ["Mumbai Indians", "Kolkata Knight Riders"],
    });
    expect(snapshots[1]?.payload).toMatchObject({
      id: "1527676",
      teams: ["Rajasthan Royals", "Chennai Super Kings"],
    });
  });

  it("derives toss choice from live status when toss decision field is missing", async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockImplementation(async (url) => {
      const stringUrl = String(url);

      if (stringUrl.includes("view=fixtures")) {
        return buildTextResponse(`
          <html><body>
            <a href="/series/8048/game/1527676/rajasthan-royals-vs-chennai-super-kings-3rd-match-indian-premier-league-2026">Scorecard</a>
          </body></html>
        `);
      }

      return buildJsonResponse({
        match: {
          object_id: "1527676",
          team1_name: "Rajasthan Royals",
          team2_name: "Chennai Super Kings",
          team1_id: "4345",
          team2_id: "4343",
          ground_name: "Barsapara Cricket Stadium, Guwahati",
          start_datetime_gmt_raw: "2026-03-30 14:00:00",
          winner_team_id: null,
          toss_winner_team_id: "4345",
          toss_decision: null,
        },
        live: {
          status: "Rajasthan Royals won the toss and elected to field",
        },
        innings: [],
        series: [{ object_id: 1510719 }],
      });
    });

    const snapshots = await fetchLiveEspncricinfoSnapshots({
      fetchedAt: "2026-03-30T11:00:00.000Z",
      fetchImpl,
      nextMatchOnly: true,
    });

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.payload).toMatchObject({
      tossWinner: "Rajasthan Royals",
      tossChoice: "bowl",
    });
  });
});

function buildTextResponse(body: string): Response {
  return {
    ok: true,
    status: 200,
    text: async () => body,
  } as Response;
}

function buildJsonResponse(body: unknown): Response {
  return {
    ok: true,
    status: 200,
    text: async () => JSON.stringify(body),
  } as Response;
}
