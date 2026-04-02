import { describe, expect, it } from "vitest";

import {
  normalizeCricsheetMatch,
  parseSeason,
} from "../../scripts/db/import-cricsheet-ipl.js";

describe("import-cricsheet-ipl helpers", () => {
  it("parses numeric season values directly", () => {
    expect(parseSeason(2012, "2012-05-17T14:00:00.000Z")).toBe(2012);
  });

  it("uses match date year for split-season strings", () => {
    expect(parseSeason("2007/08", "2008-05-17T14:00:00.000Z")).toBe(2008);
    expect(parseSeason("2009/10", "2010-04-03T14:00:00.000Z")).toBe(2010);
    expect(parseSeason("2020/21", "2020-11-01T14:00:00.000Z")).toBe(2020);
  });

  it("falls back to scheduled start year when season is missing", () => {
    expect(parseSeason(undefined, "2014-04-23T14:00:00.000Z")).toBe(2014);
  });

  it("normalizes a numeric-season Cricsheet match into a canonical IPL row", () => {
    expect(
      normalizeCricsheetMatch(
        {
          info: {
            gender: "male",
            match_type: "T20",
            season: 2012,
            dates: ["2012-05-17"],
            teams: ["Delhi Daredevils", "Royal Challengers Bangalore"],
            city: "Delhi",
            venue: "Feroz Shah Kotla",
            toss: {
              winner: "Delhi Daredevils",
              decision: "field",
            },
            outcome: {
              winner: "Royal Challengers Bangalore",
            },
          },
        },
        "548372",
      ),
    ).toEqual({
      season: 2012,
      sourceMatchId: "548372",
      scheduledStart: "2012-05-17T14:00:00.000Z",
      teamAName: "Delhi Capitals",
      teamBName: "Royal Challengers Bengaluru",
      venueName: "Feroz Shah Kotla, Delhi",
      tossWinnerTeamName: "Delhi Capitals",
      tossDecision: "bowl",
      winningTeamName: "Royal Challengers Bengaluru",
      resultType: "win",
      status: "completed",
      lineups: [],
    });
  });
});
