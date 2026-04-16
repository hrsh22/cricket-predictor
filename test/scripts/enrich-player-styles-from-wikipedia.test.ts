import { describe, expect, it } from "vitest";

import { parseWikipediaInfoboxFields } from "../../scripts/db/enrich-player-styles-from-wikipedia.js";

describe("enrich-player-styles-from-wikipedia helpers", () => {
  it("extracts batting, bowling, and role fields from infobox wikitext", () => {
    const wikitext = `
{{Infobox cricketer
| name = Example Player
| batting = Left-handed
| bowling = Right-arm off break
| role = Bowling all-rounder
}}
`;

    expect(parseWikipediaInfoboxFields(wikitext)).toEqual({
      battingHand: "left",
      bowlingArm: "right",
      bowlingStyle: "Right-arm off break",
      bowlingTypeGroup: "spin",
      playerRole: "all_rounder",
    });
  });
});
