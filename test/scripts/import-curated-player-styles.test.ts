import { describe, expect, it } from "vitest";

import { parseCsv } from "../../scripts/db/import-curated-player-styles.js";

describe("import-curated-player-styles helpers", () => {
  it("skips shell banner lines before the CSV header", () => {
    const csv = `
> db:export-player-style-candidates
> tsx scripts/db/export-player-style-candidates.ts --season-from 2024 --season-to 2025 --output csv
player_registry_id,source,batting_hand,bowling_arm,bowling_style,bowling_type_group,player_role,confidence,notes
123,curated_manual,right,right,right-arm medium,pace,bowler,1.0,
`;

    expect(parseCsv(csv)).toEqual([
      {
        player_registry_id: "123",
        source: "curated_manual",
        batting_hand: "right",
        bowling_arm: "right",
        bowling_style: "right-arm medium",
        bowling_type_group: "pace",
        player_role: "bowler",
        confidence: "1.0",
        notes: "",
      },
    ]);
  });
});
