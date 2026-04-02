import { describe, expect, it } from "vitest";

import { parseIplSpecialization } from "../../scripts/db/enrich-player-registry.js";

describe("enrich-player-registry helpers", () => {
  it("extracts specialization from IPL player overview markup", () => {
    const html = `
      <div class="grid-items">
        <p>2008</p>
        <span>IPL Debut</span>
      </div>
      <div class="grid-items">
        <p>Wicketkeeper Batter</p>
        <span>Specialization</span>
      </div>
    `;

    expect(parseIplSpecialization(html)).toBe("Wicketkeeper Batter");
  });
});
