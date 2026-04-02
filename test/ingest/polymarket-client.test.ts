import { describe, expect, it } from "vitest";

import {
  DomainValidationError,
  normalizePolymarketIplWinnerMarket,
  normalizePolymarketIplWinnerMarkets,
  toPolymarketIplWinnerDiscovery,
} from "../../src/index.js";
import {
  duplicateIplWinnerMarketDiscoveries,
  liveIplMoneylineMarket,
  malformedIplWinnerMarketDiscovery,
  missingFieldsIplWinnerMarketDiscovery,
  staleIplWinnerMarketDiscovery,
  unsupportedIplWinnerMarketDiscovery,
  validIplWinnerMarketDiscovery,
} from "../fixtures/polymarket/markets.js";

describe("polymarket client contracts", () => {
  it("normalizes a valid IPL winner market into the shared market snapshot shape", () => {
    const market = normalizePolymarketIplWinnerMarket(
      validIplWinnerMarketDiscovery,
    );

    expect(market.competition).toBe("IPL");
    expect(market.sourceMarketId).toBe("pm-ipl-001");
    expect(market.eventSlug).toBe(
      "ipl-2026-chennai-super-kings-vs-mumbai-indians",
    );
    expect(market.yesOutcomeName).toBe("Chennai Super Kings");
    expect(market.outcomeProbabilities["yes"]).toBe(0.54);
    expect(market.outcomeProbabilities["no"]).toBe(0.46);
    expect(market.liquidity).toBe(42000);

    const tokenIds = market.payload["tokenIds"] as Record<string, unknown>;
    expect(tokenIds["yes"]).toBe("pm-token-yes");
    expect(tokenIds["no"]).toBe("pm-token-no");
  });

  it("rejects duplicate markets in a discovery batch", () => {
    expect(() =>
      normalizePolymarketIplWinnerMarkets(duplicateIplWinnerMarketDiscoveries),
    ).toThrow(DomainValidationError);
  });

  it("rejects stale and unsupported markets", () => {
    expect(() =>
      normalizePolymarketIplWinnerMarket(staleIplWinnerMarketDiscovery),
    ).toThrow(DomainValidationError);
    expect(() =>
      normalizePolymarketIplWinnerMarket(unsupportedIplWinnerMarketDiscovery),
    ).toThrow(DomainValidationError);
  });

  it("rejects missing fields and malformed payloads", () => {
    expect(() =>
      normalizePolymarketIplWinnerMarket(missingFieldsIplWinnerMarketDiscovery),
    ).toThrow(DomainValidationError);
    expect(() =>
      normalizePolymarketIplWinnerMarket(malformedIplWinnerMarketDiscovery),
    ).toThrow(DomainValidationError);
  });

  it("normalizes live IPL moneyline markets adapted from Gamma market rows", () => {
    const discovery = toPolymarketIplWinnerDiscovery(
      liveIplMoneylineMarket,
      "2026-03-29T14:40:00.000Z",
    );

    expect(discovery).not.toBeNull();

    const market = normalizePolymarketIplWinnerMarket(discovery);
    expect(market.marketSlug).toBe("cricipl-mum-kol-2026-03-29");
    expect(market.snapshotTime).toBe("2026-03-29T14:39:20.030Z");
    expect(market.outcomeProbabilities["yes"]).toBe(0.425);
    expect(market.outcomeProbabilities["no"]).toBe(0.575);
  });
});
