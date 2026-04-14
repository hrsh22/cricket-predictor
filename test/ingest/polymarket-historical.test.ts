import { describe, expect, it } from "vitest";

import {
  backfillPolymarketEventHistoricalOdds,
  createTradeKey,
  type PolymarketFetch,
  type PolymarketResponseLike,
} from "../../src/ingest/polymarket/index.js";
import type {
  RawPolymarketPriceHistoryInsert,
  RawPolymarketPriceHistoryRecord,
  RawPolymarketTradeInsert,
  RawPolymarketTradeRecord,
  RawSnapshotRepository,
} from "../../src/repositories/index.js";

describe("polymarket historical backfill", () => {
  it("fetches token chart history and exact trades for a reusable event backfill", async () => {
    const rawRepository = createMemoryHistoricalRepository();
    const fetchImpl: PolymarketFetch = async (input) => {
      const url = new URL(input);

      if (
        url.hostname === "gamma-api.polymarket.com" &&
        url.pathname === "/events"
      ) {
        return createResponse([
          {
            id: "336477",
            slug: "cricipl-kol-luc-2026-04-09",
            startDate: "2026-04-02T16:32:52.691675Z",
            markets: [
              {
                id: "1831591",
                slug: "cricipl-kol-luc-2026-04-09",
                conditionId: "0xwinner",
                sportsMarketType: "moneyline",
                outcomes: '["Kolkata Knight Riders", "Lucknow Super Giants"]',
                clobTokenIds:
                  '["20994999450418646625607343049053915187661854374961490789349968414006072201453", "114069619728365717365213547758236037233539614349096135240391410683869625477794"]',
              },
              {
                id: "1831592",
                slug: "cricipl-kol-luc-2026-04-09-toss-winner",
                conditionId: "0xtoss",
                sportsMarketType: "cricket_toss_winner",
                outcomes: '["Kolkata Knight Riders", "Lucknow Super Giants"]',
                clobTokenIds:
                  '["113674658533802497893272753833212738222251556779213135207110098688525066788715", "78083421391748853260137110189092019271346644348112760145715039381121477137805"]',
              },
            ],
          },
        ]);
      }

      if (
        url.hostname === "clob.polymarket.com" &&
        url.pathname === "/prices-history"
      ) {
        const tokenId = url.searchParams.get("market");
        if (tokenId === null) {
          throw new Error("Missing token market param in test");
        }

        return createResponse({
          history: [
            { t: 1775149226, p: tokenId.startsWith("2099") ? 0.505 : 0.495 },
            { t: 1775156455, p: tokenId.startsWith("2099") ? 0.5 : 0.5 },
          ],
        });
      }

      if (
        url.hostname === "data-api.polymarket.com" &&
        url.pathname === "/trades"
      ) {
        const market = url.searchParams.get("market");
        const side = url.searchParams.get("side");
        const offset = Number(url.searchParams.get("offset") ?? "0");
        if (offset > 0) {
          return createResponse([]);
        }

        if (market === "0xwinner" && side === "SELL") {
          return createResponse([
            {
              proxyWallet: "0xwallet-1",
              side: "SELL",
              asset:
                "114069619728365717365213547758236037233539614349096135240391410683869625477794",
              conditionId: "0xwinner",
              size: 7.58,
              price: 0.999,
              timestamp: 1775764355,
              outcome: "Lucknow Super Giants",
              outcomeIndex: 1,
              transactionHash: "0xtrade-1",
            },
          ]);
        }

        if (market === "0xtoss" && side === "BUY") {
          return createResponse([
            {
              proxyWallet: "0xwallet-2",
              side: "BUY",
              asset:
                "113674658533802497893272753833212738222251556779213135207110098688525066788715",
              conditionId: "0xtoss",
              size: 5,
              price: 0.41,
              timestamp: 1775744600,
              outcome: "Kolkata Knight Riders",
              outcomeIndex: 0,
              transactionHash: "0xtrade-2",
            },
          ]);
        }

        return createResponse([]);
      }

      throw new Error(`Unexpected fetch URL in test: ${input}`);
    };

    const summary = await backfillPolymarketEventHistoricalOdds({
      eventSlug: "cricipl-kol-luc-2026-04-09",
      rawRepository,
      fetchImpl,
      startTs: 1775146757,
      endTs: 1775764423,
      fidelityMinutes: 60,
      tradePageSize: 2,
    });

    expect(summary.selectedMarketCount).toBe(2);
    expect(summary.selectedTokenCount).toBe(4);
    expect(summary.fetchedPricePointCount).toBe(8);
    expect(summary.persistedPricePointCount).toBe(8);
    expect(summary.fetchedTradeCount).toBe(2);
    expect(summary.persistedTradeCount).toBe(2);
    expect(rawRepository.priceHistoryRecords).toHaveLength(8);
    expect(rawRepository.tradeRecords).toHaveLength(2);
    expect(rawRepository.tradeRecords[0]?.tradeKey).toBe(
      createTradeKey({
        asset:
          "114069619728365717365213547758236037233539614349096135240391410683869625477794",
        transactionHash: "0xtrade-1",
        timestamp: 1775764355,
        side: "SELL",
        price: 0.999,
        size: 7.58,
        proxyWallet: "0xwallet-1",
      }),
    );
  });
});

function createResponse(body: unknown): PolymarketResponseLike {
  return {
    ok: true,
    status: 200,
    headers: {
      get(): string | null {
        return null;
      },
    },
    async json(): Promise<unknown> {
      return body;
    },
    async text(): Promise<string> {
      return JSON.stringify(body);
    },
  };
}

function createMemoryHistoricalRepository(): Pick<
  RawSnapshotRepository,
  "savePolymarketPriceHistoryPoint" | "savePolymarketTrade"
> & {
  priceHistoryRecords: RawPolymarketPriceHistoryRecord[];
  tradeRecords: RawPolymarketTradeRecord[];
} {
  let nextPriceId = 1;
  let nextTradeId = 1;
  const priceHistoryStorage = new Map<
    string,
    RawPolymarketPriceHistoryRecord
  >();
  const tradeStorage = new Map<string, RawPolymarketTradeRecord>();

  return {
    priceHistoryRecords: [],
    tradeRecords: [],
    async savePolymarketPriceHistoryPoint(
      point: RawPolymarketPriceHistoryInsert,
    ): Promise<RawPolymarketPriceHistoryRecord> {
      const key = `${point.tokenId}:${point.pointTime}`;
      const existing = priceHistoryStorage.get(key);
      const record: RawPolymarketPriceHistoryRecord = {
        id: existing?.id ?? nextPriceId,
        competition: "IPL",
        createdAt: existing?.createdAt ?? point.pointTime,
        ...point,
      };

      if (existing === undefined) {
        nextPriceId += 1;
      }

      priceHistoryStorage.set(key, record);
      this.priceHistoryRecords = Array.from(priceHistoryStorage.values());
      return record;
    },
    async savePolymarketTrade(
      trade: RawPolymarketTradeInsert,
    ): Promise<RawPolymarketTradeRecord> {
      const existing = tradeStorage.get(trade.tradeKey);
      const record: RawPolymarketTradeRecord = {
        id: existing?.id ?? nextTradeId,
        competition: "IPL",
        createdAt: existing?.createdAt ?? trade.tradeTime,
        ...trade,
      };

      if (existing === undefined) {
        nextTradeId += 1;
      }

      tradeStorage.set(trade.tradeKey, record);
      this.tradeRecords = Array.from(tradeStorage.values());
      return record;
    },
  };
}
