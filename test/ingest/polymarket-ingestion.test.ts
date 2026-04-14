import { describe, expect, it, vi } from "vitest";

import {
  createPolymarketGammaReadClient,
  ingestPolymarketIplWinnerMarkets,
  type PolymarketResponseLike,
} from "../../src/ingest/polymarket/index.js";
import type {
  RawCricketSnapshotInsert,
  RawCricketSnapshotRecord,
  RawMarketSnapshotRecord,
  RawPolymarketPriceHistoryInsert,
  RawPolymarketPriceHistoryRecord,
  RawPolymarketTradeInsert,
  RawPolymarketTradeRecord,
  RawSnapshotRepository,
} from "../../src/repositories/index.js";
import {
  liveIplMoneylineMarket,
  liveIplTossMarket,
} from "../fixtures/polymarket/markets.js";

describe("polymarket ingestion", () => {
  it("paginates Gamma market listings and retries rate-limited requests with backoff", async () => {
    const sleep = vi.fn(async () => undefined);
    const fetchImpl = vi.fn();

    fetchImpl.mockResolvedValueOnce(
      createResponse({
        ok: false,
        status: 429,
        body: { error: "rate-limited" },
        retryAfter: "1",
      }),
    );
    fetchImpl.mockResolvedValueOnce(
      createResponse({
        ok: true,
        status: 200,
        body: [liveIplMoneylineMarket, liveIplTossMarket],
      }),
    );
    fetchImpl.mockResolvedValueOnce(
      createResponse({
        ok: true,
        status: 200,
        body: [
          {
            ...liveIplMoneylineMarket,
            id: "1683655",
            slug: "cricipl-csk-rcb-2026-03-30",
            question:
              "Indian Premier League: Chennai Super Kings vs Royal Challengers Bengaluru",
            outcomes: '["Chennai Super Kings", "Royal Challengers Bengaluru"]',
            outcomePrices: '["0.61", "0.39"]',
            events: [
              {
                id: "296912",
                slug: "cricipl-csk-rcb-2026-03-30",
                title:
                  "Indian Premier League: Chennai Super Kings vs Royal Challengers Bengaluru",
                seriesSlug: "indian-premier-league",
              },
            ],
          },
        ],
      }),
    );

    const client = createPolymarketGammaReadClient({
      fetchImpl,
      sleep,
      pageSize: 2,
      retryPolicy: {
        maxRetries: 2,
        initialDelayMs: 200,
      },
    });

    const markets = await client.listMarkets({
      tag: "ipl",
      active: true,
      closed: false,
      archived: false,
    });

    expect(markets).toHaveLength(3);
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(fetchImpl.mock.calls[0]?.[0]).toContain("offset=0");
    expect(fetchImpl.mock.calls[2]?.[0]).toContain("offset=2");
    expect(sleep).toHaveBeenCalledWith(1000);
  });

  it("dedupes repeated logical snapshots and skips unsupported IPL markets", async () => {
    const repository = createMemoryRawSnapshotRepository();
    const gammaClient = {
      listMarkets: vi.fn(async () => [
        liveIplMoneylineMarket,
        {
          ...liveIplMoneylineMarket,
          liquidityClob: 20500,
          liquidity: "20500",
        },
        liveIplTossMarket,
      ]),
    };

    const firstRun = await ingestPolymarketIplWinnerMarkets({
      gammaClient,
      rawRepository: repository,
      fetchedAt: "2026-03-29T14:40:00.000Z",
    });

    const secondRun = await ingestPolymarketIplWinnerMarkets({
      gammaClient,
      rawRepository: repository,
      fetchedAt: "2026-03-29T14:41:00.000Z",
    });

    expect(firstRun.persistedCount).toBe(1);
    expect(firstRun.duplicateCount).toBe(1);
    expect(firstRun.skippedCount).toBe(1);
    expect(secondRun.persistedCount).toBe(1);
    expect(repository.records).toHaveLength(1);
    expect(repository.records[0]?.payload["dedupeKey"]).toBe(
      "1683648:2026-03-29T14:39:20.030Z",
    );
  });
});

function createResponse(options: {
  ok: boolean;
  status: number;
  body: unknown;
  retryAfter?: string;
}): PolymarketResponseLike {
  return {
    ok: options.ok,
    status: options.status,
    headers: {
      get(name: string): string | null {
        return name.toLowerCase() === "retry-after"
          ? (options.retryAfter ?? null)
          : null;
      },
    },
    async json(): Promise<unknown> {
      return options.body;
    },
    async text(): Promise<string> {
      return JSON.stringify(options.body);
    },
  };
}

function createMemoryRawSnapshotRepository(): RawSnapshotRepository & {
  records: RawMarketSnapshotRecord[];
} {
  const storage = new Map<string, RawMarketSnapshotRecord>();
  let nextId = 1;

  const repository: RawSnapshotRepository & {
    records: RawMarketSnapshotRecord[];
  } = {
    records: [],
    async saveMarketSnapshot(snapshot) {
      const dedupeKey = `${snapshot.sourceMarketId}:${snapshot.snapshotTime}`;
      const existing = storage.get(dedupeKey);

      if (existing !== undefined) {
        const updated: RawMarketSnapshotRecord = {
          ...existing,
          ...snapshot,
        };
        storage.set(dedupeKey, updated);
        repository.records = Array.from(storage.values());
        return updated;
      }

      const created: RawMarketSnapshotRecord = {
        id: nextId,
        createdAt: snapshot.snapshotTime,
        ...snapshot,
      };
      nextId += 1;
      storage.set(dedupeKey, created);
      repository.records = Array.from(storage.values());
      return created;
    },
    async saveCricketSnapshot(
      snapshot: RawCricketSnapshotInsert,
    ): Promise<RawCricketSnapshotRecord> {
      throw new Error(
        `Unexpected cricket snapshot in market test: ${snapshot.sourceMatchId}`,
      );
    },
    async savePolymarketPriceHistoryPoint(
      point: RawPolymarketPriceHistoryInsert,
    ): Promise<RawPolymarketPriceHistoryRecord> {
      throw new Error(
        `Unexpected Polymarket price history point in market test: ${point.tokenId}`,
      );
    },
    async savePolymarketTrade(
      trade: RawPolymarketTradeInsert,
    ): Promise<RawPolymarketTradeRecord> {
      throw new Error(
        `Unexpected Polymarket trade in market test: ${trade.tokenId}`,
      );
    },
  };

  return repository;
}
