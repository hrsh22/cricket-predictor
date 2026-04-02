import { describe, expect, it } from "vitest";

import { resolveAndPersistMarketMatchMappings } from "../../src/matching/index.js";
import type {
  MarketMatchMappingInsert,
  MarketMatchMappingRecord,
  MatchResolutionCandidate,
  MatchResolutionMarketSnapshot,
  MatchingRepository,
} from "../../src/repositories/index.js";

describe("market-to-match entity resolution", () => {
  it("produces high-confidence resolved winner mappings", async () => {
    const repository = createMemoryMatchingRepository({
      markets: [
        {
          id: 1,
          competition: "IPL",
          sourceMarketId: "clean-market",
          marketSlug: "ipl-2027-lsg-gt-winner",
          eventSlug: "ipl-2027-lsg-gt",
          snapshotTime: "2027-01-10T12:00:00.000Z",
          marketStatus: "open",
          yesOutcomeName: "Lucknow Super Giants",
          noOutcomeName: "Gujarat Titans",
          outcomeProbabilities: { yes: 0.56, no: 0.44 },
          lastTradedPrice: 0.55,
          liquidity: 25000,
          payload: {
            gamma: {
              question:
                "Indian Premier League: Lucknow Super Giants vs Gujarat Titans",
            },
          },
        },
      ],
      matches: [
        {
          id: 101,
          competition: "IPL",
          matchSlug: "ipl-2027-lsg-vs-gt",
          sourceMatchId: "fixture-101",
          season: 2027,
          scheduledStart: "2027-01-10T14:00:00.000Z",
          teamAName: "Lucknow Super Giants",
          teamBName: "Gujarat Titans",
          venueName: "Lucknow",
          status: "scheduled",
          tossWinnerTeamName: null,
          tossDecision: null,
          winningTeamName: null,
          resultType: null,
        },
      ],
    });

    const summary = await resolveAndPersistMarketMatchMappings({
      repository,
      sourceMarketIds: ["clean-market"],
    });

    const mapping = summary.mappings[0];
    expect(mapping).toBeDefined();
    expect(mapping?.mappingStatus).toBe("resolved");
    expect(mapping?.canonicalMatchId).toBe(101);
    expect(mapping?.matchSlug).toBe("ipl-2027-lsg-vs-gt");
    expect((mapping?.confidence ?? 0) > 0.85).toBe(true);
  });

  it("uses market event time rather than fetch time for early-listed markets", async () => {
    const repository = createMemoryMatchingRepository({
      markets: [
        {
          id: 11,
          competition: "IPL",
          sourceMarketId: "early-listed-market",
          marketSlug: "cricipl-del-mum-2026-04-04",
          eventSlug: "cricipl-del-mum-2026-04-04",
          snapshotTime: "2026-03-30T10:00:00.000Z",
          marketStatus: "open",
          yesOutcomeName: "Delhi Capitals",
          noOutcomeName: "Mumbai Indians",
          outcomeProbabilities: { yes: 0.47, no: 0.53 },
          lastTradedPrice: 0.47,
          liquidity: 20000,
          payload: {
            gamma: {
              raw: {
                events: [
                  {
                    startTime: "2026-04-04T10:00:00.000Z",
                  },
                ],
              },
              question:
                "Indian Premier League: Delhi Capitals vs Mumbai Indians",
            },
          },
        },
      ],
      matches: [
        {
          id: 401,
          competition: "IPL",
          matchSlug: "ipl-2026-dc-vs-mi",
          sourceMatchId: "fixture-401",
          season: 2026,
          scheduledStart: "2026-04-04T10:00:00.000Z",
          teamAName: "Delhi Capitals",
          teamBName: "Mumbai Indians",
          venueName: "Delhi",
          status: "scheduled",
          tossWinnerTeamName: null,
          tossDecision: null,
          winningTeamName: null,
          resultType: null,
        },
      ],
    });

    const summary = await resolveAndPersistMarketMatchMappings({
      repository,
      sourceMarketIds: ["early-listed-market"],
    });

    expect(summary.mappings[0]?.mappingStatus).toBe("resolved");
    expect(summary.mappings[0]?.canonicalMatchId).toBe(401);
  });

  it("quarantines duplicate markets targeting the same match", async () => {
    const repository = createMemoryMatchingRepository({
      markets: [
        {
          id: 1,
          competition: "IPL",
          sourceMarketId: "dup-market-a",
          marketSlug: "ipl-2027-rr-rcb-winner-a",
          eventSlug: "ipl-2027-rr-rcb",
          snapshotTime: "2027-01-11T11:00:00.000Z",
          marketStatus: "open",
          yesOutcomeName: "Rajasthan Royals",
          noOutcomeName: "Royal Challengers Bengaluru",
          outcomeProbabilities: { yes: 0.52, no: 0.48 },
          lastTradedPrice: 0.51,
          liquidity: 21000,
          payload: {
            gamma: {
              question:
                "Indian Premier League: Rajasthan Royals vs Royal Challengers Bengaluru",
            },
          },
        },
        {
          id: 2,
          competition: "IPL",
          sourceMarketId: "dup-market-b",
          marketSlug: "ipl-2027-rr-rcb-winner-b",
          eventSlug: "ipl-2027-rr-rcb",
          snapshotTime: "2027-01-11T11:02:00.000Z",
          marketStatus: "open",
          yesOutcomeName: "Rajasthan Royals",
          noOutcomeName: "Royal Challengers Bengaluru",
          outcomeProbabilities: { yes: 0.53, no: 0.47 },
          lastTradedPrice: 0.52,
          liquidity: 22000,
          payload: {
            gamma: {
              question:
                "Indian Premier League: Rajasthan Royals vs Royal Challengers Bengaluru",
            },
          },
        },
      ],
      matches: [
        {
          id: 201,
          competition: "IPL",
          matchSlug: "ipl-2027-rr-vs-rcb",
          sourceMatchId: "fixture-201",
          season: 2027,
          scheduledStart: "2027-01-11T14:00:00.000Z",
          teamAName: "Rajasthan Royals",
          teamBName: "Royal Challengers Bengaluru",
          venueName: "Jaipur",
          status: "scheduled",
          tossWinnerTeamName: null,
          tossDecision: null,
          winningTeamName: null,
          resultType: null,
        },
      ],
    });

    const summary = await resolveAndPersistMarketMatchMappings({
      repository,
      sourceMarketIds: ["dup-market-a", "dup-market-b"],
    });

    expect(summary.mappings).toHaveLength(2);
    expect(
      summary.mappings.every(
        (mapping) => mapping.mappingStatus === "ambiguous",
      ),
    ).toBe(true);
    expect(
      summary.mappings.every(
        (mapping) => mapping.reason === "duplicate_market_for_match",
      ),
    ).toBe(true);

    const scorerEligible = await repository.listScorerEligibleMappings();
    expect(scorerEligible).toHaveLength(0);
  });

  it("marks close naming collisions as ambiguous", async () => {
    const repository = createMemoryMatchingRepository({
      markets: [
        {
          id: 1,
          competition: "IPL",
          sourceMarketId: "ambiguous-market",
          marketSlug: "cricipl-mum-vs-roy-2027-01-12",
          eventSlug: "cricipl-mum-vs-roy-2027-01-12",
          snapshotTime: "2027-01-12T10:00:00.000Z",
          marketStatus: "open",
          yesOutcomeName: "Mumbai Indians",
          noOutcomeName: "Roy",
          outcomeProbabilities: { yes: 0.58, no: 0.42 },
          lastTradedPrice: 0.57,
          liquidity: 18000,
          payload: {
            gamma: {
              question: "Indian Premier League: Mumbai Indians vs Roy",
            },
          },
        },
      ],
      matches: [
        {
          id: 301,
          competition: "IPL",
          matchSlug: "ipl-2027-mi-vs-rr",
          sourceMatchId: "fixture-301",
          season: 2027,
          scheduledStart: "2027-01-12T13:30:00.000Z",
          teamAName: "Mumbai Indians",
          teamBName: "Rajasthan Royals",
          venueName: "Wankhede",
          status: "scheduled",
          tossWinnerTeamName: null,
          tossDecision: null,
          winningTeamName: null,
          resultType: null,
        },
        {
          id: 302,
          competition: "IPL",
          matchSlug: "ipl-2027-mi-vs-rcb",
          sourceMatchId: "fixture-302",
          season: 2027,
          scheduledStart: "2027-01-12T14:00:00.000Z",
          teamAName: "Mumbai Indians",
          teamBName: "Royal Challengers Bengaluru",
          venueName: "Chinnaswamy",
          status: "scheduled",
          tossWinnerTeamName: null,
          tossDecision: null,
          winningTeamName: null,
          resultType: null,
        },
      ],
    });

    const summary = await resolveAndPersistMarketMatchMappings({
      repository,
      sourceMarketIds: ["ambiguous-market"],
      resolverOptions: {
        minimumConfidence: 0.7,
        minimumMargin: 0.15,
      },
    });

    const mapping = summary.mappings[0];
    expect(mapping?.mappingStatus).toBe("ambiguous");
    expect(mapping?.reason).toBe("multiple_close_candidates");
    expect(mapping?.canonicalMatchId).toBeNull();

    const scorerEligible = await repository.listScorerEligibleMappings({
      minimumConfidence: 0.7,
    });
    expect(scorerEligible).toHaveLength(0);
  });
});

function createMemoryMatchingRepository(input: {
  markets: MatchResolutionMarketSnapshot[];
  matches: MatchResolutionCandidate[];
}): MatchingRepository {
  let nextMappingId = 1;
  const persisted = new Map<string, MarketMatchMappingRecord>();

  return {
    async listLatestMarketSnapshots(): Promise<
      MatchResolutionMarketSnapshot[]
    > {
      return input.markets;
    },
    async listCanonicalMatchesForWindow(): Promise<MatchResolutionCandidate[]> {
      return input.matches;
    },
    async saveMarketMatchMapping(
      mapping: MarketMatchMappingInsert,
    ): Promise<MarketMatchMappingRecord> {
      const existing = persisted.get(mapping.sourceMarketId);
      const relatedMatch =
        mapping.canonicalMatchId === null
          ? null
          : (input.matches.find(
              (match) => match.id === mapping.canonicalMatchId,
            ) ?? null);
      const nowIso = new Date().toISOString();

      const created: MarketMatchMappingRecord = {
        id: existing?.id ?? nextMappingId,
        competition: "IPL",
        sourceMarketId: mapping.sourceMarketId,
        sourceMarketSnapshotId: mapping.sourceMarketSnapshotId,
        canonicalMatchId: mapping.canonicalMatchId,
        matchSlug: relatedMatch?.matchSlug ?? null,
        mappingStatus: mapping.mappingStatus,
        confidence: mapping.confidence,
        resolverVersion: mapping.resolverVersion,
        reason: mapping.reason,
        payload: mapping.payload,
        createdAt: existing?.createdAt ?? nowIso,
        updatedAt: nowIso,
      };

      if (existing === undefined) {
        nextMappingId += 1;
      }

      persisted.set(mapping.sourceMarketId, created);
      return created;
    },
    async listScorerEligibleMappings(inputOptions?: {
      minimumConfidence?: number;
    }): Promise<MarketMatchMappingRecord[]> {
      const minimumConfidence = inputOptions?.minimumConfidence ?? 0.85;

      return Array.from(persisted.values()).filter(
        (mapping) =>
          mapping.mappingStatus === "resolved" &&
          mapping.canonicalMatchId !== null &&
          mapping.confidence !== null &&
          mapping.confidence >= minimumConfidence,
      );
    },
  };
}
