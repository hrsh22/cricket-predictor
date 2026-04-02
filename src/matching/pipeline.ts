import type {
  MappingStatus,
  MarketMatchMappingRecord,
  MatchingRepository,
  ResolverCandidate,
} from "../repositories/matching.js";
import {
  getMarketReferenceTime,
  resolveMarketMatchCandidate,
  type MatchResolverOptions,
} from "./resolver.js";

const RESOLVER_VERSION = "task10-v1";

export interface MatchResolutionPipelineOptions {
  repository: MatchingRepository;
  resolverOptions?: MatchResolverOptions;
  candidatePaddingHours?: number;
  sourceMarketIds?: readonly string[];
}

export interface MatchResolutionPipelineSummary {
  resolverVersion: string;
  totalMarkets: number;
  resolvedCount: number;
  ambiguousCount: number;
  unresolvedCount: number;
  scorerEligibleCount: number;
  mappings: MarketMatchMappingRecord[];
}

export async function resolveAndPersistMarketMatchMappings(
  options: MatchResolutionPipelineOptions,
): Promise<MatchResolutionPipelineSummary> {
  const latestMarketSnapshots =
    await options.repository.listLatestMarketSnapshots();
  const scopedMarketSnapshots =
    options.sourceMarketIds === undefined
      ? latestMarketSnapshots
      : latestMarketSnapshots.filter(
          (snapshot) =>
            options.sourceMarketIds?.includes(snapshot.sourceMarketId) ?? false,
        );

  if (scopedMarketSnapshots.length === 0) {
    return {
      resolverVersion: RESOLVER_VERSION,
      totalMarkets: 0,
      resolvedCount: 0,
      ambiguousCount: 0,
      unresolvedCount: 0,
      scorerEligibleCount: 0,
      mappings: [],
    };
  }

  const candidatePaddingHours = options.candidatePaddingHours ?? 72;
  const snapshotTimes = scopedMarketSnapshots.map((snapshot) =>
    Date.parse(getMarketReferenceTime(snapshot)),
  );
  const minSnapshotTimeMs = Math.min(...snapshotTimes);
  const maxSnapshotTimeMs = Math.max(...snapshotTimes);
  const from = new Date(
    minSnapshotTimeMs - candidatePaddingHours * 60 * 60 * 1000,
  ).toISOString();
  const to = new Date(
    maxSnapshotTimeMs + candidatePaddingHours * 60 * 60 * 1000,
  ).toISOString();

  const candidateMatches =
    await options.repository.listCanonicalMatchesForWindow({
      from,
      to,
    });

  const preliminary = scopedMarketSnapshots.map((market) =>
    resolveMarketMatchCandidate({
      market,
      matches: candidateMatches,
      ...(options.resolverOptions === undefined
        ? {}
        : { options: options.resolverOptions }),
    }),
  );

  const duplicateMatchIds = findDuplicateResolvedMatchIds(preliminary);
  const mappings: MarketMatchMappingRecord[] = [];

  for (const resolution of preliminary) {
    const duplicateResolvedMatch =
      resolution.mappingStatus === "resolved" &&
      resolution.canonicalMatchId !== null &&
      duplicateMatchIds.has(resolution.canonicalMatchId);

    const mappingStatus: MappingStatus = duplicateResolvedMatch
      ? "ambiguous"
      : resolution.mappingStatus;
    const canonicalMatchId = duplicateResolvedMatch
      ? null
      : resolution.canonicalMatchId;
    const reason = duplicateResolvedMatch
      ? "duplicate_market_for_match"
      : resolution.reason;

    const persisted = await options.repository.saveMarketMatchMapping({
      sourceMarketId: resolution.sourceMarketId,
      sourceMarketSnapshotId: resolution.sourceMarketSnapshotId,
      canonicalMatchId,
      mappingStatus,
      confidence: resolution.confidence,
      resolverVersion: RESOLVER_VERSION,
      reason,
      payload: {
        evaluatedAt: new Date().toISOString(),
        duplicateResolvedMatch,
        candidates: resolution.candidates.map((candidate) => ({
          canonicalMatchId: candidate.canonicalMatchId,
          matchSlug: candidate.matchSlug,
          confidence: candidate.confidence,
        })),
      },
    });

    mappings.push(persisted);
  }

  const scorerEligible = await options.repository.listScorerEligibleMappings();

  return {
    resolverVersion: RESOLVER_VERSION,
    totalMarkets: mappings.length,
    resolvedCount: mappings.filter(
      (mapping) => mapping.mappingStatus === "resolved",
    ).length,
    ambiguousCount: mappings.filter(
      (mapping) => mapping.mappingStatus === "ambiguous",
    ).length,
    unresolvedCount: mappings.filter(
      (mapping) => mapping.mappingStatus === "unresolved",
    ).length,
    scorerEligibleCount: scorerEligible.length,
    mappings,
  };
}

function findDuplicateResolvedMatchIds(
  resolutions: readonly {
    mappingStatus: MappingStatus;
    canonicalMatchId: number | null;
  }[],
): Set<number> {
  const counts = new Map<number, number>();

  for (const resolution of resolutions) {
    if (
      resolution.mappingStatus === "resolved" &&
      resolution.canonicalMatchId !== null
    ) {
      counts.set(
        resolution.canonicalMatchId,
        (counts.get(resolution.canonicalMatchId) ?? 0) + 1,
      );
    }
  }

  const duplicates = new Set<number>();
  for (const [matchId, count] of counts.entries()) {
    if (count > 1) {
      duplicates.add(matchId);
    }
  }

  return duplicates;
}

export type { ResolverCandidate };
