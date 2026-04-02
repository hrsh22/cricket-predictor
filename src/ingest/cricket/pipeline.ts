import {
  parseCanonicalCheckpoint,
  type CanonicalCheckpoint,
  type CanonicalMatch,
} from "../../domain/index.js";
import { isRecord, type JsonObject } from "../../domain/primitives.js";
import type {
  CanonicalMatchRecord,
  CheckpointStateRecord,
  NormalizedRepository,
} from "../../repositories/normalized.js";
import type {
  RawCricketSnapshotInsert,
  RawCricketSnapshotRecord,
  RawSnapshotRepository,
} from "../../repositories/raw.js";
import {
  type AdapterIssue,
  type AdapterRetrievalResult,
  type CricketDataProviderAdapter,
  type CricketFinalResult,
  type CricketInningsState,
  type CricketLifecycleState,
  type CricketProviderKey,
  type CricketTossState,
} from "./adapter.js";
import { cricapiAdapter } from "./cricapi.js";

const supportedAdapters: Record<
  CricketProviderKey,
  CricketDataProviderAdapter
> = {
  cricapi: cricapiAdapter,
};

export interface CricketSnapshotInput {
  snapshotTime: string;
  payload: unknown;
  stateVersion?: number;
}

export interface CricketNormalizationResult {
  provider: CricketProviderKey;
  snapshotTime: string;
  sourceMatchId: string;
  lifecycle: CricketLifecycleState | null;
  status: "normalized" | "degraded";
  rawSnapshot: RawCricketSnapshotInsert;
  canonicalMatch: CanonicalMatch | null;
  checkpoint: CanonicalCheckpoint | null;
  degradationReason: string | null;
  issues: readonly AdapterIssue[];
}

export interface CricketIngestResult {
  provider: CricketProviderKey;
  snapshotTime: string;
  sourceMatchId: string;
  lifecycle: CricketLifecycleState | null;
  status: "normalized" | "degraded";
  rawSnapshot: RawCricketSnapshotRecord;
  canonicalMatch: CanonicalMatchRecord | null;
  checkpoint: CheckpointStateRecord | null;
  degradationReason: string | null;
  issues: readonly AdapterIssue[];
}

export interface CricketIngestSummary {
  provider: CricketProviderKey;
  totalSnapshots: number;
  normalizedSnapshots: number;
  degradedSnapshots: number;
  checkpointSnapshots: number;
  finalResultSnapshots: number;
  results: readonly CricketIngestResult[];
}

interface CricketIngestRepositories {
  raw: Pick<RawSnapshotRepository, "saveCricketSnapshot">;
  normalized: Pick<
    NormalizedRepository,
    "saveCanonicalMatch" | "saveCheckpoint"
  >;
}

interface CoverageFlags {
  dlsApplied: boolean;
  noResult: boolean;
  superOver: boolean;
  reducedOvers: boolean;
  incomplete: boolean;
}

interface RawSnapshotMetadata {
  sourceMatchId: string;
  matchStatus: string | null;
  inningsNumber: number | null;
  overNumber: number | null;
}

export function getCricketAdapter(
  providerKey: CricketProviderKey,
): CricketDataProviderAdapter {
  return supportedAdapters[providerKey];
}

export function normalizeCricketSnapshot(
  input: CricketSnapshotInput,
  providerKey: CricketProviderKey = "cricapi",
): CricketNormalizationResult {
  const adapter = getCricketAdapter(providerKey);
  const snapshotTime = assertSnapshotTime(input.snapshotTime);
  const rawMetadata = extractRawSnapshotMetadata(snapshotTime, input.payload);
  const rawSnapshot: RawCricketSnapshotInsert = {
    provider: adapter.providerKey,
    sourceMatchId: rawMetadata.sourceMatchId,
    snapshotTime,
    matchStatus: rawMetadata.matchStatus,
    inningsNumber: rawMetadata.inningsNumber,
    overNumber: rawMetadata.overNumber,
    payload: assertJsonPayload(input.payload),
  };

  const fixture = adapter.getFixture(input.payload);
  if (fixture.status !== "available") {
    return {
      provider: adapter.providerKey,
      snapshotTime,
      sourceMatchId: rawMetadata.sourceMatchId,
      lifecycle: null,
      status: "degraded",
      rawSnapshot,
      canonicalMatch: null,
      checkpoint: null,
      degradationReason:
        fixture.status === "degraded"
          ? "fixture_unavailable"
          : "fixture_not_reported",
      issues: toIssues(fixture),
    };
  }

  const toss = adapter.getToss(input.payload);
  const innings = adapter.getInningsState(input.payload);
  const finalResult = adapter.getFinalResult(input.payload);
  const lifecycle = adapter.getLifecycleState(input.payload);

  if (lifecycle.status !== "available") {
    return {
      provider: adapter.providerKey,
      snapshotTime,
      sourceMatchId: rawMetadata.sourceMatchId,
      lifecycle: null,
      status: "degraded",
      rawSnapshot,
      canonicalMatch: fixture.value,
      checkpoint: null,
      degradationReason: "lifecycle_degraded",
      issues: toIssues(lifecycle),
    };
  }

  const coverageFlags = detectCoverageFlags(
    input.payload,
    finalResult,
    lifecycle.value,
  );
  if (coverageFlags.dlsApplied || coverageFlags.reducedOvers) {
    return {
      provider: adapter.providerKey,
      snapshotTime,
      sourceMatchId: rawMetadata.sourceMatchId,
      lifecycle: lifecycle.value,
      status: "degraded",
      rawSnapshot,
      canonicalMatch: null,
      checkpoint: null,
      degradationReason: "unsupported_provider_coverage",
      issues: buildCoverageIssues(coverageFlags),
    };
  }

  if (lifecycle.value === "final_result") {
    return {
      provider: adapter.providerKey,
      snapshotTime,
      sourceMatchId: rawMetadata.sourceMatchId,
      lifecycle: lifecycle.value,
      status: "normalized",
      rawSnapshot,
      canonicalMatch: fixture.value,
      checkpoint: null,
      degradationReason: null,
      issues: [],
    };
  }

  const checkpoint = buildCheckpoint({
    adapter,
    snapshotTime,
    sourceMatchId: rawMetadata.sourceMatchId,
    stateVersion: input.stateVersion ?? 1,
    lifecycle: lifecycle.value,
    match: fixture.value,
    toss,
    innings,
    finalResult,
    coverageFlags,
  });

  return {
    provider: adapter.providerKey,
    snapshotTime,
    sourceMatchId: rawMetadata.sourceMatchId,
    lifecycle: lifecycle.value,
    status: "normalized",
    rawSnapshot,
    canonicalMatch: fixture.value,
    checkpoint,
    degradationReason: null,
    issues: [],
  };
}

export async function ingestCricketSnapshots(
  repositories: CricketIngestRepositories,
  snapshots: readonly CricketSnapshotInput[],
  providerKey: CricketProviderKey = "cricapi",
): Promise<CricketIngestSummary> {
  const results: CricketIngestResult[] = [];

  for (const snapshot of snapshots) {
    const normalized = normalizeCricketSnapshot(snapshot, providerKey);
    const rawSnapshot = await repositories.raw.saveCricketSnapshot(
      normalized.rawSnapshot,
    );

    let canonicalMatch: CanonicalMatchRecord | null = null;
    let checkpoint: CheckpointStateRecord | null = null;

    if (
      normalized.status === "normalized" &&
      normalized.canonicalMatch !== null
    ) {
      canonicalMatch = await repositories.normalized.saveCanonicalMatch(
        normalized.canonicalMatch,
      );
    }

    if (
      normalized.status === "normalized" &&
      normalized.checkpoint !== null &&
      normalized.canonicalMatch !== null
    ) {
      checkpoint = await repositories.normalized.saveCheckpoint({
        checkpointType: normalized.checkpoint.checkpointType,
        match: normalized.checkpoint.match,
        state: {
          ...normalized.checkpoint.state,
          sourceCricketSnapshotId: rawSnapshot.id,
        },
      });
    }

    results.push({
      provider: normalized.provider,
      snapshotTime: normalized.snapshotTime,
      sourceMatchId: normalized.sourceMatchId,
      lifecycle: normalized.lifecycle,
      status: normalized.status,
      rawSnapshot,
      canonicalMatch,
      checkpoint,
      degradationReason: normalized.degradationReason,
      issues: normalized.issues,
    });
  }

  return {
    provider: providerKey,
    totalSnapshots: results.length,
    normalizedSnapshots: results.filter(
      (result) => result.status === "normalized",
    ).length,
    degradedSnapshots: results.filter((result) => result.status === "degraded")
      .length,
    checkpointSnapshots: results.filter((result) => result.checkpoint !== null)
      .length,
    finalResultSnapshots: results.filter(
      (result) =>
        result.status === "normalized" &&
        result.lifecycle === "final_result" &&
        result.checkpoint === null,
    ).length,
    results,
  };
}

function buildCheckpoint(input: {
  adapter: CricketDataProviderAdapter;
  snapshotTime: string;
  sourceMatchId: string;
  stateVersion: number;
  lifecycle: Exclude<CricketLifecycleState, "final_result">;
  match: CanonicalMatch;
  toss: AdapterRetrievalResult<CricketTossState>;
  innings: AdapterRetrievalResult<CricketInningsState>;
  finalResult: AdapterRetrievalResult<CricketFinalResult>;
  coverageFlags: CoverageFlags;
}): CanonicalCheckpoint {
  const statePayload = buildStatePayload({
    provider: input.adapter.providerKey,
    sourceMatchId: input.sourceMatchId,
    lifecycle: input.lifecycle,
    match: input.match,
    toss: input.toss,
    innings: input.innings,
    finalResult: input.finalResult,
    coverageFlags: input.coverageFlags,
  });

  if (input.lifecycle === "pre_match") {
    return parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: input.match,
      state: {
        matchSlug: input.match.matchSlug,
        checkpointType: "pre_match",
        snapshotTime: input.snapshotTime,
        stateVersion: input.stateVersion,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload,
      },
    });
  }

  if (input.lifecycle === "post_toss") {
    if (input.toss.status !== "available") {
      throw new Error("post_toss lifecycle requires toss data");
    }

    return parseCanonicalCheckpoint({
      checkpointType: "post_toss",
      match: input.match,
      state: {
        matchSlug: input.match.matchSlug,
        checkpointType: "post_toss",
        snapshotTime: input.snapshotTime,
        stateVersion: input.stateVersion,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload,
      },
    });
  }

  if (input.innings.status !== "available") {
    throw new Error("innings_break lifecycle requires innings data");
  }

  return parseCanonicalCheckpoint({
    checkpointType: "innings_break",
    match: input.match,
    state: {
      matchSlug: input.match.matchSlug,
      checkpointType: "innings_break",
      snapshotTime: input.snapshotTime,
      stateVersion: input.stateVersion,
      sourceMarketSnapshotId: null,
      sourceCricketSnapshotId: null,
      inningsNumber: input.innings.value.inningsNumber,
      battingTeamName: input.innings.value.battingTeamName,
      bowlingTeamName: input.innings.value.bowlingTeamName,
      runs: input.innings.value.runs,
      wickets: input.innings.value.wickets,
      overs: input.innings.value.overs,
      targetRuns: input.innings.value.targetRuns,
      currentRunRate: input.innings.value.currentRunRate,
      requiredRunRate: input.innings.value.requiredRunRate,
      statePayload,
    },
  });
}

function buildStatePayload(input: {
  provider: CricketProviderKey;
  sourceMatchId: string;
  lifecycle: Exclude<CricketLifecycleState, "final_result">;
  match: CanonicalMatch;
  toss: AdapterRetrievalResult<CricketTossState>;
  innings: AdapterRetrievalResult<CricketInningsState>;
  finalResult: AdapterRetrievalResult<CricketFinalResult>;
  coverageFlags: CoverageFlags;
}): JsonObject {
  return {
    provider: input.provider,
    sourceMatchId: input.sourceMatchId,
    lifecycle: input.lifecycle,
    pointInTimeMatch: input.match as unknown as JsonObject,
    toss: mapRetrievalResultToJson(
      input.toss as AdapterRetrievalResult<JsonObject>,
    ),
    innings: mapRetrievalResultToJson(
      input.innings as AdapterRetrievalResult<JsonObject>,
    ),
    result: mapRetrievalResultToJson(
      input.finalResult as AdapterRetrievalResult<JsonObject>,
    ),
    coverage: {
      dlsApplied: input.coverageFlags.dlsApplied,
      noResult: input.coverageFlags.noResult,
      superOver: input.coverageFlags.superOver,
      reducedOvers: input.coverageFlags.reducedOvers,
      incomplete: input.coverageFlags.incomplete,
    },
  };
}

function mapRetrievalResultToJson(
  result: AdapterRetrievalResult<JsonObject>,
): JsonObject {
  if (result.status === "available") {
    return {
      status: "available",
      value: result.value,
    };
  }

  if (result.status === "unavailable") {
    return {
      status: "unavailable",
      reason: result.reason,
    };
  }

  return {
    status: "degraded",
    issues: result.issues.map((issue) => ({
      path: issue.path,
      message: issue.message,
    })),
  };
}

function assertSnapshotTime(snapshotTime: string): string {
  if (Number.isNaN(Date.parse(snapshotTime))) {
    throw new Error(`Invalid cricket snapshotTime \"${snapshotTime}\".`);
  }

  return snapshotTime;
}

function assertJsonPayload(payload: unknown): JsonObject {
  if (!isRecord(payload)) {
    throw new Error("Cricket ingest payload must be a plain object.");
  }

  return payload as JsonObject;
}

function extractRawSnapshotMetadata(
  snapshotTime: string,
  payload: unknown,
): RawSnapshotMetadata {
  if (!isRecord(payload)) {
    throw new Error(
      `Cricket snapshot at ${snapshotTime} must be a plain object payload.`,
    );
  }

  const sourceMatchId = parseSourceMatchId(payload["id"]);
  if (sourceMatchId === null) {
    throw new Error(
      `Cricket snapshot at ${snapshotTime} is missing a provider match id.`,
    );
  }

  const scoreEntries = Array.isArray(payload["score"]) ? payload["score"] : [];
  const lastScore = scoreEntries.at(-1);
  const overNumber = parseOverNumber(lastScore);
  const inningsNumber =
    scoreEntries.length === 0 ? null : Math.min(scoreEntries.length, 2);
  const matchStatus = parseOptionalStatusText(payload["status"]);

  return {
    sourceMatchId,
    matchStatus,
    inningsNumber,
    overNumber,
  };
}

function parseSourceMatchId(value: unknown): string | null {
  if (typeof value === "string" && value.trim().length > 0) {
    return value.trim();
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  return null;
}

function parseOptionalStatusText(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function parseOverNumber(value: unknown): number | null {
  if (!isRecord(value)) {
    return null;
  }

  const overs = value["o"];
  if (typeof overs !== "number" || !Number.isFinite(overs) || overs < 0) {
    return null;
  }

  return overs;
}

function detectCoverageFlags(
  payload: unknown,
  finalResult: AdapterRetrievalResult<CricketFinalResult>,
  lifecycle: CricketLifecycleState,
): CoverageFlags {
  const statusText = isRecord(payload)
    ? parseOptionalStatusText(payload["status"])
    : null;
  const normalizedStatus = statusText?.toLowerCase() ?? "";

  const hasDlsToken =
    normalizedStatus.includes("dls") ||
    normalizedStatus.includes("duckworth") ||
    normalizedStatus.includes("lewis") ||
    normalizedStatus.includes("vjd");
  const reducedOvers =
    hasDlsToken ||
    normalizedStatus.includes("reduced to") ||
    normalizedStatus.includes("revised target") ||
    normalizedStatus.includes("rain-reduced");
  const noResult =
    lifecycle === "final_result" &&
    finalResult.status === "available" &&
    finalResult.value.resultType === "no_result";
  const superOver =
    lifecycle === "final_result" &&
    finalResult.status === "available" &&
    finalResult.value.resultType === "super_over";

  return {
    dlsApplied:
      hasDlsToken ||
      (isRecord(payload) &&
        (payload["dlsApplied"] === true || payload["isDls"] === true)),
    noResult,
    superOver,
    reducedOvers:
      reducedOvers ||
      (isRecord(payload) &&
        (payload["reducedOvers"] === true ||
          payload["revisedTarget"] === true)),
    incomplete: false,
  };
}

function buildCoverageIssues(flags: CoverageFlags): AdapterIssue[] {
  const issues: AdapterIssue[] = [];

  if (flags.dlsApplied) {
    issues.push({
      path: "cricapi.status",
      message:
        "DLS or revised-target payloads are persisted raw only until reduced-over normalization is modeled explicitly",
    });
  }

  if (flags.reducedOvers) {
    issues.push({
      path: "cricapi.status",
      message:
        "reduced-over payloads are persisted raw only because innings-break normalization assumes a full 20-over denominator",
    });
  }

  return issues;
}

function toIssues(result: {
  status: string;
  reason?: string;
  issues?: readonly AdapterIssue[];
}): readonly AdapterIssue[] {
  if (result.status === "degraded") {
    return result.issues ?? [];
  }

  if (result.status === "unavailable") {
    return [
      {
        path: "cricket",
        message: result.reason ?? "provider state unavailable",
      },
    ];
  }

  return [];
}
