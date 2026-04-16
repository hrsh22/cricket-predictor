import type { OpticOddsConfig } from "../../config/index.js";
import {
  isRecord,
  type JsonObject,
  type JsonValue,
} from "../../domain/primitives.js";
import type {
  OpticOddsRepository,
  OpticOddsBallOddsSnapshotInsert,
  RawOpticOddsFixtureInsert,
  RawOpticOddsOddsEventInsert,
  RawOpticOddsResultsEventInsert,
} from "../../repositories/opticodds.js";
import {
  createOpticOddsApiClient,
  type OpticOddsApiClient,
  type OpticOddsFixture,
  type OpticOddsOdd,
  type OpticOddsResultsEnvelope,
} from "./client.js";

export interface OpticOddsBallContext {
  fixtureId: string;
  seasonYear: number | null;
  fixtureStartDate: string;
  fixtureStatus: string;
  isLive: boolean;
  period: string | null;
  periodNumber: number | null;
  ballClock: string | null;
  ballKey: string | null;
  snapshotTime: string;
  homeTeamName: string;
  awayTeamName: string;
  homeScore: number | null;
  awayScore: number | null;
  sourceResultsDedupeKey: string | null;
}

export interface NormalizedOpticOddsResults {
  fixtureId: string;
  status: string;
  isLive: boolean;
  period: string | null;
  periodNumber: number | null;
  ballClock: string | null;
  ballKey: string | null;
  homeScore: number | null;
  awayScore: number | null;
  snapshotTime: string;
  payload: JsonObject;
}

export interface OpticOddsBallByBallIngestionOptions {
  repository: OpticOddsRepository;
  config: OpticOddsConfig;
  client?: OpticOddsApiClient;
  once?: boolean;
  signal?: AbortSignal;
  sleep?: (milliseconds: number) => Promise<void>;
  now?: () => Date;
}

export interface OpticOddsBallByBallIngestionSummary {
  startedAt: string;
  fixtureRefreshCount: number;
  fixturesUpserted: number;
  watchedFixtureCount: number;
  liveFixtureCount: number;
  bootstrappedOddsEvents: number;
  bootstrappedResultsEvents: number;
  streamedOddsEvents: number;
  streamedResultsEvents: number;
  ballSnapshotsUpserted: number;
  cursorUpdates: number;
  reconnectCount: number;
  lastFixtureRefreshAt: string | null;
  sportsbookBatches: string[][];
}

interface CachedOddState {
  sourceOddId: string;
  sportsbookId: string;
  sportsbookName: string;
  marketId: string;
  marketName: string;
  selection: string;
  normalizedSelection: string;
  teamId: string | null;
  playerId: string | null;
  groupingKey: string | null;
  isMain: boolean;
  isLive: boolean;
  isLocked: boolean;
  price: number | null;
  points: number | null;
  eventTime: string;
  orderBook: JsonValue | null;
  limits: JsonObject | null;
  sourceIds: JsonObject | null;
  payload: JsonObject;
  sourceOddsDedupeKey: string | null;
}

interface MutableServiceState {
  fixturesById: Map<string, RawOpticOddsFixtureInsert>;
  latestBallContextByFixtureId: Map<string, OpticOddsBallContext>;
  oddsCacheByFixtureId: Map<string, Map<string, CachedOddState>>;
  watchedFixtureIds: string[];
  watchedFixtureVersion: number;
  bootstrappedOddsFixtureIds: Set<string>;
  bootstrappedResultsFixtureIds: Set<string>;
}

interface ServerSentEvent {
  event: string;
  id: string | null;
  data: string;
}

interface StreamEnvelope {
  entry_id?: string;
  type?: string;
  data?: unknown;
}

const MAX_IDLE_FIXTURE_REFRESH_MS = 6 * 60 * 60 * 1000;
const WATCH_START_BUFFER_MS = 60 * 1000;

export async function runOpticOddsBallByBallIngestion(
  options: OpticOddsBallByBallIngestionOptions,
): Promise<OpticOddsBallByBallIngestionSummary> {
  const client =
    options.client ??
    createOpticOddsApiClient({
      apiKey: requireOpticOddsApiKey(options.config),
      baseUrl: options.config.baseUrl,
    });
  const state: MutableServiceState = {
    fixturesById: new Map(),
    latestBallContextByFixtureId: new Map(),
    oddsCacheByFixtureId: new Map(),
    watchedFixtureIds: [],
    watchedFixtureVersion: 0,
    bootstrappedOddsFixtureIds: new Set(),
    bootstrappedResultsFixtureIds: new Set(),
  };
  const summary: OpticOddsBallByBallIngestionSummary = {
    startedAt: new Date().toISOString(),
    fixtureRefreshCount: 0,
    fixturesUpserted: 0,
    watchedFixtureCount: 0,
    liveFixtureCount: 0,
    bootstrappedOddsEvents: 0,
    bootstrappedResultsEvents: 0,
    streamedOddsEvents: 0,
    streamedResultsEvents: 0,
    ballSnapshotsUpserted: 0,
    cursorUpdates: 0,
    reconnectCount: 0,
    lastFixtureRefreshAt: null,
    sportsbookBatches: chunkSportsbookIds(options.config.sportsbookIds),
  };
  const sleep = options.sleep ?? defaultSleep;
  const now = options.now ?? (() => new Date());
  const signal = options.signal;

  await refreshFixturesAndBootstrap({
    client,
    config: options.config,
    now,
    repository: options.repository,
    state,
    summary,
  });

  if (options.once === true) {
    return summary;
  }

  const loops = [
    runFixtureRefreshLoop({
      client,
      config: options.config,
      now,
      repository: options.repository,
      ...(signal === undefined ? {} : { signal }),
      sleep,
      state,
      summary,
    }),
    runLiveResultsPollingLoop({
      client,
      config: options.config,
      now,
      repository: options.repository,
      ...(signal === undefined ? {} : { signal }),
      sleep,
      state,
      summary,
    }),
    runResultsStreamLoop({
      client,
      config: options.config,
      repository: options.repository,
      ...(signal === undefined ? {} : { signal }),
      now,
      sleep,
      state,
      summary,
    }),
    ...summary.sportsbookBatches.map((sportsbookIds) =>
      runOddsStreamLoop({
        client,
        config: options.config,
        repository: options.repository,
        ...(signal === undefined ? {} : { signal }),
        now,
        sleep,
        sportsbookIds,
        state,
        summary,
      }),
    ),
  ];

  await Promise.all(loops);
  return summary;
}

export function chunkSportsbookIds(
  sportsbookIds: readonly string[],
  batchSize = 5,
): string[][] {
  const batches: string[][] = [];
  for (let index = 0; index < sportsbookIds.length; index += batchSize) {
    batches.push([...sportsbookIds.slice(index, index + batchSize)]);
  }
  return batches.length === 0 ? [["polymarket"]] : batches;
}

export function getFixtureWatchStartTime(
  startDate: string,
  config: Pick<
    OpticOddsConfig,
    "assumedTossLeadMinutesBeforeStart" | "streamStartLeadMinutesBeforeToss"
  >,
): string | null {
  const startTime = Date.parse(startDate);
  if (!Number.isFinite(startTime)) {
    return null;
  }

  return new Date(
    startTime -
      (config.assumedTossLeadMinutesBeforeStart +
        config.streamStartLeadMinutesBeforeToss) *
        60_000,
  ).toISOString();
}

export function isTerminalFixtureStatus(status: string): boolean {
  const normalized = status.trim().toLowerCase();
  return [
    "completed",
    "complete",
    "final",
    "finished",
    "ended",
    "cancelled",
    "canceled",
    "abandoned",
    "result",
    "closed",
  ].includes(normalized);
}

export function selectFixturesToWatch(
  fixtures: readonly OpticOddsFixture[],
  now: Date,
  config: Pick<
    OpticOddsConfig,
    "assumedTossLeadMinutesBeforeStart" | "streamStartLeadMinutesBeforeToss"
  >,
  watchedFixtureIds: readonly string[] = [],
): readonly OpticOddsFixture[] {
  const watchedFixtureIdSet = new Set(watchedFixtureIds);
  return fixtures.filter((fixture) =>
    shouldWatchFixture(
      {
        startDate: fixture.start_date,
        status: fixture.status,
        isLive: fixture.is_live,
      },
      now,
      config,
      watchedFixtureIdSet.has(fixture.id),
    ),
  );
}

export function createSportsbookSlug(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "_")
    .replace(/^_+|_+$/gu, "")
    .replace(/_+/gu, "_");
}

export function buildResultsEventDedupeKey(input: {
  eventSource: "stream" | "bootstrap";
  fixtureId: string;
  eventEntryId: string | null;
  snapshotTime: string;
  ballKey: string | null;
}): string {
  if (input.eventEntryId !== null) {
    return `${input.eventSource}:${input.fixtureId}:${input.eventEntryId}`;
  }

  return `${input.eventSource}:${input.fixtureId}:${input.snapshotTime}:${input.ballKey ?? "no-ball"}`;
}

export function buildOddsEventDedupeKey(input: {
  eventSource: "stream" | "bootstrap";
  fixtureId: string;
  eventEntryId: string | null;
  sourceOddId: string;
  eventType: string;
  eventTime: string;
}): string {
  if (input.eventEntryId !== null) {
    return `${input.eventSource}:${input.fixtureId}:${input.eventEntryId}:${input.sourceOddId}`;
  }

  return `${input.eventSource}:${input.fixtureId}:${input.sourceOddId}:${input.eventType}:${input.eventTime}`;
}

export function buildBallSnapshotKey(input: {
  fixtureId: string;
  ballKey: string;
  sportsbookId: string;
  marketId: string;
  normalizedSelection: string;
}): string {
  return [
    input.fixtureId,
    input.ballKey,
    input.sportsbookId,
    input.marketId,
    input.normalizedSelection,
  ].join(":");
}

export function normalizeResultsEnvelope(
  envelope: OpticOddsResultsEnvelope,
): NormalizedOpticOddsResults | null {
  const fixtureRecord = isRecord(envelope.fixture) ? envelope.fixture : null;
  const fixtureId =
    readString(envelope.fixture_id) ?? readString(fixtureRecord?.["id"]);
  const status =
    readString(envelope.status) ?? readString(fixtureRecord?.["status"]);
  const isLive =
    readBoolean(envelope.is_live) ?? readBoolean(fixtureRecord?.["is_live"]);
  const inPlay = isRecord(envelope.in_play) ? envelope.in_play : null;
  const period = readNullableString(inPlay?.["period"]);
  const periodNumber =
    readFiniteNumber(inPlay?.["period_number"]) ??
    parsePeriodNumber(period ?? readNullableString(inPlay?.["inning"]));
  const ballClock =
    readNullableString(inPlay?.["ball"]) ??
    readNullableString(inPlay?.["clock"]);
  const ballKey =
    ballClock === null
      ? null
      : `${periodNumber ?? period ?? "unknown"}:${ballClock}`;
  const scores = isRecord(envelope.scores) ? envelope.scores : null;
  const homeScore = readNestedFiniteNumber(scores, ["home", "total"]);
  const awayScore = readNestedFiniteNumber(scores, ["away", "total"]);
  const snapshotTime =
    readString(envelope.last_checked_at) ?? new Date().toISOString();

  if (fixtureId === null || status === null || isLive === null) {
    return null;
  }

  return {
    fixtureId,
    status,
    isLive,
    period,
    periodNumber,
    ballClock,
    ballKey,
    homeScore,
    awayScore,
    snapshotTime,
    payload: toJsonObject(envelope),
  };
}

async function refreshFixturesAndBootstrap(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  now: () => Date;
  repository: OpticOddsRepository;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  const fixtures = await input.client.listActiveFixtures({
    sport: input.config.sport,
    leagueId: input.config.leagueId,
    seasonYear: input.config.seasonYear,
  });

  input.summary.fixtureRefreshCount += 1;
  input.summary.lastFixtureRefreshAt = new Date().toISOString();
  input.summary.liveFixtureCount = fixtures.filter(
    (fixture) => fixture.is_live,
  ).length;

  for (const fixture of fixtures) {
    const record = toFixtureInsert(fixture);
    await input.repository.saveFixture(record);
    input.state.fixturesById.set(record.fixtureId, record);
    input.summary.fixturesUpserted += 1;
  }

  const currentTime = input.now();
  const watchedFixtures = selectFixturesToWatch(
    fixtures,
    currentTime,
    input.config,
    input.state.watchedFixtureIds,
  );
  replaceWatchedFixtureIds(
    input.state,
    watchedFixtures.map((fixture) => fixture.id),
  );
  input.summary.watchedFixtureCount = watchedFixtures.length;

  for (const fixture of watchedFixtures) {
    if (!input.state.bootstrappedOddsFixtureIds.has(fixture.id)) {
      await bootstrapFixtureOdds({
        client: input.client,
        config: input.config,
        fixtureId: fixture.id,
        repository: input.repository,
        state: input.state,
        summary: input.summary,
      });
      input.state.bootstrappedOddsFixtureIds.add(fixture.id);
    }

    if (
      (fixture.is_live || fixture.status.toLowerCase() === "live") &&
      !input.state.bootstrappedResultsFixtureIds.has(fixture.id)
    ) {
      await bootstrapFixtureResults({
        client: input.client,
        config: input.config,
        fixtureId: fixture.id,
        now: input.now,
        repository: input.repository,
        state: input.state,
        summary: input.summary,
      });
      input.state.bootstrappedResultsFixtureIds.add(fixture.id);
    }
  }
}

async function bootstrapFixtureOdds(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  fixtureId: string;
  repository: OpticOddsRepository;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  for (const sportsbookIds of chunkSportsbookIds(input.config.sportsbookIds)) {
    const fixtures = await input.client.getFixtureOdds({
      fixtureId: input.fixtureId,
      sportsbookIds,
      marketIds: input.config.marketIds,
      oddsFormat: input.config.oddsFormat,
      excludeFees: input.config.excludeFees,
    });

    for (const fixture of fixtures) {
      const odds = Array.isArray(fixture.odds) ? fixture.odds : [];
      for (const odd of odds) {
        const persisted = await persistOddsUpdate({
          eventEntryId: null,
          eventSource: "bootstrap",
          eventType: "snapshot",
          fixtureId: fixture.id,
          odd,
          repository: input.repository,
          state: input.state,
        });
        if (persisted === true) {
          input.summary.bootstrappedOddsEvents += 1;
          input.summary.ballSnapshotsUpserted +=
            await persistBallSnapshotsForOdd({
              fixtureId: fixture.id,
              repository: input.repository,
              state: input.state,
            });
        }
      }
    }
  }
}

async function bootstrapFixtureResults(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  fixtureId: string;
  now: () => Date;
  repository: OpticOddsRepository;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  const envelopes = await input.client.getFixtureResults(input.fixtureId);
  for (const envelope of envelopes) {
    const persisted = await persistResultsUpdate({
      envelope,
      config: input.config,
      eventEntryId: null,
      eventSource: "bootstrap",
      eventType: "fixture-results",
      now: input.now,
      repository: input.repository,
      state: input.state,
    });
    if (persisted > 0) {
      input.summary.bootstrappedResultsEvents += 1;
      input.summary.ballSnapshotsUpserted += persisted;
    }
  }
}

async function runFixtureRefreshLoop(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  now: () => Date;
  repository: OpticOddsRepository;
  signal?: AbortSignal;
  sleep: (milliseconds: number) => Promise<void>;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  while (input.signal?.aborted !== true) {
    await input.sleep(getIdleSleepMs(input.state, input.now(), input.config));
    if (input.signal?.aborted) {
      return;
    }

    try {
      await refreshFixturesAndBootstrap({
        client: input.client,
        config: input.config,
        now: input.now,
        repository: input.repository,
        state: input.state,
        summary: input.summary,
      });
    } catch {
      continue;
    }
  }
}

async function runLiveResultsPollingLoop(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  now: () => Date;
  repository: OpticOddsRepository;
  signal?: AbortSignal;
  sleep: (milliseconds: number) => Promise<void>;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  while (input.signal?.aborted !== true) {
    const hasLiveFixtures = Array.from(input.state.fixturesById.values()).some(
      (fixture) => fixture.isLive || fixture.status.toLowerCase() === "live",
    );
    await input.sleep(
      hasLiveFixtures
        ? input.config.liveResultsPollIntervalMs
        : getIdleSleepMs(input.state, input.now(), input.config),
    );
    if (input.signal?.aborted) {
      return;
    }

    if (!hasLiveFixtures) {
      continue;
    }

    try {
      await syncLiveResultsSnapshots({
        client: input.client,
        config: input.config,
        now: input.now,
        repository: input.repository,
        state: input.state,
        summary: input.summary,
      });
    } catch {
      continue;
    }
  }
}

async function syncLiveResultsSnapshots(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  now: () => Date;
  repository: OpticOddsRepository;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  const liveFixtures = Array.from(input.state.fixturesById.values()).filter(
    (fixture) => fixture.isLive || fixture.status.toLowerCase() === "live",
  );

  for (const fixture of liveFixtures) {
    await bootstrapFixtureResults({
      client: input.client,
      config: input.config,
      fixtureId: fixture.fixtureId,
      now: input.now,
      repository: input.repository,
      state: input.state,
      summary: input.summary,
    });
  }
}

async function runOddsStreamLoop(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  repository: OpticOddsRepository;
  signal?: AbortSignal;
  now: () => Date;
  sleep: (milliseconds: number) => Promise<void>;
  sportsbookIds: readonly string[];
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  const streamKey = `odds:${input.sportsbookIds.join(",")}`;
  let reconnectDelayMs = input.config.reconnectDelayMs;

  while (input.signal?.aborted !== true) {
    const watchedFixtureIds = [...input.state.watchedFixtureIds];
    if (watchedFixtureIds.length === 0) {
      await input.sleep(getIdleSleepMs(input.state, input.now(), input.config));
      continue;
    }

    const watchVersion = input.state.watchedFixtureVersion;
    const cursor = await input.repository.getStreamCursor(streamKey);
    const managedSignal = createManagedAbortSignal({
      externalSignal: input.signal,
      shouldAbort: () => input.state.watchedFixtureVersion !== watchVersion,
    });
    const url = input.client.buildOddsStreamUrl({
      sportsbookIds: input.sportsbookIds,
      leagueId: input.config.leagueId,
      fixtureIds: watchedFixtureIds,
      marketIds: input.config.marketIds,
      oddsFormat: input.config.oddsFormat,
      excludeFees: input.config.excludeFees,
      includeFixtureUpdates: input.config.includeFixtureUpdates,
      lastEntryId: cursor?.lastEntryId ?? null,
    });

    try {
      logWorkerEvent("info", "stream_connecting", {
        streamKey,
        fixtureCount: watchedFixtureIds.length,
        fixtureIds: watchedFixtureIds,
        lastEntryId: cursor?.lastEntryId ?? null,
      });
      await consumeServerSentEvents({
        inactivityTimeoutMs: input.config.streamInactivityTimeoutMs,
        url,
        signal: managedSignal.signal,
        onEvent: async (event) => {
          if (event.event === "ping" || event.event === "connected") {
            return;
          }

          const envelope = parseStreamEnvelope(event.data);
          const entryId = readString(envelope.entry_id) ?? event.id;

          if (event.event === "fixture-status") {
            const payload = isRecord(envelope.data)
              ? envelope.data
              : isRecord(envelope)
                ? envelope
                : null;
            if (payload !== null) {
              await persistFixtureStatusUpdate({
                config: input.config,
                now: input.now,
                payload,
                repository: input.repository,
                state: input.state,
              });
            }

            if (entryId !== null) {
              await input.repository.saveStreamCursor({
                streamKey,
                lastEntryId: entryId,
                payload: {
                  eventType: event.event,
                  updatedAt: new Date().toISOString(),
                },
              });
              input.summary.cursorUpdates += 1;
            }
            return;
          }

          if (event.event !== "odds" && event.event !== "locked-odds") {
            return;
          }
          const oddPayloads = Array.isArray(envelope.data)
            ? envelope.data.filter(isRecord)
            : [];

          for (const oddPayload of oddPayloads) {
            const persisted = await persistOddsUpdate({
              eventEntryId: entryId,
              eventSource: "stream",
              eventType: event.event,
              fixtureId:
                readString(oddPayload["fixture_id"]) ??
                readString(oddPayload["fixtureId"]) ??
                "",
              odd: oddPayload as unknown as OpticOddsOdd,
              repository: input.repository,
              state: input.state,
            });

            if (persisted === true) {
              input.summary.streamedOddsEvents += 1;
              input.summary.ballSnapshotsUpserted +=
                await persistBallSnapshotsForOdd({
                  fixtureId:
                    readString(oddPayload["fixture_id"]) ??
                    readString(oddPayload["fixtureId"]) ??
                    "",
                  repository: input.repository,
                  state: input.state,
                });
            }
          }

          if (entryId !== null) {
            await input.repository.saveStreamCursor({
              streamKey,
              lastEntryId: entryId,
              payload: {
                eventType: event.event,
                updatedAt: new Date().toISOString(),
              },
            });
            input.summary.cursorUpdates += 1;
          }
        },
      });
      reconnectDelayMs = input.config.reconnectDelayMs;
      managedSignal.dispose();
    } catch (error) {
      managedSignal.dispose();
      if (input.signal?.aborted) {
        return;
      }

      if (managedSignal.signal.aborted) {
        continue;
      }

      input.summary.reconnectCount += 1;
      const delayMs = getNextReconnectDelayMs(reconnectDelayMs, error);
      logWorkerEvent("warn", "stream_reconnecting", {
        streamKey,
        reason: describeError(error),
        delayMs,
      });
      await input.sleep(delayMs);
      reconnectDelayMs = Math.min(delayMs * 2, 60_000);
      continue;
    }
  }
}

async function runResultsStreamLoop(input: {
  client: OpticOddsApiClient;
  config: OpticOddsConfig;
  repository: OpticOddsRepository;
  signal?: AbortSignal;
  now: () => Date;
  sleep: (milliseconds: number) => Promise<void>;
  state: MutableServiceState;
  summary: OpticOddsBallByBallIngestionSummary;
}): Promise<void> {
  const streamKey = `results:${input.config.leagueId}`;
  let reconnectDelayMs = input.config.reconnectDelayMs;

  while (input.signal?.aborted !== true) {
    const watchedFixtureIds = [...input.state.watchedFixtureIds];
    if (watchedFixtureIds.length === 0) {
      await input.sleep(getIdleSleepMs(input.state, input.now(), input.config));
      continue;
    }

    const watchVersion = input.state.watchedFixtureVersion;
    const cursor = await input.repository.getStreamCursor(streamKey);
    const managedSignal = createManagedAbortSignal({
      externalSignal: input.signal,
      shouldAbort: () => input.state.watchedFixtureVersion !== watchVersion,
    });
    const url = input.client.buildResultsStreamUrl({
      leagueId: input.config.leagueId,
      fixtureIds: watchedFixtureIds,
      lastEntryId: cursor?.lastEntryId ?? null,
    });

    try {
      logWorkerEvent("info", "stream_connecting", {
        streamKey,
        fixtureCount: watchedFixtureIds.length,
        fixtureIds: watchedFixtureIds,
        lastEntryId: cursor?.lastEntryId ?? null,
      });
      await consumeServerSentEvents({
        inactivityTimeoutMs: input.config.streamInactivityTimeoutMs,
        url,
        signal: managedSignal.signal,
        onEvent: async (event) => {
          if (event.event === "ping" || event.event === "connected") {
            return;
          }

          const envelope = parseStreamEnvelope(event.data);
          if (event.event !== "fixture-results") {
            return;
          }

          const payload = isRecord(envelope.data)
            ? (envelope.data as unknown as OpticOddsResultsEnvelope)
            : null;
          if (payload === null) {
            return;
          }

          const entryId = readString(envelope.entry_id) ?? event.id;
          const persisted = await persistResultsUpdate({
            envelope: payload,
            config: input.config,
            eventEntryId: entryId,
            eventSource: "stream",
            eventType: event.event,
            now: input.now,
            repository: input.repository,
            state: input.state,
          });
          if (persisted > 0) {
            input.summary.streamedResultsEvents += 1;
            input.summary.ballSnapshotsUpserted += persisted;
          }

          if (entryId !== null) {
            await input.repository.saveStreamCursor({
              streamKey,
              lastEntryId: entryId,
              payload: {
                eventType: event.event,
                updatedAt: new Date().toISOString(),
              },
            });
            input.summary.cursorUpdates += 1;
          }
        },
      });
      reconnectDelayMs = input.config.reconnectDelayMs;
      managedSignal.dispose();
    } catch (error) {
      managedSignal.dispose();
      if (input.signal?.aborted) {
        return;
      }

      if (managedSignal.signal.aborted) {
        continue;
      }

      input.summary.reconnectCount += 1;
      const delayMs = getNextReconnectDelayMs(reconnectDelayMs, error);
      logWorkerEvent("warn", "stream_reconnecting", {
        streamKey,
        reason: describeError(error),
        delayMs,
      });
      await input.sleep(delayMs);
      reconnectDelayMs = Math.min(delayMs * 2, 60_000);
      continue;
    }
  }
}

async function persistResultsUpdate(input: {
  envelope: OpticOddsResultsEnvelope;
  config: OpticOddsConfig;
  eventEntryId: string | null;
  eventSource: "stream" | "bootstrap";
  eventType: string;
  now: () => Date;
  repository: OpticOddsRepository;
  state: MutableServiceState;
}): Promise<number> {
  const normalized = normalizeResultsEnvelope(input.envelope);
  if (normalized === null) {
    return 0;
  }

  const fixture = input.state.fixturesById.get(normalized.fixtureId);
  if (fixture === undefined) {
    return 0;
  }

  const dedupeKey = buildResultsEventDedupeKey({
    eventSource: input.eventSource,
    fixtureId: normalized.fixtureId,
    eventEntryId: input.eventEntryId,
    snapshotTime: normalized.snapshotTime,
    ballKey: normalized.ballKey,
  });
  const eventInsert: RawOpticOddsResultsEventInsert = {
    dedupeKey,
    fixtureId: normalized.fixtureId,
    eventSource: input.eventSource,
    eventType: input.eventType,
    eventEntryId: input.eventEntryId,
    snapshotTime: normalized.snapshotTime,
    status: normalized.status,
    isLive: normalized.isLive,
    period: normalized.period,
    periodNumber: normalized.periodNumber,
    ballClock: normalized.ballClock,
    homeScore: normalized.homeScore,
    awayScore: normalized.awayScore,
    payload: normalized.payload,
  };
  await input.repository.saveResultsEvent(eventInsert);

  const updatedFixture: RawOpticOddsFixtureInsert = {
    ...fixture,
    status: normalized.status,
    isLive: normalized.isLive,
  };
  await input.repository.saveFixture(updatedFixture);
  input.state.fixturesById.set(updatedFixture.fixtureId, updatedFixture);
  refreshWatchedFixtureState(input.state, input.config, input.now());

  const context: OpticOddsBallContext = {
    fixtureId: fixture.fixtureId,
    seasonYear: fixture.seasonYear,
    fixtureStartDate: fixture.startDate,
    fixtureStatus: normalized.status,
    isLive: normalized.isLive,
    period: normalized.period,
    periodNumber: normalized.periodNumber,
    ballClock: normalized.ballClock,
    ballKey: normalized.ballKey,
    snapshotTime: normalized.snapshotTime,
    homeTeamName: fixture.homeTeamName,
    awayTeamName: fixture.awayTeamName,
    homeScore: normalized.homeScore,
    awayScore: normalized.awayScore,
    sourceResultsDedupeKey: dedupeKey,
  };
  input.state.latestBallContextByFixtureId.set(normalized.fixtureId, context);

  return persistBallSnapshotsForFixture({
    fixtureId: normalized.fixtureId,
    repository: input.repository,
    state: input.state,
  });
}

async function persistFixtureStatusUpdate(input: {
  config: OpticOddsConfig;
  now: () => Date;
  payload: Record<string, unknown>;
  repository: OpticOddsRepository;
  state: MutableServiceState;
}): Promise<void> {
  const fixtureRecord = isRecord(input.payload["fixture"])
    ? input.payload["fixture"]
    : null;
  const fixtureId =
    readString(input.payload["fixture_id"]) ??
    readString(fixtureRecord?.["id"]);
  if (fixtureId === null) {
    return;
  }

  const existingFixture = input.state.fixturesById.get(fixtureId);
  if (existingFixture === undefined) {
    return;
  }

  const updatedFixture: RawOpticOddsFixtureInsert = {
    ...existingFixture,
    startDate:
      readString(input.payload["new_start_date"]) ??
      readString(fixtureRecord?.["start_date"]) ??
      existingFixture.startDate,
    status:
      readString(input.payload["new_status"]) ??
      readString(fixtureRecord?.["status"]) ??
      existingFixture.status,
    isLive:
      readBoolean(fixtureRecord?.["is_live"]) ??
      (readString(input.payload["new_status"])?.trim().toLowerCase() === "live"
        ? true
        : existingFixture.isLive),
  };

  await input.repository.saveFixture(updatedFixture);
  input.state.fixturesById.set(updatedFixture.fixtureId, updatedFixture);
  refreshWatchedFixtureState(input.state, input.config, input.now());
  logWorkerEvent("info", "fixture_status_updated", {
    fixtureId,
    startDate: updatedFixture.startDate,
    status: updatedFixture.status,
    isLive: updatedFixture.isLive,
  });
}

async function persistOddsUpdate(input: {
  eventEntryId: string | null;
  eventSource: "stream" | "bootstrap";
  eventType: string;
  fixtureId: string;
  odd: OpticOddsOdd;
  repository: OpticOddsRepository;
  state: MutableServiceState;
}): Promise<boolean> {
  if (input.fixtureId.length === 0) {
    return false;
  }

  const fixture = input.state.fixturesById.get(input.fixtureId);
  if (fixture === undefined) {
    return false;
  }

  const normalizedSelection =
    readString(input.odd.normalized_selection) ??
    createSportsbookSlug(input.odd.selection ?? input.odd.name);
  const sportsbookId =
    readString(input.odd.sportsbook_id) ??
    createSportsbookSlug(input.odd.sportsbook);
  const marketId =
    readString(input.odd.market_id) ?? createSportsbookSlug(input.odd.market);
  const eventTime = toEventTimestamp(input.odd.timestamp);
  const dedupeKey = buildOddsEventDedupeKey({
    eventSource: input.eventSource,
    fixtureId: input.fixtureId,
    eventEntryId: input.eventEntryId,
    sourceOddId: input.odd.id,
    eventType: input.eventType,
    eventTime,
  });
  const insert: RawOpticOddsOddsEventInsert = {
    dedupeKey,
    fixtureId: input.fixtureId,
    eventSource: input.eventSource,
    eventType: input.eventType,
    eventEntryId: input.eventEntryId,
    sourceOddId: input.odd.id,
    sportsbookId,
    sportsbookName: input.odd.sportsbook,
    marketId,
    marketName: input.odd.market,
    selection: input.odd.selection,
    normalizedSelection,
    teamId: input.odd.team_id ?? null,
    playerId: input.odd.player_id ?? null,
    groupingKey: input.odd.grouping_key ?? null,
    isMain: input.odd.is_main ?? false,
    isLive: input.odd.is_live ?? fixture.isLive,
    isLocked: input.eventType === "locked-odds",
    price: typeof input.odd.price === "number" ? input.odd.price : null,
    points: typeof input.odd.points === "number" ? input.odd.points : null,
    eventTime,
    orderBook: normalizeJsonValue(input.odd.order_book ?? null),
    limits: normalizeJsonObject(input.odd.limits ?? null),
    sourceIds: normalizeJsonObject(input.odd.source_ids ?? null),
    payload: toJsonObject(input.odd),
  };
  await input.repository.saveOddsEvent(insert);

  let fixtureCache = input.state.oddsCacheByFixtureId.get(input.fixtureId);
  if (fixtureCache === undefined) {
    fixtureCache = new Map();
    input.state.oddsCacheByFixtureId.set(input.fixtureId, fixtureCache);
  }

  fixtureCache.set(buildCacheKey(insert), {
    sourceOddId: insert.sourceOddId,
    sportsbookId: insert.sportsbookId,
    sportsbookName: insert.sportsbookName,
    marketId: insert.marketId,
    marketName: insert.marketName,
    selection: insert.selection,
    normalizedSelection: insert.normalizedSelection,
    teamId: insert.teamId,
    playerId: insert.playerId,
    groupingKey: insert.groupingKey,
    isMain: insert.isMain,
    isLive: insert.isLive,
    isLocked: insert.isLocked,
    price: insert.price,
    points: insert.points,
    eventTime: insert.eventTime,
    orderBook: insert.orderBook,
    limits: insert.limits,
    sourceIds: insert.sourceIds,
    payload: insert.payload,
    sourceOddsDedupeKey: dedupeKey,
  });

  return true;
}

async function persistBallSnapshotsForOdd(input: {
  fixtureId: string;
  repository: OpticOddsRepository;
  state: MutableServiceState;
}): Promise<number> {
  return persistBallSnapshotsForFixture(input);
}

async function persistBallSnapshotsForFixture(input: {
  fixtureId: string;
  repository: OpticOddsRepository;
  state: MutableServiceState;
}): Promise<number> {
  const fixture = input.state.fixturesById.get(input.fixtureId);
  const context = input.state.latestBallContextByFixtureId.get(input.fixtureId);
  const cache = input.state.oddsCacheByFixtureId.get(input.fixtureId);
  if (
    fixture === undefined ||
    context === undefined ||
    context.ballClock === null ||
    context.ballKey === null ||
    cache === undefined ||
    cache.size === 0
  ) {
    return 0;
  }

  let persistedCount = 0;
  for (const odd of cache.values()) {
    const snapshot: OpticOddsBallOddsSnapshotInsert = {
      snapshotKey: buildBallSnapshotKey({
        fixtureId: fixture.fixtureId,
        ballKey: context.ballKey,
        sportsbookId: odd.sportsbookId,
        marketId: odd.marketId,
        normalizedSelection: odd.normalizedSelection,
      }),
      fixtureId: fixture.fixtureId,
      sourceResultsDedupeKey: context.sourceResultsDedupeKey,
      sourceOddsDedupeKey: odd.sourceOddsDedupeKey,
      seasonYear: fixture.seasonYear,
      fixtureStartDate: fixture.startDate,
      fixtureStatus: context.fixtureStatus,
      isLive: context.isLive,
      period: context.period,
      periodNumber: context.periodNumber,
      ballClock: context.ballClock,
      ballKey: context.ballKey,
      snapshotTime:
        odd.eventTime > context.snapshotTime
          ? odd.eventTime
          : context.snapshotTime,
      homeTeamName: fixture.homeTeamName,
      awayTeamName: fixture.awayTeamName,
      homeScore: context.homeScore,
      awayScore: context.awayScore,
      sportsbookId: odd.sportsbookId,
      sportsbookName: odd.sportsbookName,
      marketId: odd.marketId,
      marketName: odd.marketName,
      selection: odd.selection,
      normalizedSelection: odd.normalizedSelection,
      teamId: odd.teamId,
      playerId: odd.playerId,
      groupingKey: odd.groupingKey,
      isMain: odd.isMain,
      isLocked: odd.isLocked,
      price: odd.price,
      points: odd.points,
      orderBook: odd.orderBook,
      limits: odd.limits,
      sourceIds: odd.sourceIds,
      payload: odd.payload,
    };
    await input.repository.saveBallOddsSnapshot(snapshot);
    persistedCount += 1;
  }

  return persistedCount;
}

function toFixtureInsert(fixture: OpticOddsFixture): RawOpticOddsFixtureInsert {
  const homeTeam = fixture.home_competitors?.[0];
  const awayTeam = fixture.away_competitors?.[0];
  return {
    fixtureId: fixture.id,
    gameId: fixture.game_id ?? null,
    sportId: fixture.sport?.id ?? "cricket",
    leagueId: fixture.league?.id ?? "india_-_ipl",
    seasonYear: parseSeasonYear(fixture.season_year),
    seasonType: fixture.season_type ?? null,
    seasonWeek: fixture.season_week ?? null,
    startDate: fixture.start_date,
    status: fixture.status,
    isLive: fixture.is_live,
    homeTeamName: fixture.home_team_display ?? homeTeam?.name ?? "Unknown Home",
    awayTeamName: fixture.away_team_display ?? awayTeam?.name ?? "Unknown Away",
    homeTeamId: homeTeam?.id ?? null,
    awayTeamId: awayTeam?.id ?? null,
    hasOdds: fixture.has_odds ?? Array.isArray(fixture.odds),
    venueName: fixture.venue_name ?? null,
    venueLocation: fixture.venue_location ?? null,
    payload: toJsonObject(fixture),
  };
}

function shouldWatchFixture(
  fixture: {
    startDate: string;
    status: string;
    isLive: boolean;
  },
  now: Date,
  config: Pick<
    OpticOddsConfig,
    "assumedTossLeadMinutesBeforeStart" | "streamStartLeadMinutesBeforeToss"
  >,
  wasWatched = false,
): boolean {
  if (fixture.isLive) {
    return true;
  }

  if (isTerminalFixtureStatus(fixture.status)) {
    return false;
  }

  if (wasWatched) {
    return true;
  }

  const watchStartTime = getFixtureWatchStartTime(fixture.startDate, config);
  if (watchStartTime === null) {
    return false;
  }

  return now.getTime() >= Date.parse(watchStartTime);
}

function refreshWatchedFixtureState(
  state: MutableServiceState,
  config: Pick<
    OpticOddsConfig,
    "assumedTossLeadMinutesBeforeStart" | "streamStartLeadMinutesBeforeToss"
  >,
  now: Date,
): void {
  const watchedFixtureIdSet = new Set(state.watchedFixtureIds);
  const watchedFixtureIds = Array.from(state.fixturesById.values())
    .filter((fixture) =>
      shouldWatchFixture(
        fixture,
        now,
        config,
        watchedFixtureIdSet.has(fixture.fixtureId),
      ),
    )
    .map((fixture) => fixture.fixtureId);
  replaceWatchedFixtureIds(state, watchedFixtureIds);
}

function getIdleSleepMs(
  state: MutableServiceState,
  now: Date,
  config: Pick<
    OpticOddsConfig,
    | "fixtureRefreshIntervalMs"
    | "assumedTossLeadMinutesBeforeStart"
    | "streamStartLeadMinutesBeforeToss"
  >,
): number {
  if (state.watchedFixtureIds.length > 0) {
    return config.fixtureRefreshIntervalMs;
  }

  let nextWatchStartTime: number | null = null;
  for (const fixture of state.fixturesById.values()) {
    if (isTerminalFixtureStatus(fixture.status) || fixture.isLive) {
      continue;
    }

    const watchStart = getFixtureWatchStartTime(fixture.startDate, config);
    if (watchStart === null) {
      continue;
    }

    const bufferedWatchStartTime =
      Date.parse(watchStart) - WATCH_START_BUFFER_MS;
    if (!Number.isFinite(bufferedWatchStartTime)) {
      continue;
    }

    if (bufferedWatchStartTime <= now.getTime()) {
      return config.fixtureRefreshIntervalMs;
    }

    nextWatchStartTime =
      nextWatchStartTime === null
        ? bufferedWatchStartTime
        : Math.min(nextWatchStartTime, bufferedWatchStartTime);
  }

  if (nextWatchStartTime === null) {
    return MAX_IDLE_FIXTURE_REFRESH_MS;
  }

  return Math.max(
    config.fixtureRefreshIntervalMs,
    Math.min(nextWatchStartTime - now.getTime(), MAX_IDLE_FIXTURE_REFRESH_MS),
  );
}

function replaceWatchedFixtureIds(
  state: MutableServiceState,
  watchedFixtureIds: readonly string[],
): void {
  const previousIds = [...state.watchedFixtureIds];
  const nextIds = [...watchedFixtureIds].sort((left, right) =>
    left.localeCompare(right),
  );

  if (
    nextIds.length === state.watchedFixtureIds.length &&
    nextIds.every(
      (fixtureId, index) => fixtureId === state.watchedFixtureIds[index],
    )
  ) {
    return;
  }

  const nextIdSet = new Set(nextIds);
  for (const fixtureId of state.bootstrappedOddsFixtureIds) {
    if (!nextIdSet.has(fixtureId)) {
      state.bootstrappedOddsFixtureIds.delete(fixtureId);
    }
  }
  for (const fixtureId of state.bootstrappedResultsFixtureIds) {
    if (!nextIdSet.has(fixtureId)) {
      state.bootstrappedResultsFixtureIds.delete(fixtureId);
    }
  }

  state.watchedFixtureIds = nextIds;
  state.watchedFixtureVersion += 1;
  logWorkerEvent("info", "watched_fixtures_changed", {
    previousFixtureIds: previousIds,
    nextFixtureIds: nextIds,
  });
}

function createManagedAbortSignal(input: {
  externalSignal?: AbortSignal;
  shouldAbort: () => boolean;
}): {
  signal: AbortSignal;
  dispose: () => void;
} {
  const controller = new AbortController();
  const abort = () => {
    if (!controller.signal.aborted) {
      controller.abort();
    }
  };
  const externalAbortListener = () => abort();

  if (input.externalSignal?.aborted) {
    abort();
  } else {
    input.externalSignal?.addEventListener("abort", externalAbortListener, {
      once: true,
    });
  }

  const interval = setInterval(() => {
    if (input.shouldAbort()) {
      abort();
    }
  }, 1000);

  return {
    signal: controller.signal,
    dispose: () => {
      clearInterval(interval);
      input.externalSignal?.removeEventListener("abort", externalAbortListener);
    },
  };
}

class HttpStatusError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`OpticOdds SSE stream failed with status ${status}`);
    this.name = "HttpStatusError";
    this.status = status;
  }
}

class StreamInactivityError extends Error {
  readonly inactivityTimeoutMs: number;

  constructor(inactivityTimeoutMs: number) {
    super(
      `OpticOdds SSE stream inactivity timeout after ${inactivityTimeoutMs}ms`,
    );
    this.name = "StreamInactivityError";
    this.inactivityTimeoutMs = inactivityTimeoutMs;
  }
}

function getNextReconnectDelayMs(
  currentDelayMs: number,
  error: unknown,
): number {
  if (error instanceof HttpStatusError && error.status === 429) {
    return Math.max(currentDelayMs, 15_000);
  }

  return currentDelayMs;
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message || error.name : String(error);
}

function logWorkerEvent(
  level: "info" | "warn" | "error",
  event: string,
  payload: Record<string, unknown>,
): void {
  const entry = {
    level,
    event,
    source: "opticodds-worker",
    timestamp: new Date().toISOString(),
    ...payload,
  };

  const serialized = JSON.stringify(entry);
  if (level === "error") {
    console.error(serialized);
    return;
  }
  if (level === "warn") {
    console.warn(serialized);
    return;
  }
  console.info(serialized);
}

async function consumeServerSentEvents(input: {
  inactivityTimeoutMs: number;
  url: string;
  signal?: AbortSignal;
  onEvent: (event: ServerSentEvent) => Promise<void>;
}): Promise<void> {
  const timeoutController = new AbortController();
  let timedOut = false;
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  const resetTimeout = () => {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      timeoutController.abort();
    }, input.inactivityTimeoutMs);
  };
  const externalAbortListener = () => timeoutController.abort();

  if (input.signal?.aborted) {
    timeoutController.abort();
  } else {
    input.signal?.addEventListener("abort", externalAbortListener, {
      once: true,
    });
  }

  resetTimeout();

  const response = await fetch(input.url, {
    signal: timeoutController.signal,
    headers: {
      Accept: "text/event-stream",
      "User-Agent": "cricket-predictor-opticodds/1.0",
    },
  });
  if (!response.ok || response.body === null) {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
    input.signal?.removeEventListener("abort", externalAbortListener);
    throw new HttpStatusError(response.status);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (input.signal?.aborted !== true) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }

      resetTimeout();
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/gu, "\n");

      while (true) {
        const boundary = buffer.indexOf("\n\n");
        if (boundary === -1) {
          break;
        }

        const rawEvent = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const parsed = parseServerSentEvent(rawEvent);
        if (parsed === null) {
          continue;
        }

        await input.onEvent(parsed);
      }
    }
  } catch (error) {
    if (timedOut) {
      throw new StreamInactivityError(input.inactivityTimeoutMs);
    }

    throw error;
  } finally {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
    input.signal?.removeEventListener("abort", externalAbortListener);
  }
}

function parseServerSentEvent(rawEvent: string): ServerSentEvent | null {
  const lines = rawEvent.split("\n");
  let event = "message";
  let id: string | null = null;
  const dataParts: string[] = [];

  for (const line of lines) {
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
      continue;
    }
    if (line.startsWith("id:")) {
      id = line.slice("id:".length).trim();
      continue;
    }
    if (line.startsWith("data:")) {
      dataParts.push(line.slice("data:".length).trim());
    }
  }

  const data = dataParts.join("\n");
  if (event.length === 0 && data.length === 0) {
    return null;
  }

  return { event, id, data };
}

function parseStreamEnvelope(data: string): StreamEnvelope {
  try {
    const parsed = JSON.parse(data) as unknown;
    return isRecord(parsed) ? (parsed as StreamEnvelope) : {};
  } catch {
    return {};
  }
}

function buildCacheKey(input: {
  sportsbookId: string;
  marketId: string;
  normalizedSelection: string;
}): string {
  return `${input.sportsbookId}:${input.marketId}:${input.normalizedSelection}`;
}

function requireOpticOddsApiKey(config: OpticOddsConfig): string {
  if (config.apiKey === null || config.apiKey.trim().length === 0) {
    throw new Error(
      "OPTIC_ODDS_API_KEY is required to run OpticOdds ingestion.",
    );
  }

  return config.apiKey;
}

function parseSeasonYear(value: string | null | undefined): number | null {
  if (value === undefined || value === null || value.trim().length === 0) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) ? parsed : null;
}

function parsePeriodNumber(value: string | null): number | null {
  if (value === null) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) ? parsed : null;
}

function toEventTimestamp(value: number | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return new Date().toISOString();
  }

  return new Date(value * 1000).toISOString();
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readNullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function readFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readNestedFiniteNumber(
  record: Record<string, unknown> | null,
  path: readonly string[],
): number | null {
  let current: unknown = record;
  for (const key of path) {
    if (!isRecord(current)) {
      return null;
    }
    current = current[key];
  }
  return readFiniteNumber(current);
}

function normalizeJsonObject(value: JsonObject | null): JsonObject | null {
  return value === null ? null : toJsonObject(value);
}

function normalizeJsonValue(value: unknown): JsonValue | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeJsonValue(entry));
  }
  if (isRecord(value)) {
    return toJsonObject(value);
  }
  return null;
}

function toJsonObject(value: unknown): JsonObject {
  if (!isRecord(value)) {
    return {};
  }

  const output: JsonObject = {};
  for (const [key, entry] of Object.entries(value)) {
    const normalized = normalizeJsonValue(entry);
    if (normalized !== null) {
      output[key] = normalized;
    }
  }

  return output;
}

async function defaultSleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
