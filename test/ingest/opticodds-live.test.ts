import { describe, expect, it } from "vitest";

import {
  buildBallSnapshotKey,
  buildOddsEventDedupeKey,
  buildResultsEventDedupeKey,
  chunkSportsbookIds,
  getFixtureWatchStartTime,
  isTerminalFixtureStatus,
  normalizeResultsEnvelope,
  selectFixturesToWatch,
} from "../../src/ingest/opticodds/index.js";

describe("opticodds live ingestion helpers", () => {
  it("chunks sportsbook ids into OpticOdds-compatible batches of five", () => {
    expect(
      chunkSportsbookIds([
        "polymarket",
        "bet365",
        "betfair",
        "1xbet",
        "betano",
        "novig",
      ]),
    ).toEqual([
      ["polymarket", "bet365", "betfair", "1xbet", "betano"],
      ["novig"],
    ]);
  });

  it("normalizes cricket results payloads that use in_play.clock", () => {
    const normalized = normalizeResultsEnvelope({
      fixture: {
        id: "20260415B6751F2B",
        start_date: "2026-04-15T14:00:00Z",
        status: "live",
        is_live: true,
      },
      scores: {
        home: { total: 0 },
        away: { total: 16 },
      },
      in_play: {
        period: "1",
        period_number: 1,
        clock: "2.5",
      },
      last_checked_at: "2026-04-15T14:12:59.849Z",
    });

    expect(normalized).not.toBeNull();
    expect(normalized?.fixtureId).toBe("20260415B6751F2B");
    expect(normalized?.ballClock).toBe("2.5");
    expect(normalized?.ballKey).toBe("1:2.5");
    expect(normalized?.awayScore).toBe(16);
  });

  it("builds stable dedupe and snapshot keys", () => {
    expect(
      buildResultsEventDedupeKey({
        eventSource: "stream",
        fixtureId: "fixture-1",
        eventEntryId: "1776262470913-0",
        snapshotTime: "2026-04-15T14:14:30.000Z",
        ballKey: "1:2.5",
      }),
    ).toBe("stream:fixture-1:1776262470913-0");

    expect(
      buildOddsEventDedupeKey({
        eventSource: "bootstrap",
        fixtureId: "fixture-1",
        eventEntryId: null,
        sourceOddId: "odd-1",
        eventType: "snapshot",
        eventTime: "2026-04-15T14:14:25.000Z",
      }),
    ).toBe("bootstrap:fixture-1:odd-1:snapshot:2026-04-15T14:14:25.000Z");

    expect(
      buildBallSnapshotKey({
        fixtureId: "fixture-1",
        ballKey: "1:2.5",
        sportsbookId: "polymarket",
        marketId: "moneyline",
        normalizedSelection: "lucknow_super_giants",
      }),
    ).toBe("fixture-1:1:2.5:polymarket:moneyline:lucknow_super_giants");
  });

  it("calculates the stream watch window from toss and pre-toss lead times", () => {
    expect(
      getFixtureWatchStartTime("2026-04-15T14:00:00Z", {
        assumedTossLeadMinutesBeforeStart: 30,
        streamStartLeadMinutesBeforeToss: 30,
      }),
    ).toBe("2026-04-15T13:00:00.000Z");
  });

  it("selects fixtures from 30 minutes before the assumed toss until completion", () => {
    const selected = selectFixturesToWatch(
      [
        {
          id: "live",
          start_date: "2026-04-15T14:00:00Z",
          status: "live",
          is_live: true,
        },
        {
          id: "soon",
          start_date: "2026-04-15T14:45:00Z",
          status: "unplayed",
          is_live: false,
        },
        {
          id: "innings-break",
          start_date: "2026-04-15T12:00:00Z",
          status: "half",
          is_live: false,
        },
        {
          id: "later",
          start_date: "2026-04-15T17:00:00Z",
          status: "unplayed",
          is_live: false,
        },
        {
          id: "done",
          start_date: "2026-04-15T10:00:00Z",
          status: "completed",
          is_live: false,
        },
      ],
      new Date("2026-04-15T14:00:00Z"),
      {
        assumedTossLeadMinutesBeforeStart: 30,
        streamStartLeadMinutesBeforeToss: 30,
      },
    );

    expect(selected.map((fixture) => fixture.id)).toEqual([
      "live",
      "soon",
      "innings-break",
    ]);
  });

  it("keeps already-watched fixtures active through delays until terminal status", () => {
    const selected = selectFixturesToWatch(
      [
        {
          id: "rain-delay",
          start_date: "2026-04-15T18:00:00Z",
          status: "delayed",
          is_live: false,
        },
      ],
      new Date("2026-04-15T14:05:00Z"),
      {
        assumedTossLeadMinutesBeforeStart: 30,
        streamStartLeadMinutesBeforeToss: 30,
      },
      ["rain-delay"],
    );

    expect(selected.map((fixture) => fixture.id)).toEqual(["rain-delay"]);
  });

  it("identifies terminal fixture statuses correctly", () => {
    expect(isTerminalFixtureStatus("completed")).toBe(true);
    expect(isTerminalFixtureStatus("cancelled")).toBe(true);
    expect(isTerminalFixtureStatus("live")).toBe(false);
    expect(isTerminalFixtureStatus("half")).toBe(false);
  });
});
