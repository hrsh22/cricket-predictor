import type { JsonObject, JsonValue } from "../domain/primitives.js";
import type { SqlExecutor } from "./postgres.js";

export interface RawOpticOddsFixtureInsert {
  fixtureId: string;
  gameId: string | null;
  sportId: string;
  leagueId: string;
  seasonYear: number | null;
  seasonType: string | null;
  seasonWeek: string | null;
  startDate: string;
  status: string;
  isLive: boolean;
  homeTeamName: string;
  awayTeamName: string;
  homeTeamId: string | null;
  awayTeamId: string | null;
  hasOdds: boolean;
  venueName: string | null;
  venueLocation: string | null;
  payload: JsonObject;
}

export interface RawOpticOddsResultsEventInsert {
  dedupeKey: string;
  fixtureId: string;
  eventSource: "stream" | "bootstrap";
  eventType: string;
  eventEntryId: string | null;
  snapshotTime: string;
  status: string;
  isLive: boolean;
  period: string | null;
  periodNumber: number | null;
  ballClock: string | null;
  homeScore: number | null;
  awayScore: number | null;
  payload: JsonObject;
}

export interface RawOpticOddsOddsEventInsert {
  dedupeKey: string;
  fixtureId: string;
  eventSource: "stream" | "bootstrap";
  eventType: string;
  eventEntryId: string | null;
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
}

export interface OpticOddsBallOddsSnapshotInsert {
  snapshotKey: string;
  fixtureId: string;
  sourceResultsDedupeKey: string | null;
  sourceOddsDedupeKey: string | null;
  seasonYear: number | null;
  fixtureStartDate: string;
  fixtureStatus: string;
  isLive: boolean;
  period: string | null;
  periodNumber: number | null;
  ballClock: string;
  ballKey: string;
  snapshotTime: string;
  homeTeamName: string;
  awayTeamName: string;
  homeScore: number | null;
  awayScore: number | null;
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
  isLocked: boolean;
  price: number | null;
  points: number | null;
  orderBook: JsonValue | null;
  limits: JsonObject | null;
  sourceIds: JsonObject | null;
  payload: JsonObject;
}

export interface OpticOddsStreamCursorInsert {
  streamKey: string;
  lastEntryId: string | null;
  payload: JsonObject;
}

export interface RawOpticOddsFixtureRecord extends RawOpticOddsFixtureInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
  updatedAt: string;
}

export interface RawOpticOddsResultsEventRecord extends RawOpticOddsResultsEventInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
}

export interface RawOpticOddsOddsEventRecord extends RawOpticOddsOddsEventInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
}

export interface OpticOddsBallOddsSnapshotRecord extends OpticOddsBallOddsSnapshotInsert {
  id: number;
  competition: "IPL";
  createdAt: string;
  updatedAt: string;
}

export interface OpticOddsStreamCursorRecord extends OpticOddsStreamCursorInsert {
  updatedAt: string;
}

export interface OpticOddsRepository {
  saveFixture(
    fixture: RawOpticOddsFixtureInsert,
  ): Promise<RawOpticOddsFixtureRecord>;
  saveResultsEvent(
    event: RawOpticOddsResultsEventInsert,
  ): Promise<RawOpticOddsResultsEventRecord>;
  saveOddsEvent(
    event: RawOpticOddsOddsEventInsert,
  ): Promise<RawOpticOddsOddsEventRecord>;
  saveBallOddsSnapshot(
    snapshot: OpticOddsBallOddsSnapshotInsert,
  ): Promise<OpticOddsBallOddsSnapshotRecord>;
  saveStreamCursor(
    cursor: OpticOddsStreamCursorInsert,
  ): Promise<OpticOddsStreamCursorRecord>;
  getStreamCursor(
    streamKey: string,
  ): Promise<OpticOddsStreamCursorRecord | null>;
}

export function createOpticOddsRepository(
  executor: SqlExecutor,
): OpticOddsRepository {
  return {
    async saveFixture(
      fixture: RawOpticOddsFixtureInsert,
    ): Promise<RawOpticOddsFixtureRecord> {
      const result = await executor.query<RawOpticOddsFixtureRow>(
        `
          insert into raw_opticodds_fixtures (
            competition,
            fixture_id,
            game_id,
            sport_id,
            league_id,
            season_year,
            season_type,
            season_week,
            start_date,
            status,
            is_live,
            home_team_name,
            away_team_name,
            home_team_id,
            away_team_id,
            has_odds,
            venue_name,
            venue_location,
            payload
          ) values (
            'IPL', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
          )
          on conflict (fixture_id) do update set
            game_id = excluded.game_id,
            sport_id = excluded.sport_id,
            league_id = excluded.league_id,
            season_year = excluded.season_year,
            season_type = excluded.season_type,
            season_week = excluded.season_week,
            start_date = excluded.start_date,
            status = excluded.status,
            is_live = excluded.is_live,
            home_team_name = excluded.home_team_name,
            away_team_name = excluded.away_team_name,
            home_team_id = excluded.home_team_id,
            away_team_id = excluded.away_team_id,
            has_odds = excluded.has_odds,
            venue_name = excluded.venue_name,
            venue_location = excluded.venue_location,
            payload = excluded.payload,
            updated_at = now()
          returning
            id,
            competition,
            fixture_id,
            game_id,
            sport_id,
            league_id,
            season_year,
            season_type,
            season_week,
            start_date,
            status,
            is_live,
            home_team_name,
            away_team_name,
            home_team_id,
            away_team_id,
            has_odds,
            venue_name,
            venue_location,
            payload,
            created_at,
            updated_at
        `,
        [
          fixture.fixtureId,
          fixture.gameId,
          fixture.sportId,
          fixture.leagueId,
          fixture.seasonYear,
          fixture.seasonType,
          fixture.seasonWeek,
          fixture.startDate,
          fixture.status,
          fixture.isLive,
          fixture.homeTeamName,
          fixture.awayTeamName,
          fixture.homeTeamId,
          fixture.awayTeamId,
          fixture.hasOdds,
          fixture.venueName,
          fixture.venueLocation,
          toPgJson(fixture.payload),
        ],
      );

      return mapFixtureRow(result.rows[0] as RawOpticOddsFixtureRow);
    },

    async saveResultsEvent(
      event: RawOpticOddsResultsEventInsert,
    ): Promise<RawOpticOddsResultsEventRecord> {
      const result = await executor.query<RawOpticOddsResultsEventRow>(
        `
          insert into raw_opticodds_results_events (
            competition,
            dedupe_key,
            fixture_id,
            event_source,
            event_type,
            event_entry_id,
            snapshot_time,
            status,
            is_live,
            period,
            period_number,
            ball_clock,
            home_score,
            away_score,
            payload
          ) values (
            'IPL', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
          )
          on conflict (dedupe_key) do update set
            fixture_id = excluded.fixture_id,
            event_source = excluded.event_source,
            event_type = excluded.event_type,
            event_entry_id = excluded.event_entry_id,
            snapshot_time = excluded.snapshot_time,
            status = excluded.status,
            is_live = excluded.is_live,
            period = excluded.period,
            period_number = excluded.period_number,
            ball_clock = excluded.ball_clock,
            home_score = excluded.home_score,
            away_score = excluded.away_score,
            payload = excluded.payload
          returning
            id,
            competition,
            dedupe_key,
            fixture_id,
            event_source,
            event_type,
            event_entry_id,
            snapshot_time,
            status,
            is_live,
            period,
            period_number,
            ball_clock,
            home_score,
            away_score,
            payload,
            created_at
        `,
        [
          event.dedupeKey,
          event.fixtureId,
          event.eventSource,
          event.eventType,
          event.eventEntryId,
          event.snapshotTime,
          event.status,
          event.isLive,
          event.period,
          event.periodNumber,
          event.ballClock,
          event.homeScore,
          event.awayScore,
          toPgJson(event.payload),
        ],
      );

      return mapResultsEventRow(result.rows[0] as RawOpticOddsResultsEventRow);
    },

    async saveOddsEvent(
      event: RawOpticOddsOddsEventInsert,
    ): Promise<RawOpticOddsOddsEventRecord> {
      const result = await executor.query<RawOpticOddsOddsEventRow>(
        `
          insert into raw_opticodds_odds_events (
            competition,
            dedupe_key,
            fixture_id,
            event_source,
            event_type,
            event_entry_id,
            source_odd_id,
            sportsbook_id,
            sportsbook_name,
            market_id,
            market_name,
            selection,
            normalized_selection,
            team_id,
            player_id,
            grouping_key,
            is_main,
            is_live,
            is_locked,
            price,
            points,
            event_time,
            order_book,
            limits,
            source_ids,
            payload
          ) values (
            'IPL', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
          )
          on conflict (dedupe_key) do update set
            fixture_id = excluded.fixture_id,
            event_source = excluded.event_source,
            event_type = excluded.event_type,
            event_entry_id = excluded.event_entry_id,
            source_odd_id = excluded.source_odd_id,
            sportsbook_id = excluded.sportsbook_id,
            sportsbook_name = excluded.sportsbook_name,
            market_id = excluded.market_id,
            market_name = excluded.market_name,
            selection = excluded.selection,
            normalized_selection = excluded.normalized_selection,
            team_id = excluded.team_id,
            player_id = excluded.player_id,
            grouping_key = excluded.grouping_key,
            is_main = excluded.is_main,
            is_live = excluded.is_live,
            is_locked = excluded.is_locked,
            price = excluded.price,
            points = excluded.points,
            event_time = excluded.event_time,
            order_book = excluded.order_book,
            limits = excluded.limits,
            source_ids = excluded.source_ids,
            payload = excluded.payload
          returning
            id,
            competition,
            dedupe_key,
            fixture_id,
            event_source,
            event_type,
            event_entry_id,
            source_odd_id,
            sportsbook_id,
            sportsbook_name,
            market_id,
            market_name,
            selection,
            normalized_selection,
            team_id,
            player_id,
            grouping_key,
            is_main,
            is_live,
            is_locked,
            price,
            points,
            event_time,
            order_book,
            limits,
            source_ids,
            payload,
            created_at
        `,
        [
          event.dedupeKey,
          event.fixtureId,
          event.eventSource,
          event.eventType,
          event.eventEntryId,
          event.sourceOddId,
          event.sportsbookId,
          event.sportsbookName,
          event.marketId,
          event.marketName,
          event.selection,
          event.normalizedSelection,
          event.teamId,
          event.playerId,
          event.groupingKey,
          event.isMain,
          event.isLive,
          event.isLocked,
          event.price,
          event.points,
          event.eventTime,
          toPgJson(event.orderBook),
          toPgJson(event.limits),
          toPgJson(event.sourceIds),
          toPgJson(event.payload),
        ],
      );

      return mapOddsEventRow(result.rows[0] as RawOpticOddsOddsEventRow);
    },

    async saveBallOddsSnapshot(
      snapshot: OpticOddsBallOddsSnapshotInsert,
    ): Promise<OpticOddsBallOddsSnapshotRecord> {
      const result = await executor.query<OpticOddsBallOddsSnapshotRow>(
        `
          insert into opticodds_ball_odds_snapshots (
            competition,
            snapshot_key,
            fixture_id,
            source_results_dedupe_key,
            source_odds_dedupe_key,
            season_year,
            fixture_start_date,
            fixture_status,
            is_live,
            period,
            period_number,
            ball_clock,
            ball_key,
            snapshot_time,
            home_team_name,
            away_team_name,
            home_score,
            away_score,
            sportsbook_id,
            sportsbook_name,
            market_id,
            market_name,
            selection,
            normalized_selection,
            team_id,
            player_id,
            grouping_key,
            is_main,
            is_locked,
            price,
            points,
            order_book,
            limits,
            source_ids,
            payload
          ) values (
            'IPL', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34
          )
          on conflict (snapshot_key) do update set
            source_results_dedupe_key = excluded.source_results_dedupe_key,
            source_odds_dedupe_key = excluded.source_odds_dedupe_key,
            season_year = excluded.season_year,
            fixture_start_date = excluded.fixture_start_date,
            fixture_status = excluded.fixture_status,
            is_live = excluded.is_live,
            period = excluded.period,
            period_number = excluded.period_number,
            ball_clock = excluded.ball_clock,
            ball_key = excluded.ball_key,
            snapshot_time = excluded.snapshot_time,
            home_team_name = excluded.home_team_name,
            away_team_name = excluded.away_team_name,
            home_score = excluded.home_score,
            away_score = excluded.away_score,
            sportsbook_id = excluded.sportsbook_id,
            sportsbook_name = excluded.sportsbook_name,
            market_id = excluded.market_id,
            market_name = excluded.market_name,
            selection = excluded.selection,
            normalized_selection = excluded.normalized_selection,
            team_id = excluded.team_id,
            player_id = excluded.player_id,
            grouping_key = excluded.grouping_key,
            is_main = excluded.is_main,
            is_locked = excluded.is_locked,
            price = excluded.price,
            points = excluded.points,
            order_book = excluded.order_book,
            limits = excluded.limits,
            source_ids = excluded.source_ids,
            payload = excluded.payload,
            updated_at = now()
          returning
            id,
            competition,
            snapshot_key,
            fixture_id,
            source_results_dedupe_key,
            source_odds_dedupe_key,
            season_year,
            fixture_start_date,
            fixture_status,
            is_live,
            period,
            period_number,
            ball_clock,
            ball_key,
            snapshot_time,
            home_team_name,
            away_team_name,
            home_score,
            away_score,
            sportsbook_id,
            sportsbook_name,
            market_id,
            market_name,
            selection,
            normalized_selection,
            team_id,
            player_id,
            grouping_key,
            is_main,
            is_locked,
            price,
            points,
            order_book,
            limits,
            source_ids,
            payload,
            created_at,
            updated_at
        `,
        [
          snapshot.snapshotKey,
          snapshot.fixtureId,
          snapshot.sourceResultsDedupeKey,
          snapshot.sourceOddsDedupeKey,
          snapshot.seasonYear,
          snapshot.fixtureStartDate,
          snapshot.fixtureStatus,
          snapshot.isLive,
          snapshot.period,
          snapshot.periodNumber,
          snapshot.ballClock,
          snapshot.ballKey,
          snapshot.snapshotTime,
          snapshot.homeTeamName,
          snapshot.awayTeamName,
          snapshot.homeScore,
          snapshot.awayScore,
          snapshot.sportsbookId,
          snapshot.sportsbookName,
          snapshot.marketId,
          snapshot.marketName,
          snapshot.selection,
          snapshot.normalizedSelection,
          snapshot.teamId,
          snapshot.playerId,
          snapshot.groupingKey,
          snapshot.isMain,
          snapshot.isLocked,
          snapshot.price,
          snapshot.points,
          toPgJson(snapshot.orderBook),
          toPgJson(snapshot.limits),
          toPgJson(snapshot.sourceIds),
          toPgJson(snapshot.payload),
        ],
      );

      return mapBallOddsSnapshotRow(
        result.rows[0] as OpticOddsBallOddsSnapshotRow,
      );
    },

    async saveStreamCursor(
      cursor: OpticOddsStreamCursorInsert,
    ): Promise<OpticOddsStreamCursorRecord> {
      const result = await executor.query<OpticOddsStreamCursorRow>(
        `
          insert into opticodds_stream_cursors (
            stream_key,
            last_entry_id,
            payload
          ) values ($1, $2, $3)
          on conflict (stream_key) do update set
            last_entry_id = excluded.last_entry_id,
            payload = excluded.payload,
            updated_at = now()
          returning stream_key, last_entry_id, payload, updated_at
        `,
        [cursor.streamKey, cursor.lastEntryId, toPgJson(cursor.payload)],
      );

      return mapStreamCursorRow(result.rows[0] as OpticOddsStreamCursorRow);
    },

    async getStreamCursor(
      streamKey: string,
    ): Promise<OpticOddsStreamCursorRecord | null> {
      const result = await executor.query<OpticOddsStreamCursorRow>(
        `
          select stream_key, last_entry_id, payload, updated_at
          from opticodds_stream_cursors
          where stream_key = $1
        `,
        [streamKey],
      );

      const row = result.rows[0];
      return row === undefined ? null : mapStreamCursorRow(row);
    },
  };
}

function toPgJson(value: JsonValue | JsonObject | null): string | null {
  return value === null ? null : JSON.stringify(value);
}

interface RawOpticOddsFixtureRow {
  id: string | number;
  competition: "IPL";
  fixture_id: string;
  game_id: string | null;
  sport_id: string;
  league_id: string;
  season_year: number | null;
  season_type: string | null;
  season_week: string | null;
  start_date: Date;
  status: string;
  is_live: boolean;
  home_team_name: string;
  away_team_name: string;
  home_team_id: string | null;
  away_team_id: string | null;
  has_odds: boolean;
  venue_name: string | null;
  venue_location: string | null;
  payload: JsonObject;
  created_at: Date;
  updated_at: Date;
}

interface RawOpticOddsResultsEventRow {
  id: string | number;
  competition: "IPL";
  dedupe_key: string;
  fixture_id: string;
  event_source: "stream" | "bootstrap";
  event_type: string;
  event_entry_id: string | null;
  snapshot_time: Date;
  status: string;
  is_live: boolean;
  period: string | null;
  period_number: number | null;
  ball_clock: string | null;
  home_score: number | null;
  away_score: number | null;
  payload: JsonObject;
  created_at: Date;
}

interface RawOpticOddsOddsEventRow {
  id: string | number;
  competition: "IPL";
  dedupe_key: string;
  fixture_id: string;
  event_source: "stream" | "bootstrap";
  event_type: string;
  event_entry_id: string | null;
  source_odd_id: string;
  sportsbook_id: string;
  sportsbook_name: string;
  market_id: string;
  market_name: string;
  selection: string;
  normalized_selection: string;
  team_id: string | null;
  player_id: string | null;
  grouping_key: string | null;
  is_main: boolean;
  is_live: boolean;
  is_locked: boolean;
  price: string | number | null;
  points: string | number | null;
  event_time: Date;
  order_book: JsonValue | null;
  limits: JsonObject | null;
  source_ids: JsonObject | null;
  payload: JsonObject;
  created_at: Date;
}

interface OpticOddsBallOddsSnapshotRow {
  id: string | number;
  competition: "IPL";
  snapshot_key: string;
  fixture_id: string;
  source_results_dedupe_key: string | null;
  source_odds_dedupe_key: string | null;
  season_year: number | null;
  fixture_start_date: Date;
  fixture_status: string;
  is_live: boolean;
  period: string | null;
  period_number: number | null;
  ball_clock: string;
  ball_key: string;
  snapshot_time: Date;
  home_team_name: string;
  away_team_name: string;
  home_score: number | null;
  away_score: number | null;
  sportsbook_id: string;
  sportsbook_name: string;
  market_id: string;
  market_name: string;
  selection: string;
  normalized_selection: string;
  team_id: string | null;
  player_id: string | null;
  grouping_key: string | null;
  is_main: boolean;
  is_locked: boolean;
  price: string | number | null;
  points: string | number | null;
  order_book: JsonValue | null;
  limits: JsonObject | null;
  source_ids: JsonObject | null;
  payload: JsonObject;
  created_at: Date;
  updated_at: Date;
}

interface OpticOddsStreamCursorRow {
  stream_key: string;
  last_entry_id: string | null;
  payload: JsonObject;
  updated_at: Date;
}

function mapFixtureRow(row: RawOpticOddsFixtureRow): RawOpticOddsFixtureRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    fixtureId: row.fixture_id,
    gameId: row.game_id,
    sportId: row.sport_id,
    leagueId: row.league_id,
    seasonYear: row.season_year,
    seasonType: row.season_type,
    seasonWeek: row.season_week,
    startDate: row.start_date.toISOString(),
    status: row.status,
    isLive: row.is_live,
    homeTeamName: row.home_team_name,
    awayTeamName: row.away_team_name,
    homeTeamId: row.home_team_id,
    awayTeamId: row.away_team_id,
    hasOdds: row.has_odds,
    venueName: row.venue_name,
    venueLocation: row.venue_location,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

function mapResultsEventRow(
  row: RawOpticOddsResultsEventRow,
): RawOpticOddsResultsEventRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    dedupeKey: row.dedupe_key,
    fixtureId: row.fixture_id,
    eventSource: row.event_source,
    eventType: row.event_type,
    eventEntryId: row.event_entry_id,
    snapshotTime: row.snapshot_time.toISOString(),
    status: row.status,
    isLive: row.is_live,
    period: row.period,
    periodNumber: row.period_number,
    ballClock: row.ball_clock,
    homeScore: row.home_score,
    awayScore: row.away_score,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}

function mapOddsEventRow(
  row: RawOpticOddsOddsEventRow,
): RawOpticOddsOddsEventRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    dedupeKey: row.dedupe_key,
    fixtureId: row.fixture_id,
    eventSource: row.event_source,
    eventType: row.event_type,
    eventEntryId: row.event_entry_id,
    sourceOddId: row.source_odd_id,
    sportsbookId: row.sportsbook_id,
    sportsbookName: row.sportsbook_name,
    marketId: row.market_id,
    marketName: row.market_name,
    selection: row.selection,
    normalizedSelection: row.normalized_selection,
    teamId: row.team_id,
    playerId: row.player_id,
    groupingKey: row.grouping_key,
    isMain: row.is_main,
    isLive: row.is_live,
    isLocked: row.is_locked,
    price: row.price === null ? null : Number(row.price),
    points: row.points === null ? null : Number(row.points),
    eventTime: row.event_time.toISOString(),
    orderBook: row.order_book,
    limits: row.limits,
    sourceIds: row.source_ids,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
  };
}

function mapBallOddsSnapshotRow(
  row: OpticOddsBallOddsSnapshotRow,
): OpticOddsBallOddsSnapshotRecord {
  return {
    id: Number(row.id),
    competition: row.competition,
    snapshotKey: row.snapshot_key,
    fixtureId: row.fixture_id,
    sourceResultsDedupeKey: row.source_results_dedupe_key,
    sourceOddsDedupeKey: row.source_odds_dedupe_key,
    seasonYear: row.season_year,
    fixtureStartDate: row.fixture_start_date.toISOString(),
    fixtureStatus: row.fixture_status,
    isLive: row.is_live,
    period: row.period,
    periodNumber: row.period_number,
    ballClock: row.ball_clock,
    ballKey: row.ball_key,
    snapshotTime: row.snapshot_time.toISOString(),
    homeTeamName: row.home_team_name,
    awayTeamName: row.away_team_name,
    homeScore: row.home_score,
    awayScore: row.away_score,
    sportsbookId: row.sportsbook_id,
    sportsbookName: row.sportsbook_name,
    marketId: row.market_id,
    marketName: row.market_name,
    selection: row.selection,
    normalizedSelection: row.normalized_selection,
    teamId: row.team_id,
    playerId: row.player_id,
    groupingKey: row.grouping_key,
    isMain: row.is_main,
    isLocked: row.is_locked,
    price: row.price === null ? null : Number(row.price),
    points: row.points === null ? null : Number(row.points),
    orderBook: row.order_book,
    limits: row.limits,
    sourceIds: row.source_ids,
    payload: row.payload,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

function mapStreamCursorRow(
  row: OpticOddsStreamCursorRow,
): OpticOddsStreamCursorRecord {
  return {
    streamKey: row.stream_key,
    lastEntryId: row.last_entry_id,
    payload: row.payload,
    updatedAt: row.updated_at.toISOString(),
  };
}
