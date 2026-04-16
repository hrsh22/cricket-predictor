import type { JsonObject } from "../../domain/primitives.js";

export interface OpticOddsCompetitor {
  id: string;
  numerical_id?: number;
  base_id?: number;
  name: string;
  abbreviation?: string;
  logo?: string;
}

export interface OpticOddsFixture {
  id: string;
  numerical_id?: number;
  game_id?: string | null;
  start_date: string;
  home_competitors?: OpticOddsCompetitor[];
  away_competitors?: OpticOddsCompetitor[];
  home_team_display?: string;
  away_team_display?: string;
  status: string;
  is_live: boolean;
  season_type?: string | null;
  season_year?: string | null;
  season_week?: string | null;
  venue_name?: string | null;
  venue_location?: string | null;
  venue_neutral?: boolean;
  sport?: {
    id: string;
    name?: string;
    numerical_id?: number;
  };
  league?: {
    id: string;
    name?: string;
    numerical_id?: number;
  };
  tournament?: unknown;
  has_odds?: boolean;
  odds?: OpticOddsOdd[];
}

export interface OpticOddsOdd {
  id: string;
  fixture_id: string;
  game_id?: string | null;
  sportsbook: string;
  sportsbook_id?: string;
  market: string;
  market_id?: string;
  name: string;
  selection: string;
  normalized_selection?: string;
  selection_line?: string | null;
  price?: number | null;
  points?: number | null;
  selection_points?: number | null;
  timestamp?: number;
  grouping_key?: string | null;
  is_main?: boolean;
  is_live?: boolean;
  player_id?: string | null;
  team_id?: string | null;
  limits?: JsonObject | null;
  order_book?: unknown[] | null;
  source_ids?: JsonObject | null;
  deep_link?: JsonObject | null;
}

export interface OpticOddsResultsEnvelope {
  fixture?: OpticOddsFixture;
  fixture_id?: string;
  sport?: {
    id: string;
    name?: string;
    numerical_id?: number;
  };
  league?: {
    id: string;
    name?: string;
    numerical_id?: number;
  };
  status?: string;
  is_live?: boolean;
  scores?: unknown;
  in_play?: unknown;
  events?: unknown[];
  stats?: unknown;
  extra?: unknown;
  last_checked_at?: string;
}

export interface OpticOddsGradeResponse {
  fixture_id?: string;
  market?: string;
  name?: string;
  result?: string;
  status?: string;
}

export interface OpticOddsResponseLike {
  ok: boolean;
  status: number;
  headers: {
    get(name: string): string | null;
  };
  json(): Promise<unknown>;
  text(): Promise<string>;
}

export type OpticOddsFetch = (
  input: string,
  init?: RequestInit,
) => Promise<OpticOddsResponseLike>;

export interface OpticOddsHttpClientOptions {
  apiKey: string;
  baseUrl?: string;
  fetchImpl?: OpticOddsFetch;
}

export interface ListActiveFixturesInput {
  sport?: string;
  leagueId?: string;
  seasonYear?: number;
}

export interface GetFixtureOddsInput {
  fixtureId: string;
  sportsbookIds: readonly string[];
  marketIds?: readonly string[];
  oddsFormat?: string;
  excludeFees?: boolean;
}

export interface BuildOddsStreamUrlInput {
  sportsbookIds: readonly string[];
  leagueId?: string;
  fixtureIds?: readonly string[];
  marketIds?: readonly string[];
  oddsFormat?: string;
  excludeFees?: boolean;
  includeFixtureUpdates?: boolean;
  lastEntryId?: string | null;
}

export interface BuildResultsStreamUrlInput {
  leagueId?: string;
  fixtureIds?: readonly string[];
  lastEntryId?: string | null;
}

export interface GradeOddsInput {
  fixtureId: string;
  market: string;
  name: string;
}

export interface OpticOddsApiClient {
  listActiveFixtures(
    input?: ListActiveFixturesInput,
  ): Promise<readonly OpticOddsFixture[]>;
  getFixtureOdds(
    input: GetFixtureOddsInput,
  ): Promise<readonly OpticOddsFixture[]>;
  getFixtureResults(
    fixtureId: string,
  ): Promise<readonly OpticOddsResultsEnvelope[]>;
  gradeOdds(input: GradeOddsInput): Promise<OpticOddsGradeResponse | null>;
  buildOddsStreamUrl(input: BuildOddsStreamUrlInput): string;
  buildResultsStreamUrl(input: BuildResultsStreamUrlInput): string;
}

const DEFAULT_BASE_URL = "https://api.opticodds.com/api/v3";

export function createOpticOddsApiClient(
  options: OpticOddsHttpClientOptions,
): OpticOddsApiClient {
  const fetchImpl =
    options.fetchImpl ??
    ((input: string, init?: RequestInit) =>
      fetch(input, init) as Promise<OpticOddsResponseLike>);
  const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;

  return {
    async listActiveFixtures(
      input: ListActiveFixturesInput = {},
    ): Promise<readonly OpticOddsFixture[]> {
      const payload = await fetchJson(
        fetchImpl,
        buildUrl(baseUrl, "/fixtures/active", {
          sport: input.sport ?? "cricket",
          league: input.leagueId,
          season_year: input.seasonYear,
        }),
        options.apiKey,
      );

      return toArray<OpticOddsFixture>(payload);
    },

    async getFixtureOdds(
      input: GetFixtureOddsInput,
    ): Promise<readonly OpticOddsFixture[]> {
      const payload = await fetchJson(
        fetchImpl,
        buildUrl(baseUrl, "/fixtures/odds", {
          fixture_id: input.fixtureId,
          sportsbook: input.sportsbookIds,
          market: input.marketIds,
          odds_format: input.oddsFormat ?? "DECIMAL",
          exclude_fees: input.excludeFees === true ? "true" : undefined,
        }),
        options.apiKey,
      );

      return toArray<OpticOddsFixture>(payload);
    },

    async getFixtureResults(
      fixtureId: string,
    ): Promise<readonly OpticOddsResultsEnvelope[]> {
      const payload = await fetchJson(
        fetchImpl,
        buildUrl(baseUrl, "/fixtures/results", {
          fixture_id: fixtureId,
        }),
        options.apiKey,
      );

      return toArray<OpticOddsResultsEnvelope>(payload);
    },

    async gradeOdds(
      input: GradeOddsInput,
    ): Promise<OpticOddsGradeResponse | null> {
      const payload = await fetchJson(
        fetchImpl,
        buildUrl(baseUrl, "/grader/odds", {
          fixture_id: input.fixtureId,
          market: input.market,
          name: input.name,
        }),
        options.apiKey,
      );
      const data = toArray<OpticOddsGradeResponse>(payload)[0];
      return data ?? null;
    },

    buildOddsStreamUrl(input: BuildOddsStreamUrlInput): string {
      return buildUrl(baseUrl, "/stream/odds/cricket", {
        key: options.apiKey,
        sportsbook: input.sportsbookIds,
        league: input.leagueId,
        fixture_id: input.fixtureIds,
        market: input.marketIds,
        odds_format: input.oddsFormat ?? "DECIMAL",
        exclude_fees: input.excludeFees === true ? "true" : undefined,
        include_fixture_updates:
          input.includeFixtureUpdates === true ? "true" : undefined,
        last_entry_id: input.lastEntryId,
      });
    },

    buildResultsStreamUrl(input: BuildResultsStreamUrlInput): string {
      return buildUrl(baseUrl, "/stream/results/cricket", {
        key: options.apiKey,
        league: input.leagueId,
        fixture_id: input.fixtureIds,
        last_entry_id: input.lastEntryId,
      });
    },
  };
}

function buildUrl(
  baseUrl: string,
  pathname: string,
  params: Record<
    string,
    string | number | readonly string[] | null | undefined
  >,
): string {
  const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const normalizedPath = pathname.replace(/^\//u, "");
  const url = new URL(normalizedPath, normalizedBaseUrl);

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) {
      continue;
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        continue;
      }

      for (const entry of value) {
        url.searchParams.append(key, entry);
      }
      continue;
    }

    url.searchParams.set(key, String(value));
  }

  return url.toString();
}

async function fetchJson(
  fetchImpl: OpticOddsFetch,
  url: string,
  apiKey: string,
): Promise<unknown> {
  const response = await fetchImpl(url, {
    headers: {
      Accept: "application/json",
      "X-Api-Key": apiKey,
      "User-Agent": "cricket-predictor-opticodds/1.0",
    },
  });

  if (response.ok) {
    return response.json();
  }

  throw new Error(
    `OpticOdds request failed with status ${response.status}: ${await response.text()}`,
  );
}

function toArray<T>(payload: unknown): readonly T[] {
  if (
    typeof payload === "object" &&
    payload !== null &&
    Array.isArray((payload as { data?: unknown }).data)
  ) {
    return (payload as { data: T[] }).data;
  }

  return [];
}
