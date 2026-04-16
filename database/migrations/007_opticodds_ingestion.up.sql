create table raw_opticodds_fixtures (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  fixture_id text not null unique,
  game_id text,
  sport_id text not null default 'cricket',
  league_id text not null default 'india_-_ipl',
  season_year integer check (season_year is null or season_year >= 2008),
  season_type text,
  season_week text,
  start_date timestamptz not null,
  status text not null,
  is_live boolean not null default false,
  home_team_name text not null,
  away_team_name text not null,
  home_team_id text,
  away_team_id text,
  has_odds boolean not null default false,
  venue_name text,
  venue_location text,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index raw_opticodds_fixtures_schedule_idx
  on raw_opticodds_fixtures (start_date, status);

create index raw_opticodds_fixtures_season_idx
  on raw_opticodds_fixtures (season_year, is_live);

create table raw_opticodds_results_events (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  dedupe_key text not null unique,
  fixture_id text not null references raw_opticodds_fixtures(fixture_id) on delete cascade,
  event_source text not null check (event_source in ('stream', 'bootstrap')),
  event_type text not null,
  event_entry_id text,
  snapshot_time timestamptz not null,
  status text not null,
  is_live boolean not null default false,
  period text,
  period_number integer,
  ball_clock text,
  home_score integer,
  away_score integer,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index raw_opticodds_results_events_fixture_idx
  on raw_opticodds_results_events (fixture_id, snapshot_time desc);

create index raw_opticodds_results_events_ball_idx
  on raw_opticodds_results_events (fixture_id, period_number, ball_clock, snapshot_time desc);

create table raw_opticodds_odds_events (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  dedupe_key text not null unique,
  fixture_id text not null references raw_opticodds_fixtures(fixture_id) on delete cascade,
  event_source text not null check (event_source in ('stream', 'bootstrap')),
  event_type text not null,
  event_entry_id text,
  source_odd_id text not null,
  sportsbook_id text not null,
  sportsbook_name text not null,
  market_id text not null,
  market_name text not null,
  selection text not null,
  normalized_selection text not null,
  team_id text,
  player_id text,
  grouping_key text,
  is_main boolean not null default false,
  is_live boolean not null default false,
  is_locked boolean not null default false,
  price numeric(14, 6),
  points numeric(14, 6),
  event_time timestamptz not null,
  order_book jsonb,
  limits jsonb,
  source_ids jsonb,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index raw_opticodds_odds_events_fixture_idx
  on raw_opticodds_odds_events (fixture_id, event_time desc);

create index raw_opticodds_odds_events_selection_idx
  on raw_opticodds_odds_events (fixture_id, sportsbook_id, market_id, normalized_selection, event_time desc);

create table opticodds_ball_odds_snapshots (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  snapshot_key text not null unique,
  fixture_id text not null references raw_opticodds_fixtures(fixture_id) on delete cascade,
  source_results_dedupe_key text references raw_opticodds_results_events(dedupe_key) on delete set null,
  source_odds_dedupe_key text references raw_opticodds_odds_events(dedupe_key) on delete set null,
  season_year integer check (season_year is null or season_year >= 2008),
  fixture_start_date timestamptz not null,
  fixture_status text not null,
  is_live boolean not null default false,
  period text,
  period_number integer,
  ball_clock text not null,
  ball_key text not null,
  snapshot_time timestamptz not null,
  home_team_name text not null,
  away_team_name text not null,
  home_score integer,
  away_score integer,
  sportsbook_id text not null,
  sportsbook_name text not null,
  market_id text not null,
  market_name text not null,
  selection text not null,
  normalized_selection text not null,
  team_id text,
  player_id text,
  grouping_key text,
  is_main boolean not null default false,
  is_locked boolean not null default false,
  price numeric(14, 6),
  points numeric(14, 6),
  order_book jsonb,
  limits jsonb,
  source_ids jsonb,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index opticodds_ball_odds_snapshots_fixture_idx
  on opticodds_ball_odds_snapshots (fixture_id, snapshot_time desc);

create index opticodds_ball_odds_snapshots_ball_idx
  on opticodds_ball_odds_snapshots (fixture_id, ball_key, sportsbook_id, market_id);

create table opticodds_stream_cursors (
  stream_key text primary key,
  last_entry_id text,
  payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);
