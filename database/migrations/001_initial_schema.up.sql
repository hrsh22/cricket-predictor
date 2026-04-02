create type checkpoint_type as enum ('pre_match', 'post_toss', 'innings_break');

create table raw_market_snapshots (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  source_market_id text not null,
  market_slug text not null,
  event_slug text,
  snapshot_time timestamptz not null,
  market_status text,
  yes_outcome_name text,
  no_outcome_name text,
  outcome_probabilities jsonb not null default '{}'::jsonb,
  last_traded_price numeric(10, 4) check (last_traded_price is null or last_traded_price >= 0),
  liquidity numeric(14, 2) check (liquidity is null or liquidity >= 0),
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (source_market_id, snapshot_time)
);

create index raw_market_snapshots_lookup_idx
  on raw_market_snapshots (source_market_id, snapshot_time desc);

create index raw_market_snapshots_market_slug_idx
  on raw_market_snapshots (market_slug);

create table raw_cricket_snapshots (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  provider text not null,
  source_match_id text not null,
  snapshot_time timestamptz not null,
  match_status text,
  innings_number smallint check (innings_number is null or innings_number between 0 and 2),
  over_number numeric(4, 1) check (over_number is null or over_number >= 0),
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (source_match_id, snapshot_time)
);

create index raw_cricket_snapshots_lookup_idx
  on raw_cricket_snapshots (source_match_id, snapshot_time desc);

create table canonical_matches (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  match_slug text not null unique,
  source_match_id text unique,
  season integer not null check (season >= 2008),
  scheduled_start timestamptz not null,
  team_a_name text not null,
  team_b_name text not null,
  venue_name text,
  status text not null default 'scheduled' check (status in ('scheduled', 'in_progress', 'completed', 'abandoned', 'no_result')),
  toss_winner_team_name text,
  toss_decision text check (toss_decision is null or toss_decision in ('bat', 'bowl')),
  winning_team_name text,
  result_type text check (result_type is null or result_type in ('win', 'tie', 'no_result', 'abandoned', 'super_over')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index canonical_matches_schedule_idx
  on canonical_matches (scheduled_start);

create index canonical_matches_season_idx
  on canonical_matches (season);

create table checkpoint_states (
  id bigserial primary key,
  canonical_match_id bigint not null references canonical_matches(id) on delete cascade,
  checkpoint_type checkpoint_type not null,
  snapshot_time timestamptz not null,
  state_version integer not null default 1 check (state_version > 0),
  source_market_snapshot_id bigint references raw_market_snapshots(id) on delete set null,
  source_cricket_snapshot_id bigint references raw_cricket_snapshots(id) on delete set null,
  innings_number smallint check (innings_number is null or innings_number between 1 and 2),
  batting_team_name text,
  bowling_team_name text,
  runs integer check (runs is null or runs >= 0),
  wickets integer check (wickets is null or wickets between 0 and 10),
  overs numeric(4, 1) check (overs is null or overs >= 0),
  target_runs integer check (target_runs is null or target_runs >= 0),
  current_run_rate numeric(6, 3) check (current_run_rate is null or current_run_rate >= 0),
  required_run_rate numeric(6, 3) check (required_run_rate is null or required_run_rate >= 0),
  state_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (canonical_match_id, checkpoint_type, snapshot_time, state_version)
);

create index checkpoint_states_match_checkpoint_idx
  on checkpoint_states (canonical_match_id, checkpoint_type, snapshot_time desc);

create table match_features (
  id bigserial primary key,
  checkpoint_state_id bigint not null references checkpoint_states(id) on delete cascade,
  feature_set_version text not null,
  generated_at timestamptz not null default now(),
  features jsonb not null,
  unique (checkpoint_state_id, feature_set_version)
);

create index match_features_checkpoint_idx
  on match_features (checkpoint_state_id, generated_at desc);

create table model_registry (
  id bigserial primary key,
  model_key text not null unique,
  checkpoint_type checkpoint_type not null,
  model_family text not null,
  version text not null,
  training_window text,
  is_active boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (checkpoint_type, model_family, version)
);

create table scoring_runs (
  id bigserial primary key,
  run_key text not null unique,
  checkpoint_type checkpoint_type not null,
  run_status text not null check (run_status in ('running', 'succeeded', 'failed')),
  triggered_by text not null default 'manual',
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  input_snapshot_time timestamptz,
  notes text,
  metadata jsonb not null default '{}'::jsonb
);

create index scoring_runs_started_at_idx
  on scoring_runs (started_at desc);

create table model_scores (
  id bigserial primary key,
  scoring_run_id bigint not null references scoring_runs(id) on delete cascade,
  canonical_match_id bigint not null references canonical_matches(id) on delete cascade,
  checkpoint_state_id bigint not null references checkpoint_states(id) on delete cascade,
  model_registry_id bigint not null references model_registry(id) on delete restrict,
  fair_win_probability numeric(6, 5) not null check (fair_win_probability between 0 and 1),
  market_implied_probability numeric(6, 5) check (market_implied_probability is null or market_implied_probability between 0 and 1),
  edge numeric(7, 5),
  score_payload jsonb not null default '{}'::jsonb,
  scored_at timestamptz not null default now(),
  unique (scoring_run_id, canonical_match_id, model_registry_id)
);

create index model_scores_match_idx
  on model_scores (canonical_match_id, scored_at desc);

create table backtests (
  id bigserial primary key,
  run_key text not null unique,
  model_registry_id bigint not null references model_registry(id) on delete restrict,
  checkpoint_type checkpoint_type not null,
  run_status text not null check (run_status in ('running', 'succeeded', 'failed')),
  season_from integer not null check (season_from >= 2008),
  season_to integer not null check (season_to >= season_from),
  sample_size integer check (sample_size is null or sample_size >= 0),
  log_loss numeric(10, 6),
  brier_score numeric(10, 6),
  calibration_error numeric(10, 6),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  summary jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb
);

create index backtests_model_idx
  on backtests (model_registry_id, checkpoint_type, started_at desc);
