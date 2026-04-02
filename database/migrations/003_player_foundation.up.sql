create table player_registry (
  id bigserial primary key,
  cricsheet_player_id text not null unique,
  canonical_name text not null,
  batting_style text,
  bowling_style text,
  bowling_type_group text,
  player_role text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index player_registry_canonical_name_idx
  on player_registry (canonical_name);

create table match_player_appearances (
  id bigserial primary key,
  canonical_match_id bigint not null references canonical_matches(id) on delete cascade,
  team_name text not null,
  player_registry_id bigint references player_registry(id) on delete set null,
  source_player_name text not null,
  lineup_order smallint not null check (lineup_order between 1 and 15),
  is_playing_xi boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (canonical_match_id, team_name, lineup_order)
);

create index match_player_appearances_match_idx
  on match_player_appearances (canonical_match_id, team_name, lineup_order);

create index match_player_appearances_player_idx
  on match_player_appearances (player_registry_id, created_at desc);
