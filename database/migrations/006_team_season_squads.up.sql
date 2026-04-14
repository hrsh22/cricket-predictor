create table team_season_squads (
  id bigserial primary key,
  season integer not null,
  team_name text not null,
  player_registry_id bigint references player_registry(id) on delete set null,
  source_player_name text not null,
  squad_role text,
  source text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (season, team_name, source_player_name)
);

create index team_season_squads_team_idx
  on team_season_squads (season, team_name, source_player_name);

create index team_season_squads_player_idx
  on team_season_squads (player_registry_id, season);
