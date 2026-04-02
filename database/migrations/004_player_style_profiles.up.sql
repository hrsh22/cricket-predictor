create table player_style_profiles (
  id bigserial primary key,
  player_registry_id bigint not null unique references player_registry(id) on delete cascade,
  source text not null,
  batting_hand text,
  bowling_arm text,
  bowling_style text,
  bowling_type_group text,
  player_role text,
  confidence numeric(4,3) not null default 1.0,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint player_style_profiles_source_check check (source in ('curated_manual', 'curated_import', 'external_verified')),
  constraint player_style_profiles_batting_hand_check check (batting_hand in ('left', 'right', 'unknown') or batting_hand is null),
  constraint player_style_profiles_bowling_arm_check check (bowling_arm in ('left', 'right', 'unknown') or bowling_arm is null),
  constraint player_style_profiles_bowling_type_group_check check (bowling_type_group in ('pace', 'spin', 'mixed', 'none', 'unknown') or bowling_type_group is null),
  constraint player_style_profiles_player_role_check check (player_role in ('batter', 'bowler', 'all_rounder', 'wicketkeeper_batter', 'unknown') or player_role is null),
  constraint player_style_profiles_confidence_check check (confidence >= 0 and confidence <= 1)
);

create index player_style_profiles_role_idx
  on player_style_profiles (player_role, bowling_type_group, batting_hand);
