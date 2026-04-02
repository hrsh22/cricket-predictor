create table market_match_mappings (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  source_market_id text not null unique,
  source_market_snapshot_id bigint not null references raw_market_snapshots(id) on delete cascade,
  canonical_match_id bigint references canonical_matches(id) on delete set null,
  mapping_status text not null check (mapping_status in ('resolved', 'ambiguous', 'unresolved')),
  confidence numeric(6, 5),
  resolver_version text not null,
  reason text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index market_match_mappings_status_idx
  on market_match_mappings (mapping_status, confidence desc nulls last);

create index market_match_mappings_match_idx
  on market_match_mappings (canonical_match_id, updated_at desc);
