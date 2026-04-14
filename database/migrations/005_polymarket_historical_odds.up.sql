create table raw_polymarket_price_history (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  source_event_id text not null,
  source_market_id text not null,
  event_slug text not null,
  market_slug text not null,
  condition_id text not null,
  market_type text,
  token_id text not null,
  outcome_name text not null,
  outcome_index smallint not null check (outcome_index between 0 and 1),
  point_time timestamptz not null,
  price numeric(10, 6) not null check (price between 0 and 1),
  query_start_time timestamptz,
  query_end_time timestamptz,
  fidelity_minutes integer check (fidelity_minutes is null or fidelity_minutes > 0),
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (token_id, point_time)
);

create index raw_polymarket_price_history_event_idx
  on raw_polymarket_price_history (event_slug, point_time desc);

create index raw_polymarket_price_history_market_idx
  on raw_polymarket_price_history (source_market_id, point_time desc);

create table raw_polymarket_trades (
  id bigserial primary key,
  competition text not null default 'IPL' check (competition = 'IPL'),
  trade_key text not null unique,
  source_event_id text not null,
  source_market_id text not null,
  event_slug text not null,
  market_slug text not null,
  condition_id text not null,
  market_type text,
  token_id text not null,
  outcome_name text not null,
  outcome_index smallint not null check (outcome_index between 0 and 1),
  trade_time timestamptz not null,
  price numeric(10, 6) not null check (price between 0 and 1),
  size numeric(18, 8) not null check (size >= 0),
  side text not null check (side in ('BUY', 'SELL')),
  transaction_hash text,
  proxy_wallet text,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index raw_polymarket_trades_event_idx
  on raw_polymarket_trades (event_slug, trade_time desc);

create index raw_polymarket_trades_market_idx
  on raw_polymarket_trades (source_market_id, trade_time desc);

create index raw_polymarket_trades_token_idx
  on raw_polymarket_trades (token_id, trade_time desc);
