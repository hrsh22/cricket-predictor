const validGammaMarket = {
  id: "pm-ipl-001",
  slug: "ipl-2026-chennai-super-kings-vs-mumbai-indians-winner",
  event_slug: "ipl-2026-chennai-super-kings-vs-mumbai-indians",
  question: "Who will win Chennai Super Kings vs Mumbai Indians?",
  market_type: "binary",
  active: true,
  closed: false,
  archived: false,
  end_date_iso: "2026-03-29T14:00:00.000Z",
  updated_at: "2026-03-29T13:04:00.000Z",
  outcomes: ["Chennai Super Kings", "Mumbai Indians"],
  outcome_token_ids: ["pm-token-yes", "pm-token-no"],
  outcome_prices: [0.54, 0.46],
  liquidity: 42000,
  tags: ["ipl", "cricket", "winner"],
} as const;

const validClobMarket = {
  market_id: "pm-ipl-001",
  liquidity: 42000,
  last_traded_price: 0.53,
  updated_at: "2026-03-29T13:09:00.000Z",
} as const;

export const validIplWinnerMarketDiscovery = {
  snapshotTime: "2026-03-29T13:10:00.000Z",
  gamma: validGammaMarket,
  clob: validClobMarket,
} as const;

export const duplicateIplWinnerMarketDiscoveries = [
  validIplWinnerMarketDiscovery,
  validIplWinnerMarketDiscovery,
] as const;

export const staleIplWinnerMarketDiscovery = {
  ...validIplWinnerMarketDiscovery,
  snapshotTime: "2026-03-29T15:10:00.000Z",
} as const;

export const unsupportedIplWinnerMarketDiscovery = {
  ...validIplWinnerMarketDiscovery,
  gamma: {
    ...validGammaMarket,
    slug: "ipl-2026-chennai-super-kings-vs-mumbai-indians-toss",
    question: "Who will win the toss?",
    tags: ["ipl", "cricket", "toss"],
  },
} as const;

const missingFieldGammaMarket = structuredClone(validGammaMarket) as Record<
  string,
  unknown
>;
delete missingFieldGammaMarket["event_slug"];

export const missingFieldsIplWinnerMarketDiscovery = {
  ...validIplWinnerMarketDiscovery,
  gamma: missingFieldGammaMarket,
} as const;

export const malformedIplWinnerMarketDiscovery = {
  snapshotTime: "2026-03-29T13:10:00.000Z",
  gamma: null,
  clob: [],
} as const;

export const liveIplMoneylineMarket = {
  id: "1683648",
  question: "Indian Premier League: Mumbai Indians vs Kolkata Knight Riders",
  slug: "cricipl-mum-kol-2026-03-29",
  endDate: "2026-04-05T10:00:00Z",
  liquidity: "20449.4369",
  outcomes: '["Mumbai Indians", "Kolkata Knight Riders"]',
  outcomePrices: '["0.425", "0.575"]',
  active: true,
  closed: false,
  archived: false,
  updatedAt: "2026-03-29T14:39:20.030173Z",
  liquidityClob: 20449.4369,
  clobTokenIds:
    '["9243859686424154476031393325319635706552449918713151700199742912076304706244", "36714476552064680780880870617721448145966339414996247445158282228193476201096"]',
  lastTradePrice: 0.43,
  sportsMarketType: "moneyline",
  events: [
    {
      id: "296910",
      slug: "cricipl-mum-kol-2026-03-29",
      title: "Indian Premier League: Mumbai Indians vs Kolkata Knight Riders",
      seriesSlug: "indian-premier-league",
    },
  ],
} as const;

export const liveIplTossMarket = {
  ...liveIplMoneylineMarket,
  id: "1683649",
  slug: "cricipl-mum-kol-2026-03-29-toss-winner",
  question:
    "Indian Premier League: Mumbai Indians vs Kolkata Knight Riders - Who wins the toss?",
  sportsMarketType: "cricket_toss_winner",
} as const;
