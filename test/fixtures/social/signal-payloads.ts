export const socialSignalContext = {
  competition: "IPL",
  matchSlug: "ipl-2026-chennai-vs-mumbai",
  checkpointType: "post_toss",
} as const;

export const validSocialSignalCandidate = {
  competition: "IPL",
  matchSlug: "ipl-2026-chennai-vs-mumbai",
  checkpointType: "post_toss",
  targetTeamName: "Chennai Super Kings",
  source: {
    providerKey: "mirofish-inspired-manual",
    sourceType: "analyst_note",
    sourceId: "note-001",
    sourceLabel: "Trusted IPL analyst note",
    sourceQuality: "trusted",
    capturedAt: "2026-03-29T13:40:00.000Z",
    publishedAt: "2026-03-29T13:35:00.000Z",
    provenanceUrl: "https://example.com/analyst-note",
  },
  summary: "Trusted analyst expects Chennai's batting depth to matter after the toss.",
  confidence: 0.74,
  requestedAdjustment: 0.08,
} as const;

export const noisySocialSignalCandidate = {
  ...validSocialSignalCandidate,
  source: {
    ...validSocialSignalCandidate.source,
    sourceId: "chatter-002",
    sourceType: "market_chatter",
    sourceQuality: "noisy",
  },
  summary: "Unverified chatter says the market is sleeping on Chennai.",
} as const;

export const lowConfidenceSocialSignalCandidate = {
  ...validSocialSignalCandidate,
  confidence: 0.22,
} as const;

export const emptySummarySocialSignalCandidate = {
  ...validSocialSignalCandidate,
  summary: "   ",
} as const;
