import type {
  MappingStatus,
  MatchResolutionCandidate,
  MatchResolutionMarketSnapshot,
  ResolverCandidate,
} from "../repositories/matching.js";

const TEAM_ALIASES: Readonly<Record<string, readonly string[]>> = {
  "mumbai indians": ["mumbai", "mi"],
  "chennai super kings": ["chennai", "csk", "super kings"],
  "kolkata knight riders": ["kolkata", "kkr", "knight riders"],
  "royal challengers bengaluru": [
    "rcb",
    "royal challengers",
    "royal challengers bangalore",
    "bangalore",
    "bengaluru",
  ],
  "delhi capitals": ["delhi", "dc", "daredevils", "delhi daredevils"],
  "rajasthan royals": ["rajasthan", "rr", "royals"],
  "sunrisers hyderabad": ["hyderabad", "srh", "sunrisers"],
  "punjab kings": ["punjab", "pbks", "kings", "kxi", "kings xi"],
  "gujarat titans": ["gujarat", "gt", "titans"],
  "lucknow super giants": ["lucknow", "lsg", "super giants"],
};

const DEFAULT_MINIMUM_CONFIDENCE = 0.85;
const DEFAULT_MINIMUM_MARGIN = 0.12;

export interface MatchResolverOptions {
  minimumConfidence?: number;
  minimumMargin?: number;
  strictTimeWindowHours?: number;
}

export interface MatchResolverResult {
  sourceMarketId: string;
  sourceMarketSnapshotId: number;
  mappingStatus: MappingStatus;
  canonicalMatchId: number | null;
  confidence: number | null;
  reason: string;
  candidates: ResolverCandidate[];
}

export function resolveMarketMatchCandidate(input: {
  market: MatchResolutionMarketSnapshot;
  matches: readonly MatchResolutionCandidate[];
  options?: MatchResolverOptions;
}): MatchResolverResult {
  const minimumConfidence =
    input.options?.minimumConfidence ?? DEFAULT_MINIMUM_CONFIDENCE;
  const minimumMargin = input.options?.minimumMargin ?? DEFAULT_MINIMUM_MARGIN;
  const strictTimeWindowHours = input.options?.strictTimeWindowHours ?? 120;
  const marketTeams = extractMarketTeams(input.market);

  if (marketTeams.length < 2) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "unresolved",
      canonicalMatchId: null,
      confidence: null,
      reason: "unable_to_extract_two_teams",
      candidates: [],
    };
  }

  const rankedCandidates = input.matches
    .map((match) => ({
      match,
      confidence: scoreCandidate(input.market, marketTeams, match),
    }))
    .filter(({ confidence }) => confidence > 0)
    .sort((left, right) => right.confidence - left.confidence)
    .map(({ match, confidence }) => ({
      canonicalMatchId: match.id,
      matchSlug: match.matchSlug,
      confidence,
    }));

  const bestCandidate = rankedCandidates[0] ?? null;
  const secondCandidate = rankedCandidates[1] ?? null;

  if (bestCandidate === null) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "unresolved",
      canonicalMatchId: null,
      confidence: null,
      reason: "no_viable_candidate",
      candidates: rankedCandidates,
    };
  }

  const match = input.matches.find(
    (candidate) => candidate.id === bestCandidate.canonicalMatchId,
  );

  if (match === undefined) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "unresolved",
      canonicalMatchId: null,
      confidence: null,
      reason: "internal_candidate_lookup_failed",
      candidates: rankedCandidates,
    };
  }

  const marketReferenceTime = getMarketReferenceTime(input.market);
  const hourDistance = computeHourDistance(
    marketReferenceTime,
    match.scheduledStart,
  );
  if (hourDistance > strictTimeWindowHours) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "unresolved",
      canonicalMatchId: null,
      confidence: Number(bestCandidate.confidence.toFixed(5)),
      reason: "outside_time_window",
      candidates: rankedCandidates,
    };
  }

  const margin =
    secondCandidate === null
      ? bestCandidate.confidence
      : bestCandidate.confidence - secondCandidate.confidence;

  if (bestCandidate.confidence < minimumConfidence) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "unresolved",
      canonicalMatchId: null,
      confidence: Number(bestCandidate.confidence.toFixed(5)),
      reason: "low_confidence",
      candidates: rankedCandidates,
    };
  }

  if (margin < minimumMargin) {
    return {
      sourceMarketId: input.market.sourceMarketId,
      sourceMarketSnapshotId: input.market.id,
      mappingStatus: "ambiguous",
      canonicalMatchId: null,
      confidence: Number(bestCandidate.confidence.toFixed(5)),
      reason: "multiple_close_candidates",
      candidates: rankedCandidates,
    };
  }

  return {
    sourceMarketId: input.market.sourceMarketId,
    sourceMarketSnapshotId: input.market.id,
    mappingStatus: "resolved",
    canonicalMatchId: bestCandidate.canonicalMatchId,
    confidence: Number(bestCandidate.confidence.toFixed(5)),
    reason: "high_confidence_match",
    candidates: rankedCandidates,
  };
}

function extractMarketTeams(market: MatchResolutionMarketSnapshot): string[] {
  const directTeams = [market.yesOutcomeName, market.noOutcomeName]
    .map((team) => (team === null ? null : normalizeTeamLabel(team)))
    .filter((team): team is string => team !== null && team.length > 0);

  if (directTeams.length >= 2) {
    return directTeams;
  }

  const rawQuestion = readNestedString(market.payload, ["gamma", "question"]);
  const parsedFromQuestion = splitQuestionTeams(rawQuestion);
  if (parsedFromQuestion.length >= 2) {
    return parsedFromQuestion;
  }

  const slugSource = market.eventSlug ?? market.marketSlug;
  const parsedFromSlug = splitSlugTeams(slugSource);

  return parsedFromSlug;
}

function splitQuestionTeams(question: string | null): string[] {
  if (question === null) {
    return [];
  }

  const normalized = question
    .toLowerCase()
    .replace(/^who will win\s+/u, "")
    .replace(/^indian premier league:\s*/u, "")
    .replace(/\?/gu, "")
    .trim();

  const parts = normalized
    .split(/\s+vs\s+/u)
    .map((part) => normalizeTeamLabel(part))
    .filter((part) => part.length > 0);

  return parts.slice(0, 2);
}

function splitSlugTeams(slug: string): string[] {
  const cleaned = slug.toLowerCase();
  const matcher = cleaned.match(/(?:^|-)\d{4}-\d{2}-\d{2}(?:-|$)/u);
  const trimmed =
    matcher === null
      ? cleaned
      : cleaned.slice(0, matcher.index ?? cleaned.length);

  const compact = trimmed
    .replace(/^cricipl-/u, "")
    .replace(/^ipl-\d{4}-/u, "")
    .replace(/-winner$/u, "")
    .replace(/-match-winner$/u, "")
    .trim();

  const parts = compact
    .split(/-vs-/u)
    .map((part) => normalizeTeamLabel(part.replace(/-/gu, " ")))
    .filter((part) => part.length > 0);

  return parts.slice(0, 2);
}

function scoreCandidate(
  market: MatchResolutionMarketSnapshot,
  marketTeams: readonly string[],
  match: MatchResolutionCandidate,
): number {
  const teamScore = scoreTeamFit(marketTeams, [
    match.teamAName,
    match.teamBName,
  ]);
  if (teamScore === 0) {
    return 0;
  }

  const timeScore = scoreTimeFit(
    getMarketReferenceTime(market),
    match.scheduledStart,
  );
  const contextScore = scoreContextFit(market, match);

  const weighted = teamScore * 0.72 + timeScore * 0.18 + contextScore * 0.1;
  return Number(weighted.toFixed(5));
}

function scoreTeamFit(
  marketTeams: readonly string[],
  matchTeams: readonly [string, string],
): number {
  if (marketTeams.length < 2) {
    return 0;
  }

  const normalizedMatchTeams = matchTeams.map((team) =>
    normalizeTeamLabel(team),
  );
  const firstMapping =
    teamSimilarity(marketTeams[0] ?? "", normalizedMatchTeams[0] ?? "") +
    teamSimilarity(marketTeams[1] ?? "", normalizedMatchTeams[1] ?? "");
  const secondMapping =
    teamSimilarity(marketTeams[0] ?? "", normalizedMatchTeams[1] ?? "") +
    teamSimilarity(marketTeams[1] ?? "", normalizedMatchTeams[0] ?? "");

  return Math.max(firstMapping, secondMapping) / 2;
}

function teamSimilarity(left: string, right: string): number {
  if (left.length === 0 || right.length === 0) {
    return 0;
  }

  const canonicalLeft = canonicalizeTeam(left);
  const canonicalRight = canonicalizeTeam(right);
  if (canonicalLeft !== null && canonicalLeft === canonicalRight) {
    return 1;
  }

  if (left === right) {
    return 1;
  }

  const leftTokens = new Set(tokenize(left));
  const rightTokens = new Set(tokenize(right));
  const intersection = countIntersection(leftTokens, rightTokens);
  const union = new Set([...leftTokens, ...rightTokens]).size;
  const tokenJaccard = union === 0 ? 0 : intersection / union;

  const leftCompact = left.replace(/\s+/gu, "");
  const rightCompact = right.replace(/\s+/gu, "");
  const compactSimilarity =
    leftCompact.includes(rightCompact) || rightCompact.includes(leftCompact)
      ? 0.7
      : 0;

  return Math.max(tokenJaccard, compactSimilarity);
}

function scoreTimeFit(snapshotTime: string, scheduledStart: string): number {
  const hourDistance = computeHourDistance(snapshotTime, scheduledStart);

  if (hourDistance <= 6) {
    return 1;
  }

  if (hourDistance <= 24) {
    return 0.9;
  }

  if (hourDistance <= 48) {
    return 0.75;
  }

  if (hourDistance <= 96) {
    return 0.55;
  }

  if (hourDistance <= 144) {
    return 0.35;
  }

  return 0;
}

function scoreContextFit(
  market: MatchResolutionMarketSnapshot,
  match: MatchResolutionCandidate,
): number {
  const contextText = [
    market.marketSlug,
    market.eventSlug,
    readNestedString(market.payload, ["gamma", "question"]),
  ]
    .filter((value): value is string => value !== null)
    .join(" ")
    .toLowerCase();

  if (contextText.length === 0) {
    return 0;
  }

  const teamA =
    canonicalizeTeam(match.teamAName) ?? normalizeTeamLabel(match.teamAName);
  const teamB =
    canonicalizeTeam(match.teamBName) ?? normalizeTeamLabel(match.teamBName);

  const includesA = contextContainsTeam(contextText, teamA);
  const includesB = contextContainsTeam(contextText, teamB);

  if (includesA && includesB) {
    return 1;
  }

  if (includesA || includesB) {
    return 0.5;
  }

  return 0;
}

function contextContainsTeam(contextText: string, teamName: string): boolean {
  const canonical = canonicalizeTeam(teamName);
  if (canonical !== null) {
    const aliases = TEAM_ALIASES[canonical] ?? [];
    const valuesToTest = [canonical, ...aliases]
      .map((alias) => alias.toLowerCase())
      .filter((alias) => alias.length > 1);

    for (const alias of valuesToTest) {
      if (contextText.includes(alias)) {
        return true;
      }
    }
  }

  return contextText.includes(normalizeTeamLabel(teamName));
}

function canonicalizeTeam(teamName: string): string | null {
  const normalized = normalizeTeamLabel(teamName);
  if (normalized.length === 0) {
    return null;
  }

  if (TEAM_ALIASES[normalized] !== undefined) {
    return normalized;
  }

  for (const [canonical, aliases] of Object.entries(TEAM_ALIASES)) {
    if (aliases.some((alias) => alias === normalized)) {
      return canonical;
    }
  }

  return null;
}

function normalizeTeamLabel(value: string): string {
  return value
    .toLowerCase()
    .replace(/^indian premier league:\s*/u, "")
    .replace(/\s+inning\s*\d+$/u, "")
    .replace(/[^a-z0-9\s]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function tokenize(value: string): string[] {
  return normalizeTeamLabel(value)
    .split(" ")
    .filter((token) => token.length > 1);
}

function countIntersection(left: Set<string>, right: Set<string>): number {
  let intersection = 0;

  for (const token of left) {
    if (right.has(token)) {
      intersection += 1;
    }
  }

  return intersection;
}

function computeHourDistance(leftIso: string, rightIso: string): number {
  const leftMs = Date.parse(leftIso);
  const rightMs = Date.parse(rightIso);
  const deltaMs = Math.abs(leftMs - rightMs);

  return deltaMs / (1000 * 60 * 60);
}

export function getMarketReferenceTime(
  market: Pick<MatchResolutionMarketSnapshot, "snapshotTime" | "payload">,
): string {
  return (
    readNestedString(market.payload, [
      "gamma",
      "raw",
      "events",
      "0",
      "startTime",
    ]) ??
    readNestedString(market.payload, ["gamma", "raw", "gameStartTime"]) ??
    readNestedString(market.payload, [
      "gamma",
      "raw",
      "events",
      "0",
      "eventDate",
    ]) ??
    readNestedString(market.payload, ["gamma", "raw", "startTime"]) ??
    market.snapshotTime
  );
}

function readNestedString(
  payload: JsonLike,
  path: readonly string[],
): string | null {
  let current: unknown = payload;

  for (const key of path) {
    if (!isRecord(current)) {
      return null;
    }
    current = current[key];
  }

  return typeof current === "string" ? current : null;
}

type JsonLike = Record<string, unknown>;

function isRecord(value: unknown): value is JsonLike {
  return typeof value === "object" && value !== null;
}
