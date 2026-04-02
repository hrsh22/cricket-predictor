import type { Pool } from "pg";

import type { PreMatchFeatureContext } from "../features/pre-match.js";
import {
  createVenueTossDecisionKey,
  createHeadToHeadKey,
  createVenueStrengthKey,
} from "../features/pre-match.js";
import { computeGlicko2Ratings, type Glicko2Rating } from "../ratings/index.js";

const DEFAULT_ELO = 1500;
const DEFAULT_RD = 300;
const ELO_K_FACTOR = 32;

interface MatchResult {
  teamA: string;
  teamB: string;
  winner: string | null;
  venue: string | null;
  scheduledStart: Date;
  resultKnownAt: Date;
  tossWinnerTeamName: string | null;
  tossDecision: "bat" | "bowl" | null;
}

interface TeamForm {
  [teamName: string]: {
    wins: number;
    matches: number;
    weightedWinRate: number;
  };
}

interface TeamSchedule {
  [teamName: string]: { lastMatchDate: Date | null; recentMatches: Date[] };
}

interface VenueStats {
  [key: string]: { wins: number; matches: number };
}

interface HeadToHeadStats {
  [key: string]: { wins: number; matches: number };
}

interface VenueTossStats {
  [key: string]: { wins: number; matches: number };
}

export async function buildFeatureContextFromHistory(
  pool: Pool,
  asOfDate: Date,
): Promise<PreMatchFeatureContext> {
  const matches = await fetchCompletedMatches(pool, asOfDate);

  const eloRatings = computeEloRatings(matches, asOfDate);
  const teamRatingDeviations: Record<string, number> = {};
  for (const team of Object.keys(eloRatings)) {
    teamRatingDeviations[team] = DEFAULT_RD;
  }

  const teamForm = computeRecentForm(matches, asOfDate, 8);
  const venueStrength = computeVenueStrength(matches);
  const headToHeadStrength = computeHeadToHeadStrength(matches);
  const venueTossDecisionWinRate = computeVenueTossDecisionWinRate(matches);
  const scheduleContext = computeScheduleContext(matches, asOfDate);

  return {
    teamRatings: eloRatings,
    teamRatingDeviations,
    teamRecentForm: Object.fromEntries(
      Object.entries(teamForm).map(([team, form]) => [
        team,
        {
          wins: form.wins,
          matches: form.matches,
          weightedWinRate: form.weightedWinRate,
        },
      ]),
    ),
    teamVenueStrength: venueStrength,
    teamHeadToHeadStrength: headToHeadStrength,
    venueTossDecisionWinRate,
    teamSchedule: Object.fromEntries(
      Object.entries(scheduleContext).map(([team, ctx]) => [
        team,
        {
          daysSincePreviousMatch: ctx.lastMatchDate
            ? Math.floor(
                (asOfDate.getTime() - ctx.lastMatchDate.getTime()) /
                  (1000 * 60 * 60 * 24),
              )
            : null,
          matchesInPrevious7Days: ctx.recentMatches.filter(
            (d) =>
              asOfDate.getTime() - d.getTime() < 7 * 24 * 60 * 60 * 1000 &&
              d < asOfDate,
          ).length,
        },
      ]),
    ),
  };
}

function computeEloRatings(
  matches: MatchResult[],
  asOfDate: Date,
): Record<string, number> {
  const ratings: Record<string, number> = {};

  for (const match of matches) {
    if (match.winner === null) continue;

    const teamARating = ratings[match.teamA] ?? DEFAULT_ELO;
    const teamBRating = ratings[match.teamB] ?? DEFAULT_ELO;

    const expectedA = 1 / (1 + Math.pow(10, (teamBRating - teamARating) / 400));
    const expectedB = 1 - expectedA;

    const actualA = match.winner === match.teamA ? 1 : 0;
    const actualB = 1 - actualA;

    const recencyWeight = computeRecencyWeight(match.resultKnownAt, asOfDate);
    const adjustedK = ELO_K_FACTOR * recencyWeight;

    ratings[match.teamA] = Number(
      (teamARating + adjustedK * (actualA - expectedA)).toFixed(6),
    );
    ratings[match.teamB] = Number(
      (teamBRating + adjustedK * (actualB - expectedB)).toFixed(6),
    );
  }

  return ratings;
}

function computeRecencyWeight(matchDate: Date, asOfDate: Date): number {
  const now = asOfDate.getTime();
  const daysAgo = Math.max(
    0,
    (now - matchDate.getTime()) / (1000 * 60 * 60 * 24),
  );

  if (daysAgo <= 120) {
    return 1.15;
  }

  if (daysAgo <= 365) {
    return 1.0;
  }

  if (daysAgo <= 730) {
    return 0.9;
  }

  return 0.8;
}

async function fetchCompletedMatches(
  pool: Pool,
  asOfDate: Date,
): Promise<MatchResult[]> {
  const result = await pool.query<{
    team_a_name: string;
    team_b_name: string;
    winning_team_name: string | null;
    venue_name: string | null;
    scheduled_start: Date;
    result_known_at: Date | null;
    toss_winner_team_name: string | null;
    toss_decision: "bat" | "bowl" | null;
  }>(
    `
      WITH first_completed_snapshot AS (
        SELECT
          source_match_id,
          MIN(snapshot_time) AS result_known_at
        FROM raw_cricket_snapshots
        WHERE (
          match_status IS NOT NULL
          AND (
            lower(match_status) IN ('completed', 'complete', 'result', 'finished', 'end', 'ended')
            OR lower(match_status) LIKE '%won%'
            OR lower(match_status) LIKE '%draw%'
            OR lower(match_status) LIKE '%tie%'
            OR lower(match_status) LIKE '%abandon%'
            OR lower(match_status) LIKE '%no result%'
          )
        )
        OR (
          payload ->> 'status' IS NOT NULL
          AND (
            lower(payload ->> 'status') IN ('completed', 'complete', 'result', 'finished', 'end', 'ended')
            OR lower(payload ->> 'status') LIKE '%won%'
            OR lower(payload ->> 'status') LIKE '%draw%'
            OR lower(payload ->> 'status') LIKE '%tie%'
            OR lower(payload ->> 'status') LIKE '%abandon%'
            OR lower(payload ->> 'status') LIKE '%no result%'
          )
        )
        GROUP BY source_match_id
      )
      SELECT
        cm.team_a_name,
        cm.team_b_name,
        cm.winning_team_name,
        cm.venue_name,
        cm.scheduled_start,
        COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) AS result_known_at,
        cm.toss_winner_team_name,
        cm.toss_decision
      FROM canonical_matches cm
      LEFT JOIN first_completed_snapshot fcs
        ON fcs.source_match_id = cm.source_match_id
      WHERE cm.competition = 'IPL'
        AND cm.status = 'completed'
        AND cm.result_type = 'win'
        AND cm.winning_team_name IS NOT NULL
        AND COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) < $1
      ORDER BY result_known_at ASC, cm.scheduled_start ASC
    `,
    [asOfDate],
  );

  return result.rows.map((row) => ({
    teamA: row.team_a_name,
    teamB: row.team_b_name,
    winner: row.winning_team_name,
    venue: row.venue_name,
    scheduledStart: row.scheduled_start,
    resultKnownAt: row.result_known_at ?? row.scheduled_start,
    tossWinnerTeamName: row.toss_winner_team_name,
    tossDecision: row.toss_decision,
  }));
}

function computeRecentForm(
  matches: MatchResult[],
  asOfDate: Date,
  windowSize: number,
): TeamForm {
  const form: TeamForm = {};
  const teamMatches: Record<string, MatchResult[]> = {};

  for (const match of matches) {
    if (match.resultKnownAt >= asOfDate) continue;

    if (teamMatches[match.teamA] === undefined) teamMatches[match.teamA] = [];
    if (teamMatches[match.teamB] === undefined) teamMatches[match.teamB] = [];

    teamMatches[match.teamA]!.push(match);
    teamMatches[match.teamB]!.push(match);
  }

  for (const [team, teamMatchList] of Object.entries(teamMatches)) {
    const recent = teamMatchList.slice(-windowSize);
    const wins = recent.filter((m) => m.winner === team).length;
    const rawWinRate = recent.length === 0 ? 0.5 : wins / recent.length;
    const shrunkWinRate = bayesianShrink(rawWinRate, recent.length, 0.5, 6);
    form[team] = {
      wins,
      matches: recent.length,
      weightedWinRate: Number(shrunkWinRate.toFixed(6)),
    };
  }

  return form;
}

function computeVenueStrength(matches: MatchResult[]): Record<string, number> {
  const venueStats: VenueStats = {};

  for (const match of matches) {
    if (match.venue === null || match.winner === null) continue;

    for (const team of [match.teamA, match.teamB]) {
      const key = createVenueStrengthKey(team, match.venue);
      if (!venueStats[key]) venueStats[key] = { wins: 0, matches: 0 };
      venueStats[key].matches++;
      if (match.winner === team) venueStats[key].wins++;
    }
  }

  const strengths: Record<string, number> = {};
  for (const [key, stats] of Object.entries(venueStats)) {
    if (stats.matches >= 2) {
      const rawWinRate = stats.wins / stats.matches;
      const shrunkWinRate = bayesianShrink(rawWinRate, stats.matches, 0.5, 5);
      strengths[key] = Number((shrunkWinRate - 0.5).toFixed(6));
    }
  }

  return strengths;
}

function computeHeadToHeadStrength(
  matches: MatchResult[],
): Record<string, number> {
  const h2hStats: HeadToHeadStats = {};

  for (const match of matches) {
    if (match.winner === null) continue;

    const pairA = createHeadToHeadKey(match.teamA, match.teamB);
    const pairB = createHeadToHeadKey(match.teamB, match.teamA);

    if (!h2hStats[pairA]) h2hStats[pairA] = { wins: 0, matches: 0 };
    if (!h2hStats[pairB]) h2hStats[pairB] = { wins: 0, matches: 0 };

    h2hStats[pairA].matches++;
    h2hStats[pairB].matches++;

    if (match.winner === match.teamA) {
      h2hStats[pairA].wins++;
    } else if (match.winner === match.teamB) {
      h2hStats[pairB].wins++;
    }
  }

  const strengths: Record<string, number> = {};
  for (const [key, stats] of Object.entries(h2hStats)) {
    if (stats.matches < 2) {
      continue;
    }

    const rawWinRate = stats.wins / stats.matches;
    const shrunkWinRate = bayesianShrink(rawWinRate, stats.matches, 0.5, 4);
    strengths[key] = Number((shrunkWinRate - 0.5).toFixed(6));
  }

  return strengths;
}

function computeScheduleContext(
  matches: MatchResult[],
  asOfDate: Date,
): TeamSchedule {
  const schedule: TeamSchedule = {};

  for (const match of matches) {
    if (match.resultKnownAt >= asOfDate) continue;

    for (const team of [match.teamA, match.teamB]) {
      if (!schedule[team]) {
        schedule[team] = { lastMatchDate: null, recentMatches: [] };
      }

      schedule[team].recentMatches.push(match.resultKnownAt);

      if (
        schedule[team].lastMatchDate === null ||
        match.resultKnownAt > schedule[team].lastMatchDate
      ) {
        schedule[team].lastMatchDate = match.resultKnownAt;
      }
    }
  }

  return schedule;
}

function computeVenueTossDecisionWinRate(
  matches: MatchResult[],
): Record<string, number> {
  const stats: VenueTossStats = {};

  for (const match of matches) {
    if (
      match.venue === null ||
      match.winner === null ||
      match.tossDecision === null ||
      match.tossWinnerTeamName === null
    ) {
      continue;
    }

    const key = createVenueTossDecisionKey(match.venue, match.tossDecision);
    if (stats[key] === undefined) {
      stats[key] = { wins: 0, matches: 0 };
    }

    stats[key].matches += 1;
    if (match.winner === match.tossWinnerTeamName) {
      stats[key].wins += 1;
    }
  }

  const rates: Record<string, number> = {};
  for (const [key, value] of Object.entries(stats)) {
    const rawRate = value.wins / value.matches;
    const shrunkRate = bayesianShrink(rawRate, value.matches, 0.5, 10);
    rates[key] = Number(shrunkRate.toFixed(6));
  }

  return rates;
}

function bayesianShrink(
  observedRate: number,
  sampleSize: number,
  priorMean: number,
  priorSampleSize: number,
): number {
  const boundedObserved = Math.max(0, Math.min(observedRate, 1));
  const boundedSample = Math.max(0, sampleSize);
  const boundedPriorSample = Math.max(0, priorSampleSize);

  const numerator =
    boundedObserved * boundedSample + priorMean * boundedPriorSample;
  const denominator = boundedSample + boundedPriorSample;

  if (denominator <= 0) {
    return priorMean;
  }

  return numerator / denominator;
}

export async function getGlicko2RatingsSnapshot(
  pool: Pool,
): Promise<Record<string, Glicko2Rating>> {
  const asOfDate = new Date();
  const matches = await fetchCompletedMatches(pool, asOfDate);
  return computeGlicko2Ratings(
    matches.map((m) => ({
      teamA: m.teamA,
      teamB: m.teamB,
      winner: m.winner,
      scheduledStart: m.scheduledStart,
    })),
    asOfDate,
  );
}

export async function debugFeatureContext(pool: Pool): Promise<void> {
  const context = await buildFeatureContextFromHistory(pool, new Date());

  console.log("\n=== Glicko-2 Ratings ===");
  const sortedRatings = Object.entries(context.teamRatings).sort(
    (a, b) => b[1] - a[1],
  );
  for (const [team, rating] of sortedRatings) {
    const rd = context.teamRatingDeviations[team] ?? 300;
    console.log(`  ${team}: ${rating.toFixed(0)} ± ${rd.toFixed(0)}`);
  }

  console.log("\n=== Recent Form (Last 5 matches) ===");
  for (const [team, form] of Object.entries(context.teamRecentForm)) {
    const winRate =
      form.matches > 0 ? ((form.wins / form.matches) * 100).toFixed(0) : "N/A";
    console.log(`  ${team}: ${form.wins}/${form.matches} (${winRate}%)`);
  }

  console.log("\n=== Schedule Context ===");
  for (const [team, sched] of Object.entries(context.teamSchedule)) {
    console.log(
      `  ${team}: ${sched.daysSincePreviousMatch ?? "N/A"} days rest, ${sched.matchesInPrevious7Days} matches in 7d`,
    );
  }
}
