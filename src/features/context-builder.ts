import type { Pool } from "pg";

import type { PreMatchFeatureContext } from "../features/pre-match.js";
import {
  createVenueTossDecisionKey,
  createHeadToHeadKey,
  createTeamSeasonKey,
  createVenueStrengthKey,
} from "../features/pre-match.js";
import { computeGlicko2Ratings, type Glicko2Rating } from "../ratings/index.js";

const DEFAULT_ELO = 1500;
const DEFAULT_RD = 300;
const ELO_K_FACTOR = 32;
export const ELO_SEASON_CARRYOVER = 0.65;

interface MatchResult {
  season: number;
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

interface TeamSeasonStats {
  [key: string]: { wins: number; matchesPlayed: number };
}

interface TeamLineupMatch {
  teamName: string;
  resultKnownAt: Date;
  players: string[];
}

interface TeamRoleLineupMatch {
  teamName: string;
  resultKnownAt: Date;
  roles: string[];
}

export async function buildFeatureContextFromHistory(
  pool: Pool,
  asOfDate: Date,
  options?: {
    eloSeasonCarryover?: number;
  },
): Promise<PreMatchFeatureContext> {
  const matches = await fetchCompletedMatches(pool, asOfDate);

  const eloRatings = computeEloRatings(
    matches,
    asOfDate,
    options?.eloSeasonCarryover ?? ELO_SEASON_CARRYOVER,
  );
  const teamRatingDeviations: Record<string, number> = {};
  for (const team of Object.keys(eloRatings)) {
    teamRatingDeviations[team] = DEFAULT_RD;
  }

  const teamForm = computeRecentForm(matches, asOfDate, 8);
  const venueStrength = computeVenueStrength(matches);
  const headToHeadStrength = computeHeadToHeadStrength(matches);
  const venueTossDecisionWinRate = computeVenueTossDecisionWinRate(matches);
  const scheduleContext = computeScheduleContext(matches, asOfDate);
  const teamSeasonContext = computeTeamSeasonContext(matches, asOfDate);
  const teamLineupContext = await computeTeamLineupContext(pool, asOfDate);
  const teamRoleCompositionContext = await computeTeamRoleCompositionContext(
    pool,
    asOfDate,
  );

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
    teamLineupContext,
    teamRoleCompositionContext,
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
    teamSeasonContext,
  };
}

function computeEloRatings(
  matches: MatchResult[],
  asOfDate: Date,
  seasonCarryover: number,
): Record<string, number> {
  const ratings: Record<string, number> = {};
  let activeSeason: number | null = null;

  for (const match of matches) {
    if (match.winner === null) continue;

    if (activeSeason !== null && match.season !== activeSeason) {
      regressRatingsTowardMean(ratings, seasonCarryover);
    }
    activeSeason = match.season;

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

function regressRatingsTowardMean(
  ratings: Record<string, number>,
  carryover: number,
): void {
  for (const team of Object.keys(ratings)) {
    const currentRating = ratings[team];
    if (currentRating === undefined) {
      continue;
    }

    ratings[team] = Number(
      (DEFAULT_ELO + (currentRating - DEFAULT_ELO) * carryover).toFixed(6),
    );
  }
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
    season: number;
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
        cm.season,
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
    season: row.season,
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

function computeTeamSeasonContext(
  matches: MatchResult[],
  asOfDate: Date,
): Record<
  string,
  { wins: number; matchesPlayed: number; weightedWinRate: number }
> {
  const seasonStats: TeamSeasonStats = {};

  for (const match of matches) {
    if (match.resultKnownAt >= asOfDate || match.winner === null) {
      continue;
    }

    const teamAKey = createTeamSeasonKey(match.teamA, match.season);
    const teamBKey = createTeamSeasonKey(match.teamB, match.season);

    if (seasonStats[teamAKey] === undefined) {
      seasonStats[teamAKey] = { wins: 0, matchesPlayed: 0 };
    }
    if (seasonStats[teamBKey] === undefined) {
      seasonStats[teamBKey] = { wins: 0, matchesPlayed: 0 };
    }

    seasonStats[teamAKey].matchesPlayed += 1;
    seasonStats[teamBKey].matchesPlayed += 1;

    if (match.winner === match.teamA) {
      seasonStats[teamAKey].wins += 1;
    } else if (match.winner === match.teamB) {
      seasonStats[teamBKey].wins += 1;
    }
  }

  const normalized: Record<
    string,
    { wins: number; matchesPlayed: number; weightedWinRate: number }
  > = {};

  for (const [key, stats] of Object.entries(seasonStats)) {
    const rawWinRate =
      stats.matchesPlayed <= 0 ? 0.5 : stats.wins / stats.matchesPlayed;
    const weightedWinRate = bayesianShrink(
      rawWinRate,
      stats.matchesPlayed,
      0.5,
      4,
    );
    normalized[key] = {
      wins: stats.wins,
      matchesPlayed: stats.matchesPlayed,
      weightedWinRate: Number(weightedWinRate.toFixed(6)),
    };
  }

  return normalized;
}

async function computeTeamLineupContext(
  pool: Pool,
  asOfDate: Date,
): Promise<
  Record<
    string,
    { matches: number; stability: number; continuity: number; rotation: number }
  >
> {
  const result = await pool.query<{
    team_name: string;
    result_known_at: Date;
    player_registry_id: string | number | null;
    source_player_name: string;
    lineup_order: number;
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
        mpa.team_name,
        COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) AS result_known_at,
        mpa.player_registry_id,
        mpa.source_player_name,
        mpa.lineup_order
      FROM match_player_appearances mpa
      JOIN canonical_matches cm ON cm.id = mpa.canonical_match_id
      LEFT JOIN first_completed_snapshot fcs
        ON fcs.source_match_id = cm.source_match_id
      WHERE cm.competition = 'IPL'
        AND cm.status = 'completed'
        AND COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) < $1
      ORDER BY mpa.team_name ASC, result_known_at ASC, mpa.lineup_order ASC
    `,
    [asOfDate],
  );

  const byTeam = new Map<string, TeamLineupMatch[]>();
  const lineupIndex = new Map<string, TeamLineupMatch>();

  for (const row of result.rows) {
    const lineupKey = `${row.team_name}::${row.result_known_at.toISOString()}`;
    let lineup = lineupIndex.get(lineupKey);
    if (lineup === undefined) {
      lineup = {
        teamName: row.team_name,
        resultKnownAt: row.result_known_at,
        players: [],
      };
      lineupIndex.set(lineupKey, lineup);
      const current = byTeam.get(row.team_name) ?? [];
      current.push(lineup);
      byTeam.set(row.team_name, current);
    }

    const playerKey =
      row.player_registry_id === null
        ? `name:${row.source_player_name.toLowerCase()}`
        : `id:${row.player_registry_id}`;
    lineup.players.push(playerKey);
  }

  const output: Record<
    string,
    { matches: number; stability: number; continuity: number; rotation: number }
  > = {};
  for (const [team, lineups] of byTeam.entries()) {
    output[team] = summarizeTeamLineups(lineups.slice(-5));
  }

  return output;
}

async function computeTeamRoleCompositionContext(
  pool: Pool,
  asOfDate: Date,
): Promise<
  Record<
    string,
    { matches: number; bowlerShare: number; allRounderShare: number }
  >
> {
  const result = await pool.query<{
    team_name: string;
    result_known_at: Date;
    player_role: string | null;
    lineup_order: number;
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
        mpa.team_name,
        COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) AS result_known_at,
        pr.player_role,
        mpa.lineup_order
      FROM match_player_appearances mpa
      JOIN canonical_matches cm ON cm.id = mpa.canonical_match_id
      LEFT JOIN first_completed_snapshot fcs
        ON fcs.source_match_id = cm.source_match_id
      LEFT JOIN player_registry pr ON pr.id = mpa.player_registry_id
      WHERE cm.competition = 'IPL'
        AND cm.status = 'completed'
        AND COALESCE(
          fcs.result_known_at,
          cm.scheduled_start + INTERVAL '12 hours'
        ) < $1
      ORDER BY mpa.team_name ASC, result_known_at ASC, mpa.lineup_order ASC
    `,
    [asOfDate],
  );

  const byTeam = new Map<string, TeamRoleLineupMatch[]>();
  const lineupIndex = new Map<string, TeamRoleLineupMatch>();

  for (const row of result.rows) {
    const lineupKey = `${row.team_name}::${row.result_known_at.toISOString()}`;
    let lineup = lineupIndex.get(lineupKey);
    if (lineup === undefined) {
      lineup = {
        teamName: row.team_name,
        resultKnownAt: row.result_known_at,
        roles: [],
      };
      lineupIndex.set(lineupKey, lineup);
      const current = byTeam.get(row.team_name) ?? [];
      current.push(lineup);
      byTeam.set(row.team_name, current);
    }

    lineup.roles.push(row.player_role ?? "unknown");
  }

  const output: Record<
    string,
    { matches: number; bowlerShare: number; allRounderShare: number }
  > = {};
  for (const [team, lineups] of byTeam.entries()) {
    output[team] = summarizeRoleComposition(lineups.slice(-5));
  }

  return output;
}

function summarizeRoleComposition(lineups: TeamRoleLineupMatch[]): {
  matches: number;
  bowlerShare: number;
  allRounderShare: number;
} {
  if (lineups.length === 0) {
    return { matches: 0, bowlerShare: 0.35, allRounderShare: 0.25 };
  }

  let totalBowlerShare = 0;
  let totalAllRounderShare = 0;
  for (const lineup of lineups) {
    const total = Math.max(1, lineup.roles.length);
    const bowlers = lineup.roles.filter((role) => role === "bowler").length;
    const allRounders = lineup.roles.filter(
      (role) => role === "all_rounder",
    ).length;
    totalBowlerShare += bowlers / total;
    totalAllRounderShare += allRounders / total;
  }

  return {
    matches: lineups.length,
    bowlerShare: Number((totalBowlerShare / lineups.length).toFixed(6)),
    allRounderShare: Number((totalAllRounderShare / lineups.length).toFixed(6)),
  };
}

function summarizeTeamLineups(lineups: TeamLineupMatch[]): {
  matches: number;
  stability: number;
  continuity: number;
  rotation: number;
} {
  if (lineups.length < 2) {
    return {
      matches: lineups.length,
      stability: 0.5,
      continuity: 0.5,
      rotation: 0.5,
    };
  }

  const uniqueLineups = lineups.map((lineup) =>
    Array.from(new Set(lineup.players)),
  );
  let totalJaccard = 0;
  let comparisons = 0;

  for (let index = 1; index < uniqueLineups.length; index += 1) {
    const previous = uniqueLineups[index - 1];
    const current = uniqueLineups[index];
    if (previous === undefined || current === undefined) {
      continue;
    }
    totalJaccard += jaccard(previous, current);
    comparisons += 1;
  }

  const latest = uniqueLineups[uniqueLineups.length - 1] ?? [];
  const prior = uniqueLineups.slice(0, -1);
  let continuityHits = 0;
  for (const player of latest) {
    if (prior.some((lineup) => lineup.includes(player))) {
      continuityHits += 1;
    }
  }

  const allRecentPlayers = new Set(uniqueLineups.flat());
  const averageLineupSize =
    uniqueLineups.reduce((sum, lineup) => sum + lineup.length, 0) /
    uniqueLineups.length;
  const rotation =
    averageLineupSize <= 0
      ? 0.5
      : Math.min(
          1,
          Math.max(
            0,
            (allRecentPlayers.size - averageLineupSize) / averageLineupSize,
          ),
        );

  return {
    matches: uniqueLineups.length,
    stability: Number((totalJaccard / Math.max(1, comparisons)).toFixed(6)),
    continuity: Number(
      (continuityHits / Math.max(1, latest.length)).toFixed(6),
    ),
    rotation: Number(rotation.toFixed(6)),
  };
}

function jaccard(left: string[], right: string[]): number {
  const leftSet = new Set(left);
  const rightSet = new Set(right);
  const union = new Set([...leftSet, ...rightSet]);
  if (union.size === 0) {
    return 0.5;
  }

  let intersection = 0;
  for (const value of leftSet) {
    if (rightSet.has(value)) {
      intersection += 1;
    }
  }

  return intersection / union.size;
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
