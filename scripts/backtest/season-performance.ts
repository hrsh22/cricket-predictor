import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import {
  buildBaselinePreMatchFeatureRow,
  type PreMatchFeatureContext,
} from "../../src/features/index.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import { scoreBaselineIplPreMatch } from "../../src/models/base/index.js";
import {
  applyPlattCalibration,
  calculateCalibrationSummary,
  calculateProbabilityMetrics,
  fitPlattCalibration,
  type HistoricalPredictionRow,
} from "../../src/backtest/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  season: number | null;
  binCount: number;
  calibrationTrainSeasonFrom: number | null;
  calibrationTrainSeasonTo: number | null;
}

interface SeasonRow {
  season: number;
  total_matches: string | number;
  completed_wins: string | number;
}

interface MatchRow {
  match_slug: string;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  winning_team_name: string;
}

interface MatchPredictionSummaryRow {
  index: number;
  date: string;
  matchSlug: string;
  teamA: string;
  teamB: string;
  predictedWinner: string;
  predictedWinnerProbability: number;
  teamAWinProbability: number;
  teamBWinProbability: number;
  actualWinner: string;
  correct: boolean;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const season = options.season ?? (await resolveLatestCompletedSeason(pool));
    const matches = await loadSeasonMatches(pool, season);
    const calibrationWindow = resolveCalibrationTrainingWindow({
      season,
      cliFrom: options.calibrationTrainSeasonFrom,
      cliTo: options.calibrationTrainSeasonTo,
    });

    const calibrationTrainingRows = await loadSeasonMatchesInRange(
      pool,
      calibrationWindow.from,
      calibrationWindow.to,
    );

    const report = await buildSeasonReport(
      pool,
      matches,
      options.binCount,
      calibrationTrainingRows,
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          season,
          totalMatches: report.rows.length,
          correctPredictions: report.correctPredictions,
          incorrectPredictions: report.rows.length - report.correctPredictions,
          accuracy: report.accuracy,
          metrics: report.metrics,
          calibratedMetrics: report.calibratedMetrics,
          improvement: {
            logLossDelta: roundTo(
              report.calibratedMetrics.logLoss - report.metrics.logLoss,
              6,
            ),
            brierScoreDelta: roundTo(
              report.calibratedMetrics.brierScore - report.metrics.brierScore,
              6,
            ),
            calibrationErrorDelta: roundTo(
              report.calibratedMetrics.calibrationError -
                report.metrics.calibrationError,
              6,
            ),
            accuracyDelta: roundTo(
              report.calibratedMetrics.accuracy - report.metrics.accuracy,
              6,
            ),
          },
          plattCalibration: report.plattCalibration,
          calibrationTrainingWindow: calibrationWindow,
          calibrationTrainingSampleSize: calibrationTrainingRows.length,
          calibration: report.calibration,
          calibratedCalibration: report.calibratedCalibration,
          top10ConfidentPredictions: report.top10ConfidentPredictions,
          top10HighConfidenceMisses: report.top10HighConfidenceMisses,
          rows: report.rows,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function resolveLatestCompletedSeason(
  pool: ReturnType<typeof createPgPool>,
): Promise<number> {
  const result = await pool.query<SeasonRow>(
    `
      select
        season,
        count(*) as total_matches,
        count(*) filter (
          where status = 'completed'
            and result_type = 'win'
            and winning_team_name is not null
        ) as completed_wins
      from canonical_matches
      where competition = 'IPL'
      group by season
      order by season desc
    `,
  );

  for (const row of result.rows) {
    const totalMatches = Number(row.total_matches);
    const completedWins = Number(row.completed_wins);

    if (totalMatches > 0 && totalMatches === completedWins) {
      return row.season;
    }
  }

  throw new Error("No fully completed IPL season found in canonical_matches.");
}

async function loadSeasonMatches(
  pool: ReturnType<typeof createPgPool>,
  season: number,
): Promise<MatchRow[]> {
  const result = await pool.query<MatchRow>(
    `
      select
        match_slug,
        season,
        scheduled_start,
        team_a_name,
        team_b_name,
        venue_name,
        winning_team_name
      from canonical_matches
      where competition = 'IPL'
        and season = $1
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
      order by scheduled_start asc, id asc
    `,
    [season],
  );

  return result.rows;
}

async function loadSeasonMatchesInRange(
  pool: ReturnType<typeof createPgPool>,
  seasonFrom: number,
  seasonTo: number,
): Promise<MatchRow[]> {
  const result = await pool.query<MatchRow>(
    `
      select
        match_slug,
        season,
        scheduled_start,
        team_a_name,
        team_b_name,
        venue_name,
        winning_team_name
      from canonical_matches
      where competition = 'IPL'
        and season between $1 and $2
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
      order by scheduled_start asc, id asc
    `,
    [seasonFrom, seasonTo],
  );

  return result.rows;
}

async function buildSeasonReport(
  pool: ReturnType<typeof createPgPool>,
  matches: readonly MatchRow[],
  binCount: number,
  calibrationTrainingRows: readonly MatchRow[],
): Promise<{
  rows: MatchPredictionSummaryRow[];
  correctPredictions: number;
  accuracy: number;
  metrics: ReturnType<typeof calculateProbabilityMetrics>;
  calibratedMetrics: ReturnType<typeof calculateProbabilityMetrics>;
  calibration: ReturnType<typeof calculateCalibrationSummary>;
  calibratedCalibration: ReturnType<typeof calculateCalibrationSummary>;
  plattCalibration: ReturnType<typeof fitPlattCalibration>;
  top10ConfidentPredictions: MatchPredictionSummaryRow[];
  top10HighConfidenceMisses: MatchPredictionSummaryRow[];
}> {
  const rows: MatchPredictionSummaryRow[] = [];
  const historicalRows: HistoricalPredictionRow[] = [];

  for (const [index, match] of matches.entries()) {
    const asOfDate = new Date(match.scheduled_start);
    const context: PreMatchFeatureContext =
      await buildFeatureContextFromHistory(pool, asOfDate);

    const checkpoint = parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: {
        competition: "IPL",
        matchSlug: match.match_slug,
        sourceMatchId: null,
        season: match.season,
        scheduledStart: asOfDate.toISOString(),
        teamAName: match.team_a_name,
        teamBName: match.team_b_name,
        venueName: match.venue_name,
        status: "scheduled",
        tossWinnerTeamName: null,
        tossDecision: null,
        winningTeamName: null,
        resultType: null,
      },
      state: {
        matchSlug: match.match_slug,
        checkpointType: "pre_match",
        snapshotTime: new Date(asOfDate.getTime() - 60_000).toISOString(),
        stateVersion: 1,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload: {
          source: "season-performance-report",
          generated: true,
        },
      },
    });

    const featureRow = buildBaselinePreMatchFeatureRow(checkpoint, context);
    const score = scoreBaselineIplPreMatch(featureRow);

    const teamAProbability = Number(score.teamAWinProbability.toFixed(6));
    const teamBProbability = Number((1 - score.teamAWinProbability).toFixed(6));

    const predictedWinner =
      score.teamAWinProbability >= 0.5 ? match.team_a_name : match.team_b_name;
    const predictedWinnerProbability = Number(
      Math.max(teamAProbability, teamBProbability).toFixed(6),
    );

    const actualWinner = match.winning_team_name;
    const correct = predictedWinner === actualWinner;

    rows.push({
      index: index + 1,
      date: new Date(match.scheduled_start).toISOString().slice(0, 10),
      matchSlug: match.match_slug,
      teamA: match.team_a_name,
      teamB: match.team_b_name,
      predictedWinner,
      predictedWinnerProbability,
      teamAWinProbability: teamAProbability,
      teamBWinProbability: teamBProbability,
      actualWinner,
      correct,
    });

    const actualOutcome = actualWinner === match.team_a_name ? 1 : 0;
    historicalRows.push({
      modelKey: "walk-forward-pre-match-v2",
      checkpointType: "pre_match",
      modelScoreId: index + 1,
      matchSlug: match.match_slug,
      season: match.season,
      snapshotTime: checkpoint.state.snapshotTime,
      actualOutcome,
      positiveClassLabel: match.team_a_name,
      negativeClassLabel: match.team_b_name,
      primaryProbability: teamAProbability,
      socialOnProbability: teamAProbability,
      socialOffProbability: teamAProbability,
      socialSupported: false,
      marketImpliedProbability: null,
      provenance: {
        source: "season-performance-report",
        asOfDate: asOfDate.toISOString(),
      },
    });
  }

  const correctPredictions = rows.filter((row) => row.correct).length;
  const accuracy =
    rows.length === 0
      ? 0
      : Number((correctPredictions / rows.length).toFixed(6));

  const plattCalibration = await fitCalibrationModel(
    pool,
    calibrationTrainingRows,
  );

  const metrics = calculateProbabilityMetrics(
    historicalRows,
    (row) => row.primaryProbability,
    binCount,
  );

  const calibratedMetrics = calculateProbabilityMetrics(
    historicalRows,
    (row) => applyPlattCalibration(row.primaryProbability, plattCalibration),
    binCount,
  );

  const calibration = calculateCalibrationSummary(
    historicalRows,
    (row) => row.primaryProbability,
    binCount,
  );
  const calibratedCalibration = calculateCalibrationSummary(
    historicalRows,
    (row) => applyPlattCalibration(row.primaryProbability, plattCalibration),
    binCount,
  );

  const top10ConfidentPredictions = [...rows]
    .sort(
      (left, right) =>
        right.predictedWinnerProbability - left.predictedWinnerProbability,
    )
    .slice(0, 10);

  const top10HighConfidenceMisses = [...rows]
    .filter((row) => !row.correct)
    .sort(
      (left, right) =>
        right.predictedWinnerProbability - left.predictedWinnerProbability,
    )
    .slice(0, 10);

  return {
    rows,
    correctPredictions,
    accuracy,
    metrics,
    calibratedMetrics,
    calibration,
    calibratedCalibration,
    plattCalibration,
    top10ConfidentPredictions,
    top10HighConfidenceMisses,
  };
}

async function fitCalibrationModel(
  pool: ReturnType<typeof createPgPool>,
  matches: readonly MatchRow[],
) {
  const trainingRows: Array<{ probability: number; outcome: 0 | 1 }> = [];

  for (const match of matches) {
    const asOfDate = new Date(match.scheduled_start);
    const context: PreMatchFeatureContext =
      await buildFeatureContextFromHistory(pool, asOfDate);

    const checkpoint = parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: {
        competition: "IPL",
        matchSlug: match.match_slug,
        sourceMatchId: null,
        season: match.season,
        scheduledStart: asOfDate.toISOString(),
        teamAName: match.team_a_name,
        teamBName: match.team_b_name,
        venueName: match.venue_name,
        status: "scheduled",
        tossWinnerTeamName: null,
        tossDecision: null,
        winningTeamName: null,
        resultType: null,
      },
      state: {
        matchSlug: match.match_slug,
        checkpointType: "pre_match",
        snapshotTime: new Date(asOfDate.getTime() - 60_000).toISOString(),
        stateVersion: 1,
        sourceMarketSnapshotId: null,
        sourceCricketSnapshotId: null,
        inningsNumber: null,
        battingTeamName: null,
        bowlingTeamName: null,
        runs: null,
        wickets: null,
        overs: null,
        targetRuns: null,
        currentRunRate: null,
        requiredRunRate: null,
        statePayload: {
          source: "season-performance-calibration-train",
          generated: true,
        },
      },
    });

    const featureRow = buildBaselinePreMatchFeatureRow(checkpoint, context);
    const score = scoreBaselineIplPreMatch(featureRow);
    const outcome = match.winning_team_name === match.team_a_name ? 1 : 0;
    trainingRows.push({
      probability: Number(score.teamAWinProbability.toFixed(6)),
      outcome,
    });
  }

  return fitPlattCalibration(trainingRows);
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season: number | null = null;
  let binCount = 10;
  let calibrationTrainSeasonFrom: number | null = null;
  let calibrationTrainSeasonTo: number | null = null;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--season") {
      season = parseIntegerArg("--season", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--bin-count") {
      binCount = parseIntegerArg("--bin-count", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--calibration-train-season-from") {
      calibrationTrainSeasonFrom = parseIntegerArg(
        "--calibration-train-season-from",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    if (argument === "--calibration-train-season-to") {
      calibrationTrainSeasonTo = parseIntegerArg(
        "--calibration-train-season-to",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --season <year>, optional --bin-count <n>, --calibration-train-season-from <year>, --calibration-train-season-to <year>.`,
    );
  }

  return {
    season,
    binCount,
    calibrationTrainSeasonFrom,
    calibrationTrainSeasonTo,
  };
}

function resolveCalibrationTrainingWindow(input: {
  season: number;
  cliFrom: number | null;
  cliTo: number | null;
}): { from: number; to: number } {
  const defaultFrom = Math.max(1, input.season - 2);
  const defaultTo = input.season - 1;

  const from = input.cliFrom ?? defaultFrom;
  const to = input.cliTo ?? defaultTo;

  if (to >= input.season) {
    throw new Error(
      "Calibration training season range must end before evaluation season.",
    );
  }

  if (from > to) {
    throw new Error(
      "Calibration training season range must satisfy from <= to.",
    );
  }

  return { from, to };
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Season performance report failed: ${message}`);
  process.exitCode = 1;
});
