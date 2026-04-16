import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/pre-match.js";
import {
  DEFAULT_MODEL_WEIGHTS,
  scoreBaselineIplPreMatch,
} from "../../src/models/base/index.js";
import { createPgPool, closePgPool } from "../../src/repositories/postgres.js";
import { calculateProbabilityMetrics } from "../../src/backtest/metrics.js";
import type { HistoricalPredictionRow } from "../../src/backtest/types.js";

interface CliOptions {
  trainSeasonFrom: number;
  trainSeasonTo: number;
  validationSeasonFrom: number;
  validationSeasonTo: number;
  testSeasonFrom: number;
  testSeasonTo: number;
}

interface CompletedMatchRow {
  match_slug: string;
  season: number;
  scheduled_start: Date;
  team_a_name: string;
  team_b_name: string;
  venue_name: string | null;
  winning_team_name: string;
}

const GRID = [0, 0.2, 0.4, 0.6, 0.8];

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const rows = await buildRows(pool, options.testSeasonTo);
    const validationRows = rows.filter(
      (row) =>
        row.season >= options.validationSeasonFrom &&
        row.season <= options.validationSeasonTo,
    );
    const testRows = rows.filter(
      (row) =>
        row.season >= options.testSeasonFrom &&
        row.season <= options.testSeasonTo,
    );

    let best: {
      weights: typeof DEFAULT_MODEL_WEIGHTS;
      validation: ReturnType<typeof calculateProbabilityMetrics>;
    } | null = null;

    for (const leftBatVsOppSpin of GRID) {
      for (const leftBatVsOppPace of GRID) {
        const weights = {
          ...DEFAULT_MODEL_WEIGHTS,
          leftBatVsOppSpin,
          leftBatVsOppPace,
        };
        const validationPredictions = toHistoricalRows(validationRows, weights);
        const validationMetrics = calculateProbabilityMetrics(
          validationPredictions,
          (row) => row.primaryProbability,
          10,
        );

        if (
          best === null ||
          validationMetrics.logLoss < best.validation.logLoss
        ) {
          best = {
            weights,
            validation: validationMetrics,
          };
        }
      }
    }

    const bestWeights = best?.weights ?? DEFAULT_MODEL_WEIGHTS;
    const baselineTest = calculateProbabilityMetrics(
      toHistoricalRows(testRows, DEFAULT_MODEL_WEIGHTS),
      (row) => row.primaryProbability,
      10,
    );
    const bestTest = calculateProbabilityMetrics(
      toHistoricalRows(testRows, bestWeights),
      (row) => row.primaryProbability,
      10,
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          bestWeights,
          validation: best?.validation ?? null,
          test: bestTest,
          baselineTest,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function buildRows(
  pool: ReturnType<typeof createPgPool>,
  seasonTo: number,
): Promise<
  Array<{
    season: number;
    featureRow: ReturnType<typeof buildBaselinePreMatchFeatureRow>;
    actualOutcome: 0 | 1;
  }>
> {
  const result = await pool.query<CompletedMatchRow>(
    `
      select match_slug, season, scheduled_start, team_a_name, team_b_name, venue_name, winning_team_name
      from canonical_matches
      where competition = 'IPL'
        and season <= $1
        and status = 'completed'
        and result_type = 'win'
        and winning_team_name is not null
      order by scheduled_start asc, id asc
    `,
    [seasonTo],
  );

  const rows: Array<{
    season: number;
    featureRow: ReturnType<typeof buildBaselinePreMatchFeatureRow>;
    actualOutcome: 0 | 1;
  }> = [];

  for (const match of result.rows) {
    const asOfDate = new Date(match.scheduled_start);
    const context = await buildFeatureContextFromHistory(pool, asOfDate);
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
        statePayload: { source: "tune-style-composition" },
      },
    });
    const featureRow = buildBaselinePreMatchFeatureRow(checkpoint, context);
    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;
    if (actualOutcome === null) {
      continue;
    }

    rows.push({ season: match.season, featureRow, actualOutcome });
  }

  return rows;
}

function toHistoricalRows(
  rows: Array<{
    season: number;
    featureRow: ReturnType<typeof buildBaselinePreMatchFeatureRow>;
    actualOutcome: 0 | 1;
  }>,
  weights: typeof DEFAULT_MODEL_WEIGHTS,
): HistoricalPredictionRow[] {
  return rows.map((row, index) => {
    const score = scoreBaselineIplPreMatch(row.featureRow, { weights });
    return {
      modelKey: "tune-style-composition",
      checkpointType: "pre_match",
      modelScoreId: index + 1,
      matchSlug: row.featureRow.matchSlug,
      season: row.season,
      snapshotTime: row.featureRow.generatedAt,
      actualOutcome: row.actualOutcome,
      positiveClassLabel: String(row.featureRow.features["teamAName"]),
      negativeClassLabel: String(row.featureRow.features["teamBName"]),
      primaryProbability: score.teamAWinProbability,
      socialOnProbability: score.teamAWinProbability,
      socialOffProbability: score.teamAWinProbability,
      socialSupported: false,
      marketImpliedProbability: null,
      provenance: { source: "tune-style-composition" },
    };
  });
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let trainSeasonFrom: number | null = null;
  let trainSeasonTo: number | null = null;
  let validationSeasonFrom: number | null = null;
  let validationSeasonTo: number | null = null;
  let testSeasonFrom: number | null = null;
  let testSeasonTo: number | null = null;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const next = argv[index + 1];
    if (argument === "--train-from") {
      trainSeasonFrom = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    if (argument === "--train-to") {
      trainSeasonTo = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    if (argument === "--validation-from") {
      validationSeasonFrom = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    if (argument === "--validation-to") {
      validationSeasonTo = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    if (argument === "--test-from") {
      testSeasonFrom = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    if (argument === "--test-to") {
      testSeasonTo = parseIntegerArg(argument, next);
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --train-from, --train-to, --validation-from, --validation-to, --test-from, --test-to.`,
    );
  }

  if (
    trainSeasonFrom === null ||
    trainSeasonTo === null ||
    validationSeasonFrom === null ||
    validationSeasonTo === null ||
    testSeasonFrom === null ||
    testSeasonTo === null
  ) {
    throw new Error("Missing required arguments for style composition tuning.");
  }

  return {
    trainSeasonFrom,
    trainSeasonTo,
    validationSeasonFrom,
    validationSeasonTo,
    testSeasonFrom,
    testSeasonTo,
  };
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
  console.error(`Style composition tuning failed: ${message}`);
  process.exitCode = 1;
});
