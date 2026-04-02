import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/index.js";
import {
  buildFeatureContextFromHistory,
  ELO_SEASON_CARRYOVER,
} from "../../src/features/context-builder.js";
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

const CARRYOVER_GRID = [0.35, 0.45, 0.55, 0.65, 0.75, 0.85];

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const matches = await loadCompletedMatches(pool, options.testSeasonTo);
    const candidates = [] as Array<{
      carryover: number;
      validation: ReturnType<typeof calculateProbabilityMetrics>;
      test: ReturnType<typeof calculateProbabilityMetrics>;
    }>;

    for (const carryover of CARRYOVER_GRID) {
      const rows = await buildRows(pool, matches, carryover);
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
      candidates.push({
        carryover,
        validation: calculateProbabilityMetrics(
          validationRows,
          (row) => row.primaryProbability,
          10,
        ),
        test: calculateProbabilityMetrics(
          testRows,
          (row) => row.primaryProbability,
          10,
        ),
      });
    }

    candidates.sort((a, b) => a.validation.logLoss - b.validation.logLoss);
    const best = candidates[0];
    const baseline = candidates.find(
      (entry) => entry.carryover === ELO_SEASON_CARRYOVER,
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          trainSeasons: {
            from: options.trainSeasonFrom,
            to: options.trainSeasonTo,
          },
          validationSeasons: {
            from: options.validationSeasonFrom,
            to: options.validationSeasonTo,
          },
          testSeasons: {
            from: options.testSeasonFrom,
            to: options.testSeasonTo,
          },
          baselineCarryover: ELO_SEASON_CARRYOVER,
          bestCarryover: best?.carryover ?? null,
          bestValidation: best?.validation ?? null,
          bestTest: best?.test ?? null,
          baselineValidation: baseline?.validation ?? null,
          baselineTest: baseline?.test ?? null,
          candidates: candidates.map((entry) => ({
            carryover: entry.carryover,
            validation: entry.validation,
            test: entry.test,
          })),
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
  matches: CompletedMatchRow[],
  carryover: number,
): Promise<HistoricalPredictionRow[]> {
  const rows: HistoricalPredictionRow[] = [];

  for (const match of matches) {
    const scheduledStart = new Date(match.scheduled_start);
    const checkpoint = parseCanonicalCheckpoint({
      checkpointType: "pre_match",
      match: {
        competition: "IPL",
        matchSlug: match.match_slug,
        sourceMatchId: null,
        season: match.season,
        scheduledStart: scheduledStart.toISOString(),
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
        snapshotTime: new Date(scheduledStart.getTime() - 60_000).toISOString(),
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
        statePayload: { source: "tune-elo-carryover" },
      },
    });

    const context = await buildFeatureContextFromHistory(pool, scheduledStart, {
      eloSeasonCarryover: carryover,
    });
    const featureRow = buildBaselinePreMatchFeatureRow(checkpoint, context);
    const score = scoreBaselineIplPreMatch(featureRow, {
      weights: DEFAULT_MODEL_WEIGHTS,
    });
    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;
    if (actualOutcome === null) {
      continue;
    }

    rows.push({
      modelKey: `elo-carryover-${carryover}`,
      checkpointType: "pre_match",
      modelScoreId: rows.length + 1,
      matchSlug: match.match_slug,
      season: match.season,
      snapshotTime: featureRow.generatedAt,
      actualOutcome,
      positiveClassLabel: match.team_a_name,
      negativeClassLabel: match.team_b_name,
      primaryProbability: score.teamAWinProbability,
      socialOnProbability: score.teamAWinProbability,
      socialOffProbability: score.teamAWinProbability,
      socialSupported: false,
      marketImpliedProbability: null,
      provenance: { source: "tune-elo-carryover" },
    });
  }

  return rows;
}

async function loadCompletedMatches(
  pool: ReturnType<typeof createPgPool>,
  seasonTo: number,
): Promise<CompletedMatchRow[]> {
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

  return result.rows;
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
    throw new Error(
      "Missing required arguments: --train-from, --train-to, --validation-from, --validation-to, --test-from, --test-to",
    );
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
  console.error(`Elo carryover tuning failed: ${message}`);
  process.exitCode = 1;
});
