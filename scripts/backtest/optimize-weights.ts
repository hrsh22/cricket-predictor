import { loadAppConfig } from "../../src/config/index.js";
import { parseCanonicalCheckpoint } from "../../src/domain/checkpoint.js";
import { buildBaselinePreMatchFeatureRow } from "../../src/features/index.js";
import { buildFeatureContextFromHistory } from "../../src/features/context-builder.js";
import {
  scoreBaselineIplPreMatch,
  DEFAULT_MODEL_WEIGHTS,
  type ModelWeights,
} from "../../src/models/base/index.js";
import { createPgPool, closePgPool } from "../../src/repositories/postgres.js";
import { calculateProbabilityMetrics } from "../../src/backtest/metrics.js";
import type { HistoricalPredictionRow } from "../../src/backtest/types.js";

interface CliOptions {
  trainSeasonFrom: number;
  trainSeasonTo: number;
  testSeasonFrom: number;
  testSeasonTo: number;
  gridResolution: number;
  metric: "logLoss" | "brierScore" | "calibrationError";
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

interface WeightCandidate {
  weights: ModelWeights;
  trainMetrics: {
    logLoss: number;
    brierScore: number;
    calibrationError: number;
    accuracy: number;
    sampleSize: number;
  };
}

interface OptimizationResult {
  bestWeights: ModelWeights;
  defaultWeights: ModelWeights;
  trainSeasons: { from: number; to: number };
  testSeasons: { from: number; to: number };
  optimizationMetric: string;
  gridResolution: number;
  totalCandidatesEvaluated: number;
  trainResults: {
    best: WeightCandidate["trainMetrics"];
    default: WeightCandidate["trainMetrics"];
    improvement: {
      logLoss: number;
      brierScore: number;
      calibrationError: number;
    };
  };
  testResults: {
    best: WeightCandidate["trainMetrics"];
    default: WeightCandidate["trainMetrics"];
    improvement: {
      logLoss: number;
      brierScore: number;
      calibrationError: number;
    };
  };
  topCandidates: Array<{
    weights: ModelWeights;
    trainLogLoss: number;
    testLogLoss: number;
  }>;
}

const OPTIMIZATION_MODEL_KEY = "weight-optimization";

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    console.error(
      `Loading matches for seasons ${options.trainSeasonFrom}-${options.testSeasonTo}...`,
    );
    const allMatches = await loadCompletedMatches(pool, options.testSeasonTo);
    console.error(`Loaded ${allMatches.length} completed matches.`);

    console.error("Building feature contexts for all matches...");
    const matchContexts = await buildAllMatchContexts(pool, allMatches);
    console.error(`Built contexts for ${matchContexts.length} matches.`);

    const trainMatches = matchContexts.filter(
      (m) =>
        m.season >= options.trainSeasonFrom &&
        m.season <= options.trainSeasonTo,
    );
    const testMatches = matchContexts.filter(
      (m) =>
        m.season >= options.testSeasonFrom && m.season <= options.testSeasonTo,
    );

    console.error(
      `Train set: ${trainMatches.length} matches (${options.trainSeasonFrom}-${options.trainSeasonTo})`,
    );
    console.error(
      `Test set: ${testMatches.length} matches (${options.testSeasonFrom}-${options.testSeasonTo})`,
    );

    console.error(
      `Generating weight grid with resolution ${options.gridResolution}...`,
    );
    const weightGrid = generateWeightGrid(options.gridResolution);
    console.error(`Generated ${weightGrid.length} weight combinations.`);

    console.error(`Evaluating candidates on training set...`);
    const candidates = evaluateCandidates(trainMatches, weightGrid);

    const sortedCandidates = [...candidates].sort(
      (a, b) => a.trainMetrics[options.metric] - b.trainMetrics[options.metric],
    );
    const bestCandidate = sortedCandidates[0]!;

    const defaultCandidate = evaluateSingleCandidate(
      trainMatches,
      DEFAULT_MODEL_WEIGHTS,
    );

    console.error(`Evaluating best weights on test set...`);
    const bestOnTest = evaluateSingleCandidate(
      testMatches,
      bestCandidate.weights,
    );
    const defaultOnTest = evaluateSingleCandidate(
      testMatches,
      DEFAULT_MODEL_WEIGHTS,
    );

    const topCandidatesOnTest = sortedCandidates.slice(0, 10).map((c) => {
      const testMetrics = evaluateSingleCandidate(testMatches, c.weights);
      return {
        weights: c.weights,
        trainLogLoss: c.trainMetrics.logLoss,
        testLogLoss: testMetrics.trainMetrics.logLoss,
      };
    });

    const result: OptimizationResult = {
      bestWeights: bestCandidate.weights,
      defaultWeights: DEFAULT_MODEL_WEIGHTS,
      trainSeasons: {
        from: options.trainSeasonFrom,
        to: options.trainSeasonTo,
      },
      testSeasons: { from: options.testSeasonFrom, to: options.testSeasonTo },
      optimizationMetric: options.metric,
      gridResolution: options.gridResolution,
      totalCandidatesEvaluated: candidates.length,
      trainResults: {
        best: bestCandidate.trainMetrics,
        default: defaultCandidate.trainMetrics,
        improvement: {
          logLoss: roundTo(
            defaultCandidate.trainMetrics.logLoss -
              bestCandidate.trainMetrics.logLoss,
            6,
          ),
          brierScore: roundTo(
            defaultCandidate.trainMetrics.brierScore -
              bestCandidate.trainMetrics.brierScore,
            6,
          ),
          calibrationError: roundTo(
            defaultCandidate.trainMetrics.calibrationError -
              bestCandidate.trainMetrics.calibrationError,
            6,
          ),
        },
      },
      testResults: {
        best: bestOnTest.trainMetrics,
        default: defaultOnTest.trainMetrics,
        improvement: {
          logLoss: roundTo(
            defaultOnTest.trainMetrics.logLoss -
              bestOnTest.trainMetrics.logLoss,
            6,
          ),
          brierScore: roundTo(
            defaultOnTest.trainMetrics.brierScore -
              bestOnTest.trainMetrics.brierScore,
            6,
          ),
          calibrationError: roundTo(
            defaultOnTest.trainMetrics.calibrationError -
              bestOnTest.trainMetrics.calibrationError,
            6,
          ),
        },
      },
      topCandidates: topCandidatesOnTest,
    };

    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

interface MatchWithContext {
  match: CompletedMatchRow;
  season: number;
  featureRow: ReturnType<typeof buildBaselinePreMatchFeatureRow>;
  actualOutcome: 0 | 1;
}

async function buildAllMatchContexts(
  pool: ReturnType<typeof createPgPool>,
  matches: CompletedMatchRow[],
): Promise<MatchWithContext[]> {
  const results: MatchWithContext[] = [];

  for (const match of matches) {
    const asOfDate = new Date(match.scheduled_start);
    const featureContext = await buildFeatureContextFromHistory(pool, asOfDate);
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
          source: "weight_optimization",
          generated: true,
        },
      },
    });

    const featureRow = buildBaselinePreMatchFeatureRow(
      checkpoint,
      featureContext,
    );

    const actualOutcome =
      match.winning_team_name === match.team_a_name
        ? 1
        : match.winning_team_name === match.team_b_name
          ? 0
          : null;

    if (actualOutcome === null) {
      continue;
    }

    results.push({
      match,
      season: match.season,
      featureRow,
      actualOutcome,
    });
  }

  return results;
}

function generateWeightGrid(resolution: number): ModelWeights[] {
  const ratingRange = generateRange(0.5, 2.0, resolution);
  const formRange = generateRange(0.2, 1.2, resolution);
  const venueRange = generateRange(0.0, 0.8, resolution);
  const h2hRange = generateRange(0.2, 1.0, resolution);
  const restRange = generateRange(0.0, 0.5, resolution);
  const congestionRange = generateRange(0.0, 0.4, resolution);
  const lineupStabilityRange = generateRange(0.0, 0.8, resolution);
  const lineupContinuityRange = generateRange(0.0, 0.8, resolution);
  const lineupRotationRange = generateRange(0.0, 0.8, resolution);
  const bowlerShareRange = generateRange(0.0, 0.8, resolution);
  const allRounderShareRange = generateRange(0.0, 0.8, resolution);
  const leftHandBatShareRange = generateRange(0.0, 0.8, resolution);
  const paceBowlerShareRange = generateRange(0.0, 0.8, resolution);
  const spinBowlerShareRange = generateRange(0.0, 0.8, resolution);
  const seasonWinRateRange = generateRange(0.0, 0.8, resolution);
  const seasonMatchesPlayedRange = generateRange(0.0, 0.4, resolution);
  const seasonWinStrengthRange = generateRange(0.0, 1.0, resolution);
  const dewFactorRange = generateRange(0.0, 0.6, resolution);
  const homeAdvantageRange = generateRange(0.0, 0.8, resolution);
  const pitchBattingIndexRange = generateRange(0.0, 0.6, resolution);

  const grid: ModelWeights[] = [];

  for (const rating of ratingRange) {
    for (const form of formRange) {
      for (const venue of venueRange) {
        for (const headToHead of h2hRange) {
          for (const rest of restRange) {
            for (const congestion of congestionRange) {
              for (const lineupStability of lineupStabilityRange) {
                for (const lineupContinuity of lineupContinuityRange) {
                  for (const lineupRotation of lineupRotationRange) {
                    for (const bowlerShare of bowlerShareRange) {
                      for (const allRounderShare of allRounderShareRange) {
                        for (const leftHandBatShare of leftHandBatShareRange) {
                          for (const paceBowlerShare of paceBowlerShareRange) {
                            for (const spinBowlerShare of spinBowlerShareRange) {
                              for (const seasonWinRate of seasonWinRateRange) {
                                for (const seasonMatchesPlayed of seasonMatchesPlayedRange) {
                                  for (const seasonWinStrength of seasonWinStrengthRange) {
                                    for (const dewFactor of dewFactorRange) {
                                      for (const homeAdvantage of homeAdvantageRange) {
                                        for (const pitchBattingIndex of pitchBattingIndexRange) {
                                          grid.push({
                                            rating: roundTo(rating, 2),
                                            form: roundTo(form, 2),
                                            venue: roundTo(venue, 2),
                                            headToHead: roundTo(headToHead, 2),
                                            rest: roundTo(rest, 2),
                                            congestion: roundTo(congestion, 2),
                                            lineupStability: roundTo(
                                              lineupStability,
                                              2,
                                            ),
                                            lineupContinuity: roundTo(
                                              lineupContinuity,
                                              2,
                                            ),
                                            lineupRotation: roundTo(
                                              lineupRotation,
                                              2,
                                            ),
                                            bowlerShare: roundTo(
                                              bowlerShare,
                                              2,
                                            ),
                                            allRounderShare: roundTo(
                                              allRounderShare,
                                              2,
                                            ),
                                            leftHandBatShare: roundTo(
                                              leftHandBatShare,
                                              2,
                                            ),
                                            paceBowlerShare: roundTo(
                                              paceBowlerShare,
                                              2,
                                            ),
                                            spinBowlerShare: roundTo(
                                              spinBowlerShare,
                                              2,
                                            ),
                                            leftBatVsOppSpin: 0,
                                            leftBatVsOppPace: 0,
                                            seasonWinRate: roundTo(
                                              seasonWinRate,
                                              2,
                                            ),
                                            seasonMatchesPlayed: roundTo(
                                              seasonMatchesPlayed,
                                              2,
                                            ),
                                            seasonWinStrength: roundTo(
                                              seasonWinStrength,
                                              2,
                                            ),
                                            dewFactor: roundTo(dewFactor, 2),
                                            homeAdvantage: roundTo(
                                              homeAdvantage,
                                              2,
                                            ),
                                            pitchBattingIndex: roundTo(
                                              pitchBattingIndex,
                                              2,
                                            ),
                                          });
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return grid;
}

function generateRange(min: number, max: number, steps: number): number[] {
  if (steps <= 1) return [(min + max) / 2];
  const result: number[] = [];
  const step = (max - min) / (steps - 1);
  for (let i = 0; i < steps; i++) {
    result.push(min + i * step);
  }
  return result;
}

function evaluateCandidates(
  matches: MatchWithContext[],
  weightGrid: ModelWeights[],
): WeightCandidate[] {
  const totalCandidates = weightGrid.length;
  let processed = 0;
  const progressInterval = Math.max(1, Math.floor(totalCandidates / 20));

  return weightGrid.map((weights) => {
    processed++;
    if (processed % progressInterval === 0) {
      console.error(
        `  Progress: ${processed}/${totalCandidates} (${Math.round((processed / totalCandidates) * 100)}%)`,
      );
    }
    return evaluateSingleCandidate(matches, weights);
  });
}

function evaluateSingleCandidate(
  matches: MatchWithContext[],
  weights: ModelWeights,
): WeightCandidate {
  const rows: HistoricalPredictionRow[] = matches.map((m, index) => {
    const score = scoreBaselineIplPreMatch(m.featureRow, { weights });
    return {
      modelKey: OPTIMIZATION_MODEL_KEY,
      checkpointType: "pre_match" as const,
      modelScoreId: index + 1,
      matchSlug: m.match.match_slug,
      season: m.season,
      snapshotTime: m.featureRow.generatedAt,
      actualOutcome: m.actualOutcome,
      positiveClassLabel: m.match.team_a_name,
      negativeClassLabel: m.match.team_b_name,
      primaryProbability: score.teamAWinProbability,
      socialOnProbability: score.teamAWinProbability,
      socialOffProbability: score.teamAWinProbability,
      socialSupported: false,
      marketImpliedProbability: null,
      provenance: { source: "weight_optimization" },
    };
  });

  const metrics = calculateProbabilityMetrics(
    rows,
    (r) => r.primaryProbability,
    10,
  );

  return {
    weights,
    trainMetrics: {
      logLoss: metrics.logLoss,
      brierScore: metrics.brierScore,
      calibrationError: metrics.calibrationError,
      accuracy: metrics.accuracy,
      sampleSize: metrics.sampleSize,
    },
  };
}

async function loadCompletedMatches(
  pool: ReturnType<typeof createPgPool>,
  seasonTo: number,
): Promise<CompletedMatchRow[]> {
  const result = await pool.query<CompletedMatchRow>(
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
  let testSeasonFrom: number | null = null;
  let testSeasonTo: number | null = null;
  let gridResolution = 3;
  let metric: "logLoss" | "brierScore" | "calibrationError" = "logLoss";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--train-from") {
      trainSeasonFrom = parseIntegerArg("--train-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--train-to") {
      trainSeasonTo = parseIntegerArg("--train-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--test-from") {
      testSeasonFrom = parseIntegerArg("--test-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--test-to") {
      testSeasonTo = parseIntegerArg("--test-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--grid-resolution") {
      gridResolution = parseIntegerArg("--grid-resolution", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--metric") {
      const val = argv[index + 1];
      if (
        val !== "logLoss" &&
        val !== "brierScore" &&
        val !== "calibrationError"
      ) {
        throw new Error(
          `--metric must be one of: logLoss, brierScore, calibrationError`,
        );
      }
      metric = val;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --train-from, --train-to, --test-from, --test-to, --grid-resolution, --metric.`,
    );
  }

  if (
    trainSeasonFrom === null ||
    trainSeasonTo === null ||
    testSeasonFrom === null ||
    testSeasonTo === null
  ) {
    throw new Error(
      "Missing required arguments: --train-from, --train-to, --test-from, --test-to",
    );
  }

  if (trainSeasonTo >= testSeasonFrom) {
    throw new Error(
      "Training seasons must end before test seasons start to prevent leakage.",
    );
  }

  return {
    trainSeasonFrom,
    trainSeasonTo,
    testSeasonFrom,
    testSeasonTo,
    gridResolution,
    metric,
  };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }
  return parsed;
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Weight optimization failed: ${message}`);
  process.exitCode = 1;
});
