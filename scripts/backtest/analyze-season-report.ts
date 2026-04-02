import { readFile } from "node:fs/promises";

interface MatchRow {
  predictedWinner: string;
  predictedWinnerProbability: number;
  actualWinner: string;
  correct: boolean;
}

interface SeasonReport {
  season: number;
  accuracy: number;
  metrics: {
    logLoss: number;
    brierScore: number;
    calibrationError: number;
  };
  rows: MatchRow[];
}

async function main(): Promise<void> {
  const path = process.argv[2] ?? "/tmp/ipl_season_performance_2024.json";
  const raw = await readFile(path, "utf8");
  const report = JSON.parse(raw) as SeasonReport;

  const rows = report.rows;
  const confidenceBuckets = summarizeConfidenceBuckets(rows);
  const predictedWinnerTeamStats = summarizePredictedWinnerTeams(rows);
  const actualWinnerCoverage = summarizeActualWinnerCoverage(rows);

  const highConfidence = rows.filter(
    (row) => row.predictedWinnerProbability >= 0.7,
  );
  const heavyFavorite = rows.filter(
    (row) => row.predictedWinnerProbability >= 0.75,
  );
  const coinFlip = rows.filter((row) => row.predictedWinnerProbability < 0.55);

  process.stdout.write(
    `${JSON.stringify(
      {
        season: report.season,
        overall: {
          matches: rows.length,
          accuracy: report.accuracy,
          logLoss: report.metrics.logLoss,
          brierScore: report.metrics.brierScore,
          calibrationError: report.metrics.calibrationError,
        },
        confidenceBuckets,
        highConfidenceSummary: {
          threshold: 0.7,
          matches: highConfidence.length,
          accuracy: ratio(
            highConfidence.filter((row) => row.correct).length,
            highConfidence.length,
          ),
        },
        heavyFavoriteSummary: {
          threshold: 0.75,
          matches: heavyFavorite.length,
          accuracy: ratio(
            heavyFavorite.filter((row) => row.correct).length,
            heavyFavorite.length,
          ),
        },
        coinFlipSummary: {
          threshold: 0.55,
          matches: coinFlip.length,
          accuracy: ratio(
            coinFlip.filter((row) => row.correct).length,
            coinFlip.length,
          ),
        },
        predictedWinnerTeamStats,
        actualWinnerCoverage,
      },
      null,
      2,
    )}\n`,
  );
}

function summarizeConfidenceBuckets(
  rows: readonly MatchRow[],
): Array<{ range: string; matches: number; accuracy: number }> {
  const boundaries = [0.5, 0.6, 0.7, 0.8, 0.9, 1.01];
  const buckets: Array<{ range: string; matches: number; accuracy: number }> =
    [];

  for (let index = 0; index < boundaries.length - 1; index += 1) {
    const lower = boundaries[index] as number;
    const upper = boundaries[index + 1] as number;
    const inBucket = rows.filter(
      (row) =>
        row.predictedWinnerProbability >= lower &&
        row.predictedWinnerProbability < upper,
    );

    buckets.push({
      range: `${lower.toFixed(1)}-${(upper === 1.01 ? 1.0 : upper).toFixed(1)}`,
      matches: inBucket.length,
      accuracy: ratio(
        inBucket.filter((row) => row.correct).length,
        inBucket.length,
      ),
    });
  }

  return buckets;
}

function summarizePredictedWinnerTeams(
  rows: readonly MatchRow[],
): Array<{
  team: string;
  predictedMatches: number;
  accuracyWhenPredicted: number;
}> {
  const stats = new Map<
    string,
    { predictedMatches: number; correct: number }
  >();

  for (const row of rows) {
    const existing = stats.get(row.predictedWinner) ?? {
      predictedMatches: 0,
      correct: 0,
    };

    existing.predictedMatches += 1;
    if (row.correct) {
      existing.correct += 1;
    }

    stats.set(row.predictedWinner, existing);
  }

  return [...stats.entries()]
    .map(([team, value]) => ({
      team,
      predictedMatches: value.predictedMatches,
      accuracyWhenPredicted: ratio(value.correct, value.predictedMatches),
    }))
    .sort((left, right) => right.predictedMatches - left.predictedMatches);
}

function summarizeActualWinnerCoverage(
  rows: readonly MatchRow[],
): Array<{
  team: string;
  actualWins: number;
  correctlyPredictedWinRate: number;
}> {
  const wins = new Map<
    string,
    { actualWins: number; correctlyPredicted: number }
  >();

  for (const row of rows) {
    const existing = wins.get(row.actualWinner) ?? {
      actualWins: 0,
      correctlyPredicted: 0,
    };

    existing.actualWins += 1;
    if (row.correct) {
      existing.correctlyPredicted += 1;
    }

    wins.set(row.actualWinner, existing);
  }

  return [...wins.entries()]
    .map(([team, value]) => ({
      team,
      actualWins: value.actualWins,
      correctlyPredictedWinRate: ratio(
        value.correctlyPredicted,
        value.actualWins,
      ),
    }))
    .sort((left, right) => right.actualWins - left.actualWins);
}

function ratio(numerator: number, denominator: number): number {
  if (denominator <= 0) {
    return 0;
  }

  return Number((numerator / denominator).toFixed(4));
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Season report analysis failed: ${message}`);
  process.exitCode = 1;
});
