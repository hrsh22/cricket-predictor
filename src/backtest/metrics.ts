import type {
  CalibrationBinSummary,
  CalibrationSummary,
  HistoricalPredictionRow,
  ProbabilityMetrics,
  TradingMetricsSummary,
  TradingThresholdSummary,
} from "./types.js";

const DEFAULT_EPSILON = 1e-6;
const DEFAULT_TRADING_EDGE_THRESHOLDS = [0, 0.01, 0.02, 0.03, 0.05] as const;

export function calculateProbabilityMetrics(
  rows: readonly HistoricalPredictionRow[],
  selector: (row: HistoricalPredictionRow) => number,
  calibrationBinCount = 10,
): ProbabilityMetrics {
  if (rows.length === 0) {
    throw new Error("Probability metrics require at least one evaluation row.");
  }

  let logLossTotal = 0;
  let brierScoreTotal = 0;
  let correctCount = 0;
  let predictedTotal = 0;
  let actualTotal = 0;

  for (const row of rows) {
    const probability = normalizeProbability(selector(row));
    const outcome = row.actualOutcome;
    const clamped = clampProbability(probability, DEFAULT_EPSILON);

    logLossTotal += outcome === 1 ? -Math.log(clamped) : -Math.log(1 - clamped);
    brierScoreTotal += (probability - outcome) ** 2;
    predictedTotal += probability;
    actualTotal += outcome;

    const predictedOutcome = probability >= 0.5 ? 1 : 0;
    if (predictedOutcome === outcome) {
      correctCount += 1;
    }
  }

  return {
    sampleSize: rows.length,
    logLoss: roundTo(logLossTotal / rows.length, 6),
    brierScore: roundTo(brierScoreTotal / rows.length, 6),
    calibrationError: calculateCalibrationSummary(
      rows,
      selector,
      calibrationBinCount,
    ).calibrationError,
    accuracy: roundTo(correctCount / rows.length, 6),
    meanPredictedProbability: roundTo(predictedTotal / rows.length, 6),
    positiveRate: roundTo(actualTotal / rows.length, 6),
  };
}

export function calculateCalibrationSummary(
  rows: readonly HistoricalPredictionRow[],
  selector: (row: HistoricalPredictionRow) => number,
  binCount: number,
): CalibrationSummary {
  if (!Number.isInteger(binCount) || binCount <= 0) {
    throw new Error(
      "Calibration summary requires a positive integer binCount.",
    );
  }

  if (rows.length === 0) {
    throw new Error(
      "Calibration summary requires at least one evaluation row.",
    );
  }

  const bins = Array.from({ length: binCount }, (_, index) => ({
    index,
    lowerBound: index / binCount,
    upperBound: (index + 1) / binCount,
    rows: [] as HistoricalPredictionRow[],
    predictedTotal: 0,
    actualTotal: 0,
  }));

  for (const row of rows) {
    const probability = normalizeProbability(selector(row));
    const binIndex = Math.min(binCount - 1, Math.floor(probability * binCount));
    const bucket = bins[binIndex] as (typeof bins)[number];

    bucket.rows.push(row);
    bucket.predictedTotal += probability;
    bucket.actualTotal += row.actualOutcome;
  }

  let errorTotal = 0;
  const summaries: CalibrationBinSummary[] = bins.map((bucket) => {
    if (bucket.rows.length === 0) {
      return {
        index: bucket.index,
        lowerBound: roundTo(bucket.lowerBound, 6),
        upperBound: roundTo(bucket.upperBound, 6),
        sampleSize: 0,
        averagePredictedProbability: null,
        empiricalPositiveRate: null,
        absoluteGap: null,
      };
    }

    const averagePredictedProbability =
      bucket.predictedTotal / bucket.rows.length;
    const empiricalPositiveRate = bucket.actualTotal / bucket.rows.length;
    const absoluteGap = Math.abs(
      averagePredictedProbability - empiricalPositiveRate,
    );

    errorTotal += absoluteGap * bucket.rows.length;

    return {
      index: bucket.index,
      lowerBound: roundTo(bucket.lowerBound, 6),
      upperBound: roundTo(bucket.upperBound, 6),
      sampleSize: bucket.rows.length,
      averagePredictedProbability: roundTo(averagePredictedProbability, 6),
      empiricalPositiveRate: roundTo(empiricalPositiveRate, 6),
      absoluteGap: roundTo(absoluteGap, 6),
    };
  });

  return {
    sampleSize: rows.length,
    binCount,
    calibrationError: roundTo(errorTotal / rows.length, 6),
    bins: summaries,
  };
}

export function calculateTradingMetrics(
  rows: readonly HistoricalPredictionRow[],
  selector: (row: HistoricalPredictionRow) => number,
  thresholds: readonly number[] = DEFAULT_TRADING_EDGE_THRESHOLDS,
): TradingMetricsSummary | null {
  const marketRows = rows.filter(
    (row) => row.marketImpliedProbability !== null,
  );

  if (marketRows.length === 0) {
    return null;
  }

  const normalizedThresholds = thresholds
    .filter((threshold) => Number.isFinite(threshold) && threshold >= 0)
    .map((threshold) => roundTo(threshold, 6));

  const uniqueThresholds = Array.from(new Set(normalizedThresholds)).sort(
    (left, right) => left - right,
  );

  const summaries = uniqueThresholds.map((minimumEdge) =>
    summarizeTradingThreshold(marketRows, selector, minimumEdge),
  );

  return {
    marketSampleSize: marketRows.length,
    thresholds: summaries,
    bestRoiThreshold: pickBestTradingThreshold(summaries, "roi"),
    bestProfitThreshold: pickBestTradingThreshold(summaries, "totalProfit"),
  };
}

export function normalizeProbability(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error("Probability values must be finite and within [0, 1].");
  }

  return roundTo(value, 6);
}

function clampProbability(value: number, epsilon: number): number {
  if (value <= epsilon) {
    return epsilon;
  }

  if (value >= 1 - epsilon) {
    return 1 - epsilon;
  }

  return value;
}

function summarizeTradingThreshold(
  rows: readonly HistoricalPredictionRow[],
  selector: (row: HistoricalPredictionRow) => number,
  minimumEdge: number,
): TradingThresholdSummary {
  let betCount = 0;
  let winCount = 0;
  let totalProfit = 0;
  let totalEntryPrice = 0;
  let totalModelProbability = 0;
  let totalEdge = 0;
  let totalExpectedValue = 0;

  for (const row of rows) {
    const marketProbability = row.marketImpliedProbability;
    if (marketProbability === null) {
      continue;
    }

    const modelProbability = normalizeProbability(selector(row));
    const yesEdge = modelProbability - marketProbability;
    if (Math.abs(yesEdge) <= minimumEdge) {
      continue;
    }

    const backYes = yesEdge > 0;
    const entryPrice = backYes ? marketProbability : 1 - marketProbability;
    const modelProbabilityForTrade = backYes
      ? modelProbability
      : roundTo(1 - modelProbability, 6);
    const tradeEdge = roundTo(modelProbabilityForTrade - entryPrice, 6);
    const tradeWon = backYes
      ? row.actualOutcome === 1
      : row.actualOutcome === 0;
    const profit = tradeWon ? 1 - entryPrice : -entryPrice;

    betCount += 1;
    winCount += tradeWon ? 1 : 0;
    totalProfit += profit;
    totalEntryPrice += entryPrice;
    totalModelProbability += modelProbabilityForTrade;
    totalEdge += tradeEdge;
    totalExpectedValue += tradeEdge;
  }

  return {
    minimumEdge,
    betCount,
    winCount,
    winRate: betCount === 0 ? null : roundTo(winCount / betCount, 6),
    totalStake: betCount,
    totalProfit: roundTo(totalProfit, 6),
    roi: betCount === 0 ? null : roundTo(totalProfit / betCount, 6),
    averageEntryPrice:
      betCount === 0 ? null : roundTo(totalEntryPrice / betCount, 6),
    averageModelProbability:
      betCount === 0 ? null : roundTo(totalModelProbability / betCount, 6),
    averageEdge: betCount === 0 ? null : roundTo(totalEdge / betCount, 6),
    totalExpectedValue: roundTo(totalExpectedValue, 6),
  };
}

function pickBestTradingThreshold(
  thresholds: readonly TradingThresholdSummary[],
  metric: "roi" | "totalProfit",
): TradingThresholdSummary | null {
  const eligible = thresholds.filter((threshold) => threshold.betCount > 0);
  if (eligible.length === 0) {
    return null;
  }

  return eligible.reduce((best, current) => {
    const currentMetric =
      metric === "roi"
        ? (current.roi ?? Number.NEGATIVE_INFINITY)
        : current.totalProfit;
    const bestMetric =
      metric === "roi"
        ? (best.roi ?? Number.NEGATIVE_INFINITY)
        : best.totalProfit;

    if (currentMetric > bestMetric) {
      return current;
    }

    if (currentMetric < bestMetric) {
      return best;
    }

    if (current.totalProfit > best.totalProfit) {
      return current;
    }

    if (current.totalProfit < best.totalProfit) {
      return best;
    }

    return current.minimumEdge < best.minimumEdge ? current : best;
  });
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
