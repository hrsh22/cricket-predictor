import type {
  CalibrationBinSummary,
  CalibrationSummary,
  HistoricalPredictionRow,
  ProbabilityMetrics,
} from "./types.js";

const DEFAULT_EPSILON = 1e-6;

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

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}
