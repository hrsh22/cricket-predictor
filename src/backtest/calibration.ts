import { normalizeProbability } from "./metrics.js";

export interface PlattCalibrationModel {
  intercept: number;
  slope: number;
  iterations: number;
  converged: boolean;
  trainSampleSize: number;
}

/**
 * Isotonic calibration model using Pool Adjacent Violators (PAV) algorithm.
 * Better than Platt scaling for non-linear miscalibration patterns.
 */
export interface IsotonicCalibrationModel {
  /** Sorted probability thresholds for interpolation */
  thresholds: number[];
  /** Calibrated values at each threshold (monotonically increasing) */
  calibratedValues: number[];
  trainSampleSize: number;
}

interface TrainingRow {
  probability: number;
  outcome: 0 | 1;
}

const MIN_PROBABILITY = 1e-6;

export function fitPlattCalibration(
  rows: readonly TrainingRow[],
): PlattCalibrationModel {
  if (rows.length < 10) {
    return {
      intercept: 0,
      slope: 1,
      iterations: 0,
      converged: false,
      trainSampleSize: rows.length,
    };
  }

  const logits = rows.map((row) => ({
    x: probabilityToLogit(row.probability),
    y: row.outcome,
  }));

  let intercept = 0;
  let slope = 1;
  const maxIterations = 200;
  const tolerance = 1e-8;
  const ridge = 1e-6;

  let converged = false;
  let iteration = 0;

  for (iteration = 1; iteration <= maxIterations; iteration += 1) {
    let gradIntercept = 0;
    let gradSlope = 0;
    let h00 = 0;
    let h01 = 0;
    let h11 = 0;

    for (const row of logits) {
      const prediction = sigmoid(intercept + slope * row.x);
      const residual = row.y - prediction;
      const weight = prediction * (1 - prediction);

      gradIntercept += residual;
      gradSlope += residual * row.x;
      h00 += weight;
      h01 += weight * row.x;
      h11 += weight * row.x * row.x;
    }

    const adjustedH00 = h00 + ridge;
    const adjustedH11 = h11 + ridge;
    const determinant = adjustedH00 * adjustedH11 - h01 * h01;

    if (!Number.isFinite(determinant) || Math.abs(determinant) < 1e-12) {
      break;
    }

    const stepIntercept =
      (gradIntercept * adjustedH11 - gradSlope * h01) / determinant;
    const stepSlope =
      (gradSlope * adjustedH00 - gradIntercept * h01) / determinant;

    intercept += stepIntercept;
    slope += stepSlope;

    if (
      Math.abs(stepIntercept) < tolerance &&
      Math.abs(stepSlope) < tolerance
    ) {
      converged = true;
      break;
    }
  }

  return {
    intercept: roundTo(intercept, 8),
    slope: roundTo(slope, 8),
    iterations: iteration,
    converged,
    trainSampleSize: rows.length,
  };
}

export function applyPlattCalibration(
  probability: number,
  model: PlattCalibrationModel,
): number {
  const normalized = normalizeProbability(probability);
  const logit = probabilityToLogit(normalized);
  const calibrated = sigmoid(model.intercept + model.slope * logit);
  return clampProbability(calibrated);
}

function probabilityToLogit(probability: number): number {
  const p = clampProbability(probability);
  return Math.log(p / (1 - p));
}

function sigmoid(value: number): number {
  if (value >= 0) {
    const expNeg = Math.exp(-value);
    return 1 / (1 + expNeg);
  }

  const expPos = Math.exp(value);
  return expPos / (1 + expPos);
}

function clampProbability(value: number): number {
  if (!Number.isFinite(value)) {
    return 0.5;
  }

  if (value < MIN_PROBABILITY) {
    return MIN_PROBABILITY;
  }

  if (value > 1 - MIN_PROBABILITY) {
    return 1 - MIN_PROBABILITY;
  }

  return roundTo(value, 6);
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}

export function fitIsotonicCalibration(
  rows: readonly TrainingRow[],
): IsotonicCalibrationModel {
  if (rows.length < 10) {
    return {
      thresholds: [0, 1],
      calibratedValues: [0, 1],
      trainSampleSize: rows.length,
    };
  }

  const sorted = [...rows].sort((a, b) => a.probability - b.probability);

  const blocks: Array<{ sumProb: number; sumOutcome: number; count: number }> =
    sorted.map((row) => ({
      sumProb: row.probability,
      sumOutcome: row.outcome,
      count: 1,
    }));

  let changed = true;
  while (changed) {
    changed = false;
    for (let i = 0; i < blocks.length - 1; i++) {
      const current = blocks[i]!;
      const next = blocks[i + 1]!;
      const currentMean = current.sumOutcome / current.count;
      const nextMean = next.sumOutcome / next.count;

      if (currentMean > nextMean) {
        blocks[i] = {
          sumProb: current.sumProb + next.sumProb,
          sumOutcome: current.sumOutcome + next.sumOutcome,
          count: current.count + next.count,
        };
        blocks.splice(i + 1, 1);
        changed = true;
        break;
      }
    }
  }

  const thresholds: number[] = [0];
  const calibratedValues: number[] = [0];

  let runningCount = 0;
  for (const block of blocks) {
    runningCount += block.count;
    const threshold = sorted[runningCount - 1]!.probability;
    const calibratedValue = block.sumOutcome / block.count;

    if (threshold > thresholds[thresholds.length - 1]!) {
      thresholds.push(roundTo(threshold, 6));
      calibratedValues.push(roundTo(calibratedValue, 6));
    }
  }

  if (thresholds[thresholds.length - 1]! < 1) {
    thresholds.push(1);
    calibratedValues.push(calibratedValues[calibratedValues.length - 1]!);
  }

  return {
    thresholds,
    calibratedValues,
    trainSampleSize: rows.length,
  };
}

export function applyIsotonicCalibration(
  probability: number,
  model: IsotonicCalibrationModel,
): number {
  const p = clampProbability(probability);

  if (model.thresholds.length < 2) {
    return p;
  }

  let low = 0;
  let high = model.thresholds.length - 1;

  while (low < high - 1) {
    const mid = Math.floor((low + high) / 2);
    if (model.thresholds[mid]! <= p) {
      low = mid;
    } else {
      high = mid;
    }
  }

  const x0 = model.thresholds[low]!;
  const x1 = model.thresholds[high]!;
  const y0 = model.calibratedValues[low]!;
  const y1 = model.calibratedValues[high]!;

  if (x1 === x0) {
    return clampProbability(y0);
  }

  const t = (p - x0) / (x1 - x0);
  const interpolated = y0 + t * (y1 - y0);

  return clampProbability(interpolated);
}
