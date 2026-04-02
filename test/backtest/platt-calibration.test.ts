import { describe, expect, it } from "vitest";

import {
  applyPlattCalibration,
  fitPlattCalibration,
} from "../../src/backtest/index.js";

describe("platt calibration", () => {
  it("returns identity-like model for tiny datasets", () => {
    const model = fitPlattCalibration([
      { probability: 0.6, outcome: 1 },
      { probability: 0.4, outcome: 0 },
    ]);

    expect(model.trainSampleSize).toBe(2);
    expect(model.intercept).toBe(0);
    expect(model.slope).toBe(1);
  });

  it("fits a stable finite calibration model on larger samples", () => {
    const training = [
      { probability: 0.8, outcome: 1 as const },
      { probability: 0.75, outcome: 1 as const },
      { probability: 0.7, outcome: 1 as const },
      { probability: 0.65, outcome: 1 as const },
      { probability: 0.6, outcome: 1 as const },
      { probability: 0.55, outcome: 1 as const },
      { probability: 0.45, outcome: 0 as const },
      { probability: 0.4, outcome: 0 as const },
      { probability: 0.35, outcome: 0 as const },
      { probability: 0.3, outcome: 0 as const },
      { probability: 0.25, outcome: 0 as const },
      { probability: 0.2, outcome: 0 as const },
    ];

    const model = fitPlattCalibration(training);
    expect(Number.isFinite(model.intercept)).toBe(true);
    expect(Number.isFinite(model.slope)).toBe(true);

    const calibratedHigh = applyPlattCalibration(0.8, model);
    const calibratedLow = applyPlattCalibration(0.2, model);

    expect(calibratedHigh).toBeGreaterThan(calibratedLow);
    expect(calibratedHigh).toBeGreaterThan(0);
    expect(calibratedHigh).toBeLessThan(1);
    expect(calibratedLow).toBeGreaterThan(0);
    expect(calibratedLow).toBeLessThan(1);
  });
});
