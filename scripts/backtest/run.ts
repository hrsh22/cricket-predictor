import { loadAppConfig } from "../../src/config/index.js";
import {
  runStoredScoreBacktest,
  type HistoricalBacktestOptions,
} from "../../src/backtest/index.js";
import { createModelingRepository } from "../../src/repositories/modeling.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions extends HistoricalBacktestOptions {
  runKey: string;
  triggeredBy: string;
  persist: boolean;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const result = await runStoredScoreBacktest({
      executor: pool,
      modelingRepository: createModelingRepository(pool),
      options,
      runKey: options.runKey,
      triggeredBy: options.triggeredBy,
      persist: options.persist,
    });

    console.log(
      JSON.stringify(
        {
          runKey: options.runKey,
          persisted: options.persist,
          modelKey: result.modelKey,
          checkpointType: result.checkpointType,
          evaluationSeasonFrom: result.evaluationSeasonFrom,
          evaluationSeasonTo: result.evaluationSeasonTo,
          splitStrategy: result.splitStrategy,
          overallMetrics: result.overallMetrics,
          calibration: result.calibration,
          folds: result.folds,
          skippedFolds: result.skippedFolds,
          socialComparison: result.socialComparison,
          skippedRows: result.skippedRows,
        },
        null,
        2,
      ),
    );
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let modelKey: string | null = null;
  let checkpointType: HistoricalBacktestOptions["checkpointType"] | null = null;
  let evaluationSeasonFrom: number | null = null;
  let evaluationSeasonTo: number | null = null;
  let calibrationBinCount: number | undefined;
  let minimumTrainingSamples: number | undefined;
  let minimumTestSamples: number | undefined;
  let runKey: string | null = null;
  let triggeredBy = "manual";
  let persist = true;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--model-key") {
      modelKey = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--checkpoint") {
      const value = argv[index + 1] ?? null;
      if (
        value !== "pre_match" &&
        value !== "post_toss" &&
        value !== "innings_break"
      ) {
        throw new Error(
          `Unsupported checkpoint "${value ?? ""}". Expected pre_match, post_toss, or innings_break.`,
        );
      }

      checkpointType = value;
      index += 1;
      continue;
    }

    if (argument === "--season-from") {
      evaluationSeasonFrom = parseIntegerArg("--season-from", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--season-to") {
      evaluationSeasonTo = parseIntegerArg("--season-to", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--bin-count") {
      calibrationBinCount = parseIntegerArg("--bin-count", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--min-training-samples") {
      minimumTrainingSamples = parseIntegerArg(
        "--min-training-samples",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    if (argument === "--min-test-samples") {
      minimumTestSamples = parseIntegerArg(
        "--min-test-samples",
        argv[index + 1],
      );
      index += 1;
      continue;
    }

    if (argument === "--run-key") {
      runKey = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--triggered-by") {
      triggeredBy = argv[index + 1] ?? triggeredBy;
      index += 1;
      continue;
    }

    if (argument === "--dry-run") {
      persist = false;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --model-key, --checkpoint, --season-from, --season-to, optional --bin-count, --min-training-samples, --min-test-samples, --run-key, --triggered-by, or --dry-run.`,
    );
  }

  if (modelKey === null || modelKey.trim().length === 0) {
    throw new Error("Missing required --model-key <value> argument.");
  }

  if (checkpointType === null) {
    throw new Error("Missing required --checkpoint <value> argument.");
  }

  if (evaluationSeasonFrom === null || evaluationSeasonTo === null) {
    throw new Error(
      "Missing required --season-from <year> and --season-to <year> arguments.",
    );
  }

  return {
    modelKey,
    checkpointType,
    evaluationSeasonFrom,
    evaluationSeasonTo,
    ...(calibrationBinCount === undefined ? {} : { calibrationBinCount }),
    ...(minimumTrainingSamples === undefined ? {} : { minimumTrainingSamples }),
    ...(minimumTestSamples === undefined ? {} : { minimumTestSamples }),
    runKey:
      runKey ??
      buildDefaultRunKey(
        modelKey,
        checkpointType,
        evaluationSeasonFrom,
        evaluationSeasonTo,
      ),
    triggeredBy,
    persist,
  };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }

  return parsed;
}

function buildDefaultRunKey(
  modelKey: string,
  checkpointType: HistoricalBacktestOptions["checkpointType"],
  seasonFrom: number,
  seasonTo: number,
): string {
  const sanitizedModelKey = modelKey.replace(/[^a-zA-Z0-9_-]+/g, "-");
  return `backtest-${sanitizedModelKey}-${checkpointType}-${seasonFrom}-${seasonTo}`;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Backtest run failed: ${message}`);
  process.exitCode = 1;
});
