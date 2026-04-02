import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadAppConfig } from "../src/config/index.js";
import {
  fetchLiveCricketSnapshots,
  type CricketSnapshotInput,
} from "../src/ingest/cricket/index.js";
import {
  runRecurringPipeline,
  type RecurringRunRequest,
} from "../src/orchestration/index.js";
import { createPgPool } from "../src/repositories/index.js";
import { isRecord } from "../src/domain/primitives.js";
import type { PolymarketGammaReadClient } from "../src/ingest/polymarket/index.js";
import { buildFeatureContextFromHistory } from "../src/features/context-builder.js";

async function main(): Promise<void> {
  const options = await parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const featureContext = await buildFeatureContextFromHistory(
      pool,
      new Date(),
    );
    const summaries = [];

    for (let index = 0; index < options.checkpointSequence.length; index += 1) {
      const checkpointType = options.checkpointSequence[index];
      if (checkpointType === undefined) {
        continue;
      }
      const summary = await runRecurringPipeline(pool, {
        ...options.request,
        checkpointType,
        ...(options.request.runKey === undefined
          ? {}
          : {
              runKey:
                options.checkpointSequence.length === 1
                  ? options.request.runKey
                  : `${options.request.runKey}-${checkpointType}`,
            }),
        featureContextByMatchSlug: {
          "*": featureContext,
        },
      });

      summaries.push(summary);
    }

    process.stdout.write(
      `${JSON.stringify(
        summaries.length === 1 ? summaries[0] : { summaries },
        null,
        2,
      )}\n`,
    );
  } finally {
    await pool.end();
  }
}

export function resolveCheckpointSequence(input: {
  checkpointType: RecurringRunRequest["checkpointType"] | null;
  startFrom: "pre_match" | "post_toss" | null;
}): RecurringRunRequest["checkpointType"][] {
  if (input.checkpointType !== null && input.startFrom !== null) {
    throw new Error(
      "Use either --checkpoint or --start-from, not both in the same run.",
    );
  }

  if (input.checkpointType === null && input.startFrom === null) {
    throw new Error(
      "Missing required argument: provide --checkpoint <value> or --start-from <pre_match|post_toss>.",
    );
  }

  if (input.checkpointType !== null) {
    return [input.checkpointType];
  }

  return input.startFrom === "pre_match"
    ? ["pre_match", "post_toss", "innings_break"]
    : ["post_toss", "innings_break"];
}

async function parseCliArgs(argv: readonly string[]): Promise<{
  request: Omit<RecurringRunRequest, "checkpointType">;
  checkpointSequence: RecurringRunRequest["checkpointType"][];
}> {
  let checkpointType: RecurringRunRequest["checkpointType"] | null = null;
  let startFrom: "pre_match" | "post_toss" | null = null;
  let runKey: string | undefined;
  let triggeredBy: string | undefined;
  let marketInputPath: string | undefined;
  let cricketInputPath: string | undefined;
  let useLiveCricket = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

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

    if (argument === "--start-from") {
      const value = argv[index + 1] ?? null;
      if (value !== "pre_match" && value !== "post_toss") {
        throw new Error(
          `Unsupported start checkpoint "${value ?? ""}". Expected pre_match or post_toss.`,
        );
      }

      startFrom = value;
      index += 1;
      continue;
    }

    if (argument === "--run-key") {
      runKey = argv[index + 1] ?? undefined;
      index += 1;
      continue;
    }

    if (argument === "--triggered-by") {
      triggeredBy = argv[index + 1] ?? undefined;
      index += 1;
      continue;
    }

    if (argument === "--market-input") {
      marketInputPath = argv[index + 1] ?? undefined;
      index += 1;
      continue;
    }

    if (argument === "--cricket-input") {
      cricketInputPath = argv[index + 1] ?? undefined;
      index += 1;
      continue;
    }

    if (argument === "--live-cricket") {
      useLiveCricket = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --checkpoint, --start-from, --run-key, --triggered-by, --market-input, --cricket-input, or --live-cricket.`,
    );
  }

  const config = loadAppConfig();
  const checkpointSequence = resolveCheckpointSequence({
    checkpointType,
    startFrom,
  });

  const request: Omit<RecurringRunRequest, "checkpointType"> = {
    ...(runKey === undefined ? {} : { runKey }),
    ...(triggeredBy === undefined ? {} : { triggeredBy }),
    marketIngestion: {
      trigger: "scheduled",
    },
  };

  if (marketInputPath !== undefined) {
    const markets = await loadMarketSnapshots(marketInputPath);
    request.marketIngestion = {
      trigger: "scheduled",
      gammaClient: createStaticGammaClient(markets),
    };
  }

  if (cricketInputPath !== undefined || useLiveCricket) {
    const snapshots =
      cricketInputPath !== undefined
        ? await loadCricketSnapshots(cricketInputPath)
        : await fetchLiveCricketSnapshots({
            config: {
              provider: config.cricketLive.provider,
              apiKey: config.cricketLive.apiKey ?? "",
              baseUrl: config.cricketLive.baseUrl,
            },
          });
    request.cricketIngestion = { snapshots };
  }

  return {
    request,
    checkpointSequence: [...checkpointSequence],
  };
}

async function loadCricketSnapshots(
  inputPath: string,
): Promise<readonly CricketSnapshotInput[]> {
  const fileContent = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const parsed = JSON.parse(fileContent) as unknown;
  const entries = normalizeSnapshotList(parsed);

  return entries.map((entry, index) => parseSnapshotInput(entry, index));
}

async function loadMarketSnapshots(
  inputPath: string,
): Promise<readonly unknown[]> {
  const fileContent = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const parsed = JSON.parse(fileContent) as unknown;

  if (Array.isArray(parsed)) {
    return parsed;
  }

  if (isRecord(parsed) && Array.isArray(parsed["markets"])) {
    return parsed["markets"];
  }

  throw new Error(
    "Market input must be a JSON array or an object with a markets array.",
  );
}

function createStaticGammaClient(
  markets: readonly unknown[],
): PolymarketGammaReadClient {
  return {
    async listMarkets(): Promise<readonly unknown[]> {
      return markets;
    },
  };
}

function normalizeSnapshotList(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (isRecord(value) && Array.isArray(value["snapshots"])) {
    return value["snapshots"];
  }

  throw new Error(
    "Cricket input must be a JSON array or an object with a snapshots array.",
  );
}

function parseSnapshotInput(
  value: unknown,
  index: number,
): CricketSnapshotInput {
  if (!isRecord(value)) {
    throw new Error(`Snapshot ${index} must be a plain object.`);
  }

  const snapshotTime = value["snapshotTime"];
  if (typeof snapshotTime !== "string" || snapshotTime.trim().length === 0) {
    throw new Error(
      `Snapshot ${index} is missing a valid snapshotTime string.`,
    );
  }

  const payload = value["payload"];
  if (!isRecord(payload)) {
    throw new Error(`Snapshot ${index} payload must be a plain object.`);
  }

  return {
    snapshotTime: snapshotTime.trim(),
    payload,
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error: unknown) => {
    const message =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
