import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { isRecord } from "../src/domain/primitives.js";
import {
  fetchLiveCricketSnapshots,
  type CricketSnapshotInput,
} from "../src/ingest/cricket/index.js";
import type { PolymarketGammaReadClient } from "../src/ingest/polymarket/index.js";
import { loadAppConfig } from "../src/config/index.js";
import {
  runRecurringPipeline,
  type RecurringRunRequest,
} from "../src/orchestration/index.js";
import { formatRecurringRunSummaryReport } from "../src/reporting/index.js";
import { createPgPool } from "../src/repositories/index.js";
import { buildFeatureContextFromHistory } from "../src/features/context-builder.js";

async function main(): Promise<void> {
  const options = await parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const featureContext = await buildFeatureContextFromHistory(
      pool,
      new Date(),
    );

    const requestWithContext: RecurringRunRequest = {
      ...options.request,
      featureContextByMatchSlug: {
        "*": featureContext,
      },
    };

    const summary = await runRecurringPipeline(pool, requestWithContext);
    const output = options.useJsonOutput
      ? `${JSON.stringify(summary, null, 2)}\n`
      : `${formatRecurringRunSummaryReport(summary)}\n`;

    process.stdout.write(output);
  } finally {
    await pool.end();
  }
}

interface ScoreCliOptions {
  request: RecurringRunRequest;
  useJsonOutput: boolean;
  nextMatchOnly: boolean;
}

async function parseCliArgs(argv: readonly string[]): Promise<ScoreCliOptions> {
  let checkpointType: RecurringRunRequest["checkpointType"] | null = null;
  let runKey: string | undefined;
  let triggeredBy: string | undefined;
  let marketInputPath: string | undefined;
  let cricketInputPath: string | undefined;
  let useLiveCricket = false;
  let useJsonOutput = false;
  let nextMatchOnly = true;

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

    if (argument === "--json" || argument === "--format=json") {
      useJsonOutput = true;
      continue;
    }

    if (argument === "--all-matches") {
      nextMatchOnly = false;
      continue;
    }

    if (argument === "--next-match-only") {
      nextMatchOnly = true;
      continue;
    }

    if (argument === "--help") {
      process.stdout.write(
        [
          "Usage: pnpm tsx scripts/score.ts [--checkpoint <pre_match|post_toss|innings_break>] [--cricket-input <path>] [--market-input <path>] [--run-key <key>] [--triggered-by <value>] [--json] [--all-matches]",
          "",
          "Defaults to --checkpoint pre_match and analyzes only the next upcoming match.",
          "Use --all-matches to analyze all matches within the time window.",
          "",
          "When --market-input is omitted, Polymarket is fetched live and the fetched snapshots are still persisted before scoring.",
          "When --cricket-input is omitted, live cricket snapshots are fetched from ESPN Cricinfo.",
        ].join("\n") + "\n",
      );
      process.exit(0);
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --checkpoint, --run-key, --triggered-by, --market-input, --cricket-input, --live-cricket, --all-matches, or --json.`,
    );
  }

  const effectiveCheckpointType = checkpointType ?? "pre_match";

  const config = loadAppConfig();

  const request: RecurringRunRequest = {
    checkpointType: effectiveCheckpointType,
    ...(runKey === undefined ? {} : { runKey }),
    triggeredBy: triggeredBy ?? "manual",
    marketIngestion: {
      trigger: "manual",
    },
    cricketIngestion: {
      snapshots:
        cricketInputPath !== undefined
          ? await loadCricketSnapshots(cricketInputPath)
          : await fetchLiveCricketSnapshots({
              config: {
                provider: config.cricketLive.provider,
                apiKey: config.cricketLive.apiKey ?? "",
                baseUrl: config.cricketLive.baseUrl,
              },
              nextMatchOnly,
            }),
    },
  };

  if (useLiveCricket) {
    request.triggeredBy = triggeredBy ?? "manual_live";
  }

  if (marketInputPath !== undefined) {
    const markets = await loadMarketSnapshots(marketInputPath);
    request.marketIngestion = {
      trigger: "manual",
      gammaClient: createStaticGammaClient(markets),
    };
  }

  return {
    request,
    useJsonOutput,
    nextMatchOnly,
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

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
