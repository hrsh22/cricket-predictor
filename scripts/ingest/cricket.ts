import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { isRecord } from "../../src/domain/primitives.js";
import { loadAppConfig } from "../../src/config/index.js";
import {
  fetchLiveCricketSnapshots,
  ingestCricketSnapshots,
  type CricketProviderKey,
  type CricketSnapshotInput,
} from "../../src/ingest/cricket/index.js";
import { createRepositorySet } from "../../src/repositories/index.js";

interface CliOptions {
  inputPath: string | null;
  provider: CricketProviderKey;
  live: boolean;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const snapshots =
    options.live || options.inputPath === null
      ? await fetchLiveCricketSnapshots({
          config: {
            provider: config.cricketLive.provider,
            apiKey: config.cricketLive.apiKey ?? "",
            baseUrl: config.cricketLive.baseUrl,
          },
        })
      : await loadSnapshotFile(options.inputPath);
  const repositories = createRepositorySet(config);

  try {
    const summary = await ingestCricketSnapshots(
      repositories,
      snapshots,
      options.provider,
    );

    console.log(
      JSON.stringify(
        {
          provider: summary.provider,
          totalSnapshots: summary.totalSnapshots,
          normalizedSnapshots: summary.normalizedSnapshots,
          degradedSnapshots: summary.degradedSnapshots,
          checkpointSnapshots: summary.checkpointSnapshots,
          finalResultSnapshots: summary.finalResultSnapshots,
          results: summary.results.map((result) => ({
            snapshotTime: result.snapshotTime,
            sourceMatchId: result.sourceMatchId,
            lifecycle: result.lifecycle,
            status: result.status,
            rawSnapshotId: result.rawSnapshot.id,
            canonicalMatchId: result.canonicalMatch?.id ?? null,
            checkpointId: result.checkpoint?.id ?? null,
            degradationReason: result.degradationReason,
            issues: result.issues,
          })),
        },
        null,
        2,
      ),
    );
  } finally {
    await repositories.close();
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath: string | null = null;
  let provider: CricketProviderKey = "cricapi";
  let live = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--input") {
      inputPath = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--provider") {
      const value = argv[index + 1] ?? null;
      if (value !== "cricapi") {
        throw new Error(
          `Unsupported cricket provider "${value ?? ""}". Expected cricapi.`,
        );
      }

      provider = value;
      index += 1;
      continue;
    }

    if (argument === "--live") {
      live = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --input <file>, --live, and optionally --provider cricapi.`,
    );
  }

  if (inputPath === null && !live) {
    throw new Error("Missing required --input <file> argument or use --live.");
  }

  return {
    inputPath: inputPath === null ? null : resolve(process.cwd(), inputPath),
    provider,
    live,
  };
}

async function loadSnapshotFile(
  inputPath: string,
): Promise<readonly CricketSnapshotInput[]> {
  const fileContent = await readFile(inputPath, "utf8");
  const parsed = JSON.parse(fileContent) as unknown;
  const snapshotList = normalizeSnapshotList(parsed);

  return snapshotList.map((entry, index) => parseSnapshotInput(entry, index));
}

function normalizeSnapshotList(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (isRecord(value) && Array.isArray(value["snapshots"])) {
    return value["snapshots"];
  }

  throw new Error(
    "Cricket ingest input must be a JSON array or an object with a snapshots array.",
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

  const stateVersionValue = value["stateVersion"];
  const stateVersion =
    typeof stateVersionValue === "number" &&
    Number.isInteger(stateVersionValue) &&
    stateVersionValue > 0
      ? stateVersionValue
      : undefined;

  const parsedSnapshot: CricketSnapshotInput = {
    snapshotTime: snapshotTime.trim(),
    payload,
  };

  if (stateVersion !== undefined) {
    parsedSnapshot.stateVersion = stateVersion;
  }

  return parsedSnapshot;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Cricket ingest failed: ${message}`);
  process.exitCode = 1;
});
