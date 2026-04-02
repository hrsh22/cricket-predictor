import { fileURLToPath } from "node:url";

import { loadAppConfig } from "../../src/config/index.js";
import { createModelingRepository } from "../../src/repositories/modeling.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";
import type { CheckpointType } from "../../src/domain/checkpoint.js";

interface CliOptions {
  modelKey: string;
  checkpointType: CheckpointType;
  modelFamily: string;
  version: string;
  trainingWindow: string | null;
  isActive: boolean;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const modeling = createModelingRepository(pool);
    const now = new Date().toISOString();

    const saved = await modeling.saveModelRegistry({
      modelKey: options.modelKey,
      checkpointType: options.checkpointType,
      modelFamily: options.modelFamily,
      version: options.version,
      trainingWindow: options.trainingWindow,
      isActive: options.isActive,
      metadata: {
        source: "register-model-script",
      },
      createdAt: now,
    });

    process.stdout.write(
      `${JSON.stringify(
        {
          id: saved.id,
          modelKey: saved.modelKey,
          checkpointType: saved.checkpointType,
          modelFamily: saved.modelFamily,
          version: saved.version,
          trainingWindow: saved.trainingWindow,
          isActive: saved.isActive,
          createdAt: saved.createdAt,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let modelKey: string | null = null;
  let checkpointType: CheckpointType | null = null;
  let modelFamily: string | null = null;
  let version: string | null = null;
  let trainingWindow: string | null = null;
  let isActive = true;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--model-key") {
      modelKey = readStringArg("--model-key", argv[index + 1]);
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

    if (argument === "--model-family") {
      modelFamily = readStringArg("--model-family", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--version") {
      version = readStringArg("--version", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--training-window") {
      trainingWindow = readStringArg("--training-window", argv[index + 1]);
      index += 1;
      continue;
    }

    if (argument === "--inactive") {
      isActive = false;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --model-key, --checkpoint, --model-family, --version, optional --training-window, --inactive.`,
    );
  }

  if (modelKey === null) {
    throw new Error("Missing required --model-key <value> argument.");
  }

  if (checkpointType === null) {
    throw new Error("Missing required --checkpoint <value> argument.");
  }

  if (modelFamily === null) {
    throw new Error("Missing required --model-family <value> argument.");
  }

  if (version === null) {
    throw new Error("Missing required --version <value> argument.");
  }

  return {
    modelKey,
    checkpointType,
    modelFamily,
    version,
    trainingWindow,
    isActive,
  };
}

function readStringArg(flag: string, value: string | undefined): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${flag} requires a non-empty value.`);
  }

  return value.trim();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
