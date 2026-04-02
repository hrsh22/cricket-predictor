import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const DEFAULT_DATABASE_URL =
  "postgresql://localhost:5432/sports_predictor_mvp";
export const MIGRATIONS_DIRECTORY = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "migrations",
);
const PROJECT_ENV_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  ".env",
);
let envLoaded = false;

export function parseDatabaseUrl(databaseUrl: string): URL {
  let parsedUrl: URL;

  try {
    parsedUrl = new URL(databaseUrl);
  } catch {
    throw new Error(
      `Invalid DATABASE_URL \"${databaseUrl}\". Expected a PostgreSQL connection string like ${DEFAULT_DATABASE_URL}.`,
    );
  }

  if (!parsedUrl.protocol.startsWith("postgres")) {
    throw new Error(
      `Invalid DATABASE_URL \"${databaseUrl}\". Expected a PostgreSQL connection string like ${DEFAULT_DATABASE_URL}.`,
    );
  }

  const databaseName = decodeURIComponent(
    parsedUrl.pathname.replace(/^\//, ""),
  );

  if (databaseName.length === 0) {
    throw new Error(
      `Invalid DATABASE_URL \"${databaseUrl}\". The connection string must include a database name.`,
    );
  }

  return parsedUrl;
}

export function getDatabaseUrl(env: NodeJS.ProcessEnv = process.env): string {
  ensureProjectEnvLoaded(env);
  const databaseUrl = env["DATABASE_URL"]?.trim() || DEFAULT_DATABASE_URL;

  parseDatabaseUrl(databaseUrl);

  return databaseUrl;
}

export function getDatabaseName(databaseUrl: string): string {
  return decodeURIComponent(
    parseDatabaseUrl(databaseUrl).pathname.replace(/^\//, ""),
  );
}

export function withDatabaseName(
  databaseUrl: string,
  databaseName: string,
): string {
  const parsed = parseDatabaseUrl(databaseUrl);
  parsed.pathname = `/${encodeURIComponent(databaseName)}`;
  return parsed.toString();
}

export function isSafeResetTarget(databaseUrl: string): boolean {
  const databaseName = getDatabaseName(databaseUrl);

  return (
    databaseName === "sports_predictor_mvp" || databaseName.endsWith("_test")
  );
}

function ensureProjectEnvLoaded(env: NodeJS.ProcessEnv): void {
  if (env !== process.env || envLoaded || !existsSync(PROJECT_ENV_PATH)) {
    return;
  }

  const contents = readFileSync(PROJECT_ENV_PATH, "utf8");
  for (const rawLine of contents.split(/\r?\n/u)) {
    const line = rawLine.trim();
    if (line.length === 0 || line.startsWith("#")) {
      continue;
    }

    const equalsIndex = line.indexOf("=");
    if (equalsIndex <= 0) {
      continue;
    }

    const key = line.slice(0, equalsIndex).trim();
    if (key.length === 0 || process.env[key] !== undefined) {
      continue;
    }

    const rawValue = line.slice(equalsIndex + 1).trim();
    process.env[key] = stripWrappingQuotes(rawValue);
  }

  envLoaded = true;
}

function stripWrappingQuotes(value: string): string {
  if (
    value.length >= 2 &&
    ((value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'")))
  ) {
    return value.slice(1, -1);
  }

  return value;
}
