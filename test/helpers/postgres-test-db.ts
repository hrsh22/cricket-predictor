import { Client } from "pg";

import {
  getDatabaseName,
  getDatabaseUrl,
  parseDatabaseUrl,
  withDatabaseName,
} from "../../database/config.js";

export async function ensureTestDatabaseExists(
  databaseUrl: string,
): Promise<void> {
  const parsed = parseDatabaseUrl(databaseUrl);
  const databaseName = getDatabaseName(databaseUrl);
  const adminUrl = new URL(parsed.toString());
  adminUrl.pathname = "/postgres";

  const client = new Client({ connectionString: adminUrl.toString() });

  try {
    await client.connect();
    const exists = await client.query<{ exists: boolean }>(
      "select exists(select 1 from pg_database where datname = $1) as exists",
      [databaseName],
    );

    if (exists.rows[0]?.exists === true) {
      return;
    }

    const escapedName = quoteIdentifier(databaseName);
    await client.query(`create database ${escapedName}`);
  } finally {
    await client.end().catch(() => undefined);
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/gu, '""')}"`;
}

export function buildTestDatabaseUrl(databaseName: string): string {
  return withDatabaseName(getDatabaseUrl(), databaseName);
}
