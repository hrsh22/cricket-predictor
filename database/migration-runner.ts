import { readdir, readFile } from "node:fs/promises";

import { Client } from "pg";

import {
  MIGRATIONS_DIRECTORY,
  getDatabaseName,
  getDatabaseUrl,
  isSafeResetTarget,
} from "./config.js";

const UP_MIGRATION_SUFFIX = ".up.sql";
const DOWN_MIGRATION_SUFFIX = ".down.sql";

export interface MigrationFile {
  id: string;
  upFilePath: string;
  downFilePath: string;
}

export interface MigrationResult {
  databaseName: string;
  applied: string[];
  skipped: string[];
}

export interface RollbackResult {
  databaseName: string;
  rolledBackMigrationId: string | null;
}

export interface ResetResult {
  databaseName: string;
  applied: string[];
}

export async function loadMigrations(migrationsDirectory = MIGRATIONS_DIRECTORY): Promise<MigrationFile[]> {
  const fileNames = await readdir(migrationsDirectory);
  const upMigrations = fileNames.filter((fileName) => fileName.endsWith(UP_MIGRATION_SUFFIX)).sort();

  return upMigrations.map((upMigrationFileName) => {
    const migrationId = upMigrationFileName.slice(0, -UP_MIGRATION_SUFFIX.length);
    const downMigrationFileName = `${migrationId}${DOWN_MIGRATION_SUFFIX}`;

    if (!fileNames.includes(downMigrationFileName)) {
      throw new Error(`Missing down migration for ${migrationId}. Expected ${downMigrationFileName}.`);
    }

    return {
      id: migrationId,
      upFilePath: `${migrationsDirectory}/${upMigrationFileName}`,
      downFilePath: `${migrationsDirectory}/${downMigrationFileName}`,
    };
  });
}

export async function migrateDatabase(databaseUrl = getDatabaseUrl()): Promise<MigrationResult> {
  const databaseName = getDatabaseName(databaseUrl);
  const migrations = await loadMigrations();
  const client = await connectClient(databaseUrl);

  try {
    await ensureSchemaMigrationsTable(client);

    const appliedMigrationIds = await getAppliedMigrationIds(client);
    const applied: string[] = [];
    const skipped: string[] = [];

    for (const migration of migrations) {
      if (appliedMigrationIds.has(migration.id)) {
        skipped.push(migration.id);
        continue;
      }

      await runSqlFile(client, migration.upFilePath);
      await client.query("insert into schema_migrations (id) values ($1)", [migration.id]);
      applied.push(migration.id);
    }

    return {
      databaseName,
      applied,
      skipped,
    };
  } finally {
    await client.end();
  }
}

export async function rollbackLastMigration(databaseUrl = getDatabaseUrl()): Promise<RollbackResult> {
  const databaseName = getDatabaseName(databaseUrl);
  const migrations = await loadMigrations();
  const client = await connectClient(databaseUrl);

  try {
    await ensureSchemaMigrationsTable(client);

    const latestMigration = await client.query<{ id: string }>(
      "select id from schema_migrations order by applied_at desc, id desc limit 1",
    );
    const appliedMigrationId = latestMigration.rows[0]?.id ?? null;

    if (appliedMigrationId === null) {
      return {
        databaseName,
        rolledBackMigrationId: null,
      };
    }

    const migration = migrations.find(({ id }) => id === appliedMigrationId);

    if (migration === undefined) {
      throw new Error(`Unable to find migration files for applied migration ${appliedMigrationId}.`);
    }

    await runSqlFile(client, migration.downFilePath);
    await client.query("delete from schema_migrations where id = $1", [migration.id]);

    return {
      databaseName,
      rolledBackMigrationId: migration.id,
    };
  } finally {
    await client.end();
  }
}

export async function resetDatabase(databaseUrl = getDatabaseUrl()): Promise<ResetResult> {
  const databaseName = getDatabaseName(databaseUrl);

  if (!isSafeResetTarget(databaseUrl) && process.env["DB_RESET_FORCE"] !== "1") {
    throw new Error(
      `Refusing to reset database \"${databaseName}\". Reset is limited to sports_predictor_mvp or *_test unless DB_RESET_FORCE=1.`,
    );
  }

  const client = await connectClient(databaseUrl);

  try {
    await client.query("drop schema if exists public cascade");
    await client.query("create schema public");
    await client.query("grant all on schema public to current_user");
    await client.query("grant all on schema public to public");
  } finally {
    await client.end();
  }

  const migrationResult = await migrateDatabase(databaseUrl);

  return {
    databaseName,
    applied: migrationResult.applied,
  };
}

async function connectClient(databaseUrl: string): Promise<Client> {
  const client = new Client({ connectionString: databaseUrl });

  try {
    await client.connect();
    return client;
  } catch (error) {
    await client.end().catch(() => undefined);

    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to connect to PostgreSQL database \"${getDatabaseName(databaseUrl)}\": ${message}`);
  }
}

async function ensureSchemaMigrationsTable(client: Client): Promise<void> {
  await client.query(`
    create table if not exists schema_migrations (
      id text primary key,
      applied_at timestamptz not null default now()
    )
  `);
}

async function getAppliedMigrationIds(client: Client): Promise<Set<string>> {
  const result = await client.query<{ id: string }>("select id from schema_migrations order by id asc");

  return new Set(result.rows.map((row) => row.id));
}

async function runSqlFile(client: Client, filePath: string): Promise<void> {
  const sql = await readFile(filePath, "utf8");

  await client.query("begin");

  try {
    await client.query(sql);
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}
