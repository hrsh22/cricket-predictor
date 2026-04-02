import { getDatabaseUrl } from "../../database/config.js";
import { rollbackLastMigration } from "../../database/migration-runner.js";

async function main(): Promise<void> {
  const result = await rollbackLastMigration(getDatabaseUrl());

  console.log(`Database: ${result.databaseName}`);

  if (result.rolledBackMigrationId === null) {
    console.log("No migrations were applied.");
    return;
  }

  console.log(`Rolled back migration: ${result.rolledBackMigrationId}`);
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Database rollback failed: ${message}`);
  process.exitCode = 1;
});
