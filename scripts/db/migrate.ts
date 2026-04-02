import { getDatabaseUrl } from "../../database/config.js";
import { migrateDatabase } from "../../database/migration-runner.js";

async function main(): Promise<void> {
  const result = await migrateDatabase(getDatabaseUrl());

  console.log(`Database: ${result.databaseName}`);
  console.log(`Applied migrations: ${result.applied.length}`);
  console.log(`Skipped migrations: ${result.skipped.length}`);

  if (result.applied.length > 0) {
    console.log(`Applied IDs: ${result.applied.join(", ")}`);
  }
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Database migration failed: ${message}`);
  process.exitCode = 1;
});
