import { getDatabaseUrl } from "../../database/config.js";
import { resetDatabase } from "../../database/migration-runner.js";

async function main(): Promise<void> {
  const result = await resetDatabase(getDatabaseUrl());

  console.log(`Database: ${result.databaseName}`);
  console.log(`Applied migrations after reset: ${result.applied.length}`);

  if (result.applied.length > 0) {
    console.log(`Applied IDs: ${result.applied.join(", ")}`);
  }
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Database reset failed: ${message}`);
  process.exitCode = 1;
});
