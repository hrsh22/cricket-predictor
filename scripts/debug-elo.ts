import { Pool } from "pg";
import { loadAppConfig } from "../src/config/index.js";
import { debugFeatureContext } from "../src/features/context-builder.js";

async function main(): Promise<void> {
  const config = loadAppConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });

  try {
    await debugFeatureContext(pool);
  } finally {
    await pool.end();
  }
}

void main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
