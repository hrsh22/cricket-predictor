import { loadAppConfig } from "../src/config/index.js";
import { createPgPool, closePgPool } from "../src/repositories/postgres.js";
import {
  formatActiveIplValuationReport,
  loadActiveIplValuationReport,
} from "../src/reporting/index.js";

const useJsonOutput =
  process.argv.includes("--json") || process.argv.includes("--format=json");

async function main(): Promise<void> {
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const report = await loadActiveIplValuationReport(pool);
    const output = useJsonOutput
      ? `${JSON.stringify(report, null, 2)}\n`
      : `${formatActiveIplValuationReport(report)}\n`;

    process.stdout.write(output);
  } finally {
    await closePgPool(pool);
  }
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
