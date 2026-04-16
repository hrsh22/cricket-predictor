import { loadAppConfig } from "../../src/config/index.js";
import { runOpticOddsBallByBallIngestion } from "../../src/ingest/opticodds/index.js";
import { createRepositorySet } from "../../src/repositories/index.js";

async function main(): Promise<void> {
  const runOnce = process.argv.slice(2).includes("--once");
  const config = loadAppConfig();
  const repositories = createRepositorySet(config);

  try {
    if (runOnce) {
      const summary = await runOpticOddsBallByBallIngestion({
        repository: repositories.opticodds,
        config: config.opticOdds,
        once: true,
      });
      process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
      return;
    }

    const abortController = new AbortController();
    const stop = () => abortController.abort();
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);

    process.stdout.write(
      `${JSON.stringify(
        {
          mode: "continuous",
          seasonYear: config.opticOdds.seasonYear,
          leagueId: config.opticOdds.leagueId,
          sportsbookIds: config.opticOdds.sportsbookIds,
          marketIds: config.opticOdds.marketIds,
        },
        null,
        2,
      )}\n`,
    );

    await runOpticOddsBallByBallIngestion({
      repository: repositories.opticodds,
      config: config.opticOdds,
      signal: abortController.signal,
    });
  } finally {
    await repositories.close();
  }
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
