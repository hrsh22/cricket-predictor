import { loadAppConfig } from "../../src/config/index.js";
import { runPolymarketHistoricalBackfill } from "../../src/ingest/polymarket/index.js";

interface CliOptions {
  eventSlug: string;
  marketTypes?: string[];
  startTs?: number;
  endTs?: number;
  fidelityMinutes?: number;
  tradePageSize?: number;
  dryRun: boolean;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const summary = await runPolymarketHistoricalBackfill({
    eventSlug: options.eventSlug,
    ...(options.dryRun ? { dryRun: true } : {}),
    ...(options.marketTypes === undefined
      ? {}
      : { marketTypes: options.marketTypes }),
    ...(options.startTs === undefined ? {} : { startTs: options.startTs }),
    ...(options.endTs === undefined ? {} : { endTs: options.endTs }),
    ...(options.fidelityMinutes === undefined
      ? {}
      : { fidelityMinutes: options.fidelityMinutes }),
    ...(options.tradePageSize === undefined
      ? {}
      : { tradePageSize: options.tradePageSize }),
  });

  process.stdout.write(
    `${JSON.stringify(
      {
        dryRun: options.dryRun,
        ...(options.dryRun
          ? {}
          : { databaseName: loadAppConfig().databaseName }),
        eventSlug: summary.eventSlug,
        sourceEventId: summary.sourceEventId,
        selectedMarketCount: summary.selectedMarketCount,
        selectedTokenCount: summary.selectedTokenCount,
        fetchedTradeCount: summary.fetchedTradeCount,
        persistedTradeCount: summary.persistedTradeCount,
        tradeWarnings: summary.tradeWarnings,
        fetchedPricePointCount: summary.fetchedPricePointCount,
        persistedPricePointCount: summary.persistedPricePointCount,
        queryWindow: summary.queryWindow,
        markets: summary.markets,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let eventSlug: string | null = null;
  let marketTypes: string[] | undefined;
  let startTs: number | undefined;
  let endTs: number | undefined;
  let fidelityMinutes: number | undefined;
  let tradePageSize: number | undefined;
  let dryRun = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === "--event-slug") {
      eventSlug = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (argument === "--market-types") {
      const raw = argv[index + 1] ?? "";
      marketTypes = raw
        .split(",")
        .map((value) => value.trim())
        .filter((value) => value.length > 0);
      index += 1;
      continue;
    }

    if (argument === "--start-ts") {
      startTs = parseIntegerArg(argv[index + 1], "--start-ts");
      index += 1;
      continue;
    }

    if (argument === "--end-ts") {
      endTs = parseIntegerArg(argv[index + 1], "--end-ts");
      index += 1;
      continue;
    }

    if (argument === "--fidelity-minutes") {
      fidelityMinutes = parseIntegerArg(argv[index + 1], "--fidelity-minutes");
      index += 1;
      continue;
    }

    if (argument === "--trade-page-size") {
      tradePageSize = parseIntegerArg(argv[index + 1], "--trade-page-size");
      index += 1;
      continue;
    }

    if (argument === "--dry-run") {
      dryRun = true;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --event-slug <slug> and optional --market-types <csv>, --start-ts <unix>, --end-ts <unix>, --fidelity-minutes <int>, --trade-page-size <int>, --dry-run.`,
    );
  }

  if (eventSlug === null || eventSlug.trim().length === 0) {
    throw new Error("--event-slug is required.");
  }

  return {
    eventSlug: eventSlug.trim(),
    ...(marketTypes === undefined || marketTypes.length === 0
      ? {}
      : { marketTypes }),
    ...(startTs === undefined ? {} : { startTs }),
    ...(endTs === undefined ? {} : { endTs }),
    ...(fidelityMinutes === undefined ? {} : { fidelityMinutes }),
    ...(tradePageSize === undefined ? {} : { tradePageSize }),
    dryRun,
  };
}

function parseIntegerArg(value: string | undefined, flag: string): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} expects an integer value.`);
  }

  return parsed;
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
