import { runPolymarketIplWinnerIngestion } from "../../src/ingest/polymarket/index.js";

async function main(): Promise<void> {
  const trigger = parseTrigger(process.argv.slice(2));
  const summary = await runPolymarketIplWinnerIngestion({ trigger });
  const output = {
    ...summary,
    snapshots: summary.snapshots.map((snapshot) => ({
      id: snapshot.id,
      sourceMarketId: snapshot.sourceMarketId,
      marketSlug: snapshot.marketSlug,
      snapshotTime: snapshot.snapshotTime,
      dedupeKey: String(snapshot.payload["dedupeKey"]),
      createdAt: snapshot.createdAt,
    })),
  };

  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
}

function parseTrigger(
  argumentsList: readonly string[],
): "manual" | "scheduled" {
  for (const argument of argumentsList) {
    if (!argument.startsWith("--trigger=")) {
      continue;
    }

    const value = argument.slice("--trigger=".length).trim().toLowerCase();
    if (value === "manual" || value === "scheduled") {
      return value;
    }

    throw new Error(
      `Unsupported trigger "${value}". Expected manual or scheduled.`,
    );
  }

  return "manual";
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
