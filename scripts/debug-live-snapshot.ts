import { loadAppConfig } from "../src/config/index.js";
import { fetchLiveCricketSnapshots } from "../src/ingest/cricket/index.js";

async function main(): Promise<void> {
  const config = loadAppConfig();
  const snapshots = await fetchLiveCricketSnapshots({
    config: {
      provider: config.cricketLive.provider,
      apiKey: config.cricketLive.apiKey ?? "",
      baseUrl: config.cricketLive.baseUrl,
    },
    nextMatchOnly: true,
  });

  const first = snapshots[0];
  if (first === undefined) {
    console.log("no snapshots");
    return;
  }

  const payload = first.payload as Record<string, unknown>;
  const score = Array.isArray(payload["score"]) ? payload["score"] : [];
  console.log(
    JSON.stringify(
      {
        provider: config.cricketLive.provider,
        snapshotTime: first.snapshotTime,
        id: payload["id"],
        name: payload["name"],
        status: payload["status"],
        date: payload["date"],
        tossWinner: payload["tossWinner"],
        tossChoice: payload["tossChoice"],
        matchWinner: payload["matchWinner"],
        scoreLen: score.length,
        firstScore: score[0] ?? null,
      },
      null,
      2,
    ),
  );
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  console.error(message);
  process.exitCode = 1;
});
