import { fileURLToPath } from "node:url";

import { loadAppConfig } from "../src/config/index.js";
import { buildFeatureContextFromHistory } from "../src/features/context-builder.js";
import { fetchLiveCricketSnapshots } from "../src/ingest/cricket/index.js";
import {
  runRecurringPipeline,
  type RecurringRunReportRow,
  type RecurringRunSummary,
} from "../src/orchestration/index.js";
import { createPgPool } from "../src/repositories/index.js";

interface TradeThesisView {
  position: "bet_yes" | "bet_no" | "hold";
  outcomeName: string;
  edgeCents: number;
  contractPriceCents: number;
  fairValueCents: number;
  conviction: "fragile" | "tradable" | "strong";
  mispricingSummary: string;
  counterpartySummary: string;
}

interface NextMatchPredictionView {
  matchSlug: string;
  teamAName: string;
  teamBName: string;
  modelKey: string;
  modelVersion: string;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  spread: number | null;
  scoredAt: string;
  recommendation: string;
  tradeThesis: TradeThesisView | null;
}

async function main(): Promise<void> {
  const useJsonOutput =
    process.argv.includes("--json") || process.argv.includes("--format=json");
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const featureContext = await buildFeatureContextFromHistory(
      pool,
      new Date(),
    );
    const summary = await runRecurringPipeline(pool, {
      checkpointType: "pre_match",
      triggeredBy: "manual_next_match",
      marketIngestion: {
        trigger: "manual",
      },
      cricketIngestion: {
        snapshots: await fetchLiveCricketSnapshots({
          config: {
            provider: config.cricketLive.provider,
            apiKey: config.cricketLive.apiKey ?? "",
            baseUrl: config.cricketLive.baseUrl,
          },
          nextMatchOnly: true,
        }),
      },
      featureContextByMatchSlug: {
        "*": featureContext,
      },
    });

    const prediction = buildNextMatchPredictionView(summary);
    if (prediction === null) {
      process.stdout.write("No upcoming IPL match prediction available.\n");
      return;
    }

    process.stdout.write(
      useJsonOutput
        ? `${JSON.stringify(prediction, null, 2)}\n`
        : `${formatNextMatchPrediction(prediction)}\n`,
    );
  } finally {
    await pool.end();
  }
}

export function buildNextMatchPredictionView(
  summary: RecurringRunSummary,
): NextMatchPredictionView | null {
  const row = summary.report.rows[0];
  if (row === undefined) {
    return null;
  }

  return {
    matchSlug: row.matchSlug,
    teamAName: row.teamAName,
    teamBName: row.teamBName,
    modelKey: row.modelKey,
    modelVersion: row.modelVersion,
    fairWinProbability: row.fairWinProbability,
    marketImpliedProbability: row.marketImpliedProbability,
    spread: row.spread,
    scoredAt: row.scoredAt,
    recommendation: buildRecommendation(row),
    tradeThesis: normalizeTradeThesis(row),
  };
}

export function formatNextMatchPrediction(
  prediction: NextMatchPredictionView,
): string {
  const lines = [
    "Next IPL match prediction",
    `Match: ${prediction.teamAName} vs ${prediction.teamBName}`,
    `Recommendation: ${prediction.recommendation}`,
    `Model: ${prediction.modelKey} (${prediction.modelVersion})`,
    `Fair: ${formatProbability(prediction.fairWinProbability)} | Market: ${formatOptionalProbability(prediction.marketImpliedProbability)} | Spread: ${formatSpread(prediction.spread)}`,
  ];

  if (prediction.tradeThesis !== null) {
    lines.push(
      `Edge: ${formatCents(prediction.tradeThesis.edgeCents)} | Price: ${formatCents(prediction.tradeThesis.contractPriceCents)} | Fair value: ${formatCents(prediction.tradeThesis.fairValueCents)} | Quality: ${prediction.tradeThesis.conviction.toUpperCase()}`,
    );
    lines.push(`Why: ${prediction.tradeThesis.mispricingSummary}`);
    lines.push(`Other side: ${prediction.tradeThesis.counterpartySummary}`);
  }

  lines.push(`Scored at: ${prediction.scoredAt}`);
  return lines.join("\n");
}

function buildRecommendation(row: RecurringRunReportRow): string {
  const tradeThesis = normalizeTradeThesis(row);
  if (tradeThesis !== null) {
    if (tradeThesis.position === "bet_yes") {
      return `BET ${tradeThesis.outcomeName.toUpperCase()}`;
    }

    if (tradeThesis.position === "bet_no") {
      return `BET ${tradeThesis.outcomeName.toUpperCase()}`;
    }

    return "NO TRADE";
  }

  if (row.spread === null || Math.abs(row.spread) < 0.01) {
    return "NO TRADE";
  }

  return row.spread > 0
    ? `BET ${row.yesOutcomeName.toUpperCase()}`
    : `BET ${resolveOpposingTeam(row).toUpperCase()}`;
}

function normalizeTradeThesis(
  row: RecurringRunReportRow,
): NextMatchPredictionView["tradeThesis"] {
  return row.tradeThesis === undefined ? null : row.tradeThesis;
}

function resolveOpposingTeam(row: RecurringRunReportRow): string {
  return row.yesOutcomeName === row.teamAName ? row.teamBName : row.teamAName;
}

function formatProbability(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function formatOptionalProbability(value: number | null): string {
  return value === null ? "n/a" : formatProbability(value);
}

function formatSpread(value: number | null): string {
  if (value === null) {
    return "n/a";
  }

  const points = (value * 100).toFixed(1);
  return `${value > 0 ? "+" : ""}${points}pp`;
}

function formatCents(value: number): string {
  return `${value.toFixed(1)}c`;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error: unknown) => {
    const message =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
