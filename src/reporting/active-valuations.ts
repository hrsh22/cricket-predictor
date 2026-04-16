import type { CheckpointType } from "../domain/checkpoint.js";
import type { JsonObject, JsonValue } from "../domain/primitives.js";
import { isRecord } from "../domain/primitives.js";
import type { SqlExecutor } from "../repositories/postgres.js";

export interface ActiveIplValuationReportItem {
  matchSlug: string;
  checkpoint: {
    type: CheckpointType;
    snapshotTime: string;
    checkpointStateId: number;
  };
  model: {
    key: string;
    family: string;
    version: string;
    registryId: number;
  };
  scoringRunKey: string;
  scoredAt: string;
  fairProbability: number;
  marketProbability: number | null;
  spread: number | null;
  social: {
    applied: boolean | null;
    mode: string | null;
    status: string | null;
    note: string;
    sourceProviderKey: string | null;
    sourceId: string | null;
  };
  tradeThesis: {
    position: "bet_yes" | "bet_no" | "hold";
    outcomeName: string;
    edgeCents: number;
    contractPriceCents: number;
    fairValueCents: number;
    conviction: "fragile" | "tradable" | "strong";
    mispricingSummary: string;
    counterpartySummary: string;
  } | null;
  explanationNote: string;
}

export interface ActiveIplValuationReport {
  generatedAt: string;
  totalCount: number;
  items: ActiveIplValuationReportItem[];
  emptyMessage: string | null;
}

interface ActiveIplValuationRow {
  model_score_id: string | number;
  match_slug: string;
  checkpoint_type: CheckpointType;
  checkpoint_state_id: string | number;
  checkpoint_snapshot_time: Date;
  scoring_run_key: string;
  model_registry_id: string | number;
  model_key: string;
  model_family: string;
  model_version: string;
  scored_at: Date;
  fair_win_probability: string | number;
  market_implied_probability: string | number | null;
  edge: string | number | null;
  score_payload: JsonObject;
}

export async function loadActiveIplValuationReport(
  executor: SqlExecutor,
  generatedAt: string = new Date().toISOString(),
): Promise<ActiveIplValuationReport> {
  const result = await executor.query<ActiveIplValuationRow>(
    `
      select
        ms.id as model_score_id,
        cm.match_slug,
        cs.checkpoint_type,
        cs.id as checkpoint_state_id,
        cs.snapshot_time as checkpoint_snapshot_time,
        sr.run_key as scoring_run_key,
        mr.id as model_registry_id,
        mr.model_key,
        mr.model_family,
        coalesce(
          nullif((ms.score_payload -> 'lineage' ->> 'modelVersion'), ''),
          nullif((ms.score_payload ->> 'modelVersion'), ''),
          mr.version
        ) as model_version,
        ms.scored_at,
        ms.fair_win_probability,
        ms.market_implied_probability,
        ms.edge,
        ms.score_payload
      from model_scores ms
      join model_registry mr on mr.id = ms.model_registry_id
      join scoring_runs sr on sr.id = ms.scoring_run_id
      join checkpoint_states cs on cs.id = ms.checkpoint_state_id
      join canonical_matches cm on cm.id = ms.canonical_match_id
      where cm.competition = 'IPL'
        and mr.is_active = true
        and sr.run_status = 'succeeded'
      order by ms.edge desc nulls last, ms.scored_at desc, ms.id desc
    `,
    [],
  );

  const items = result.rows.map((row) =>
    mapActiveIplValuationRow(row as ActiveIplValuationRow),
  );

  return {
    generatedAt,
    totalCount: items.length,
    items,
    emptyMessage: items.length === 0 ? "No active IPL valuations found." : null,
  };
}

export function formatActiveIplValuationReport(
  report: ActiveIplValuationReport,
): string {
  if (report.items.length === 0) {
    return [
      "Active IPL valuations",
      `Generated at: ${report.generatedAt}`,
      report.emptyMessage ?? "No active IPL valuations found.",
    ].join("\n");
  }

  const lines = [
    "Active IPL valuations",
    `Generated at: ${report.generatedAt}`,
    `Total: ${report.totalCount}`,
    "",
  ];

  for (const [index, item] of report.items.entries()) {
    lines.push(`${index + 1}. ${item.matchSlug}`);
    lines.push(
      `   checkpoint: ${item.checkpoint.type} @ ${item.checkpoint.snapshotTime}`,
    );
    lines.push(
      `   fair: ${formatProbability(item.fairProbability)} | market: ${formatOptionalProbability(item.marketProbability)} | spread: ${formatSpread(item.spread)}`,
    );
    lines.push(
      `   model: ${item.model.key} | version: ${item.model.version} | family: ${item.model.family}`,
    );
    if (item.tradeThesis !== null) {
      lines.push(
        `   thesis: ${formatTradePosition(item.tradeThesis.position)} ${item.tradeThesis.outcomeName} | edge: ${formatCents(item.tradeThesis.edgeCents)} | price: ${formatCents(item.tradeThesis.contractPriceCents)} | fair: ${formatCents(item.tradeThesis.fairValueCents)} | quality: ${item.tradeThesis.conviction}`,
      );
      lines.push(`   why: ${item.tradeThesis.mispricingSummary}`);
      lines.push(`   other side: ${item.tradeThesis.counterpartySummary}`);
    }
    lines.push(
      `   social: ${formatSocialSummary(item.social)} | note: ${item.explanationNote}`,
    );
    lines.push(`   scoredAt: ${item.scoredAt}`);
    if (index < report.items.length - 1) {
      lines.push("");
    }
  }

  return lines.join("\n");
}

function mapActiveIplValuationRow(
  row: ActiveIplValuationRow,
): ActiveIplValuationReportItem {
  const scorePayload = row.score_payload;
  const social = extractSocialMetadata(row.checkpoint_type, scorePayload);
  const tradeThesis = extractTradeThesisMetadata(scorePayload);

  return {
    matchSlug: row.match_slug,
    checkpoint: {
      type: row.checkpoint_type,
      snapshotTime: row.checkpoint_snapshot_time.toISOString(),
      checkpointStateId: Number(row.checkpoint_state_id),
    },
    model: {
      key: row.model_key,
      family: row.model_family,
      version: row.model_version,
      registryId: Number(row.model_registry_id),
    },
    scoringRunKey: row.scoring_run_key,
    scoredAt: row.scored_at.toISOString(),
    fairProbability: Number(row.fair_win_probability),
    marketProbability:
      row.market_implied_probability === null
        ? null
        : Number(row.market_implied_probability),
    spread: row.edge === null ? null : Number(row.edge),
    social,
    tradeThesis,
    explanationNote: social.note,
  };
}

function extractTradeThesisMetadata(
  scorePayload: JsonObject,
): ActiveIplValuationReportItem["tradeThesis"] {
  const tradeThesis = readRecord(scorePayload["tradeThesis"]);
  if (tradeThesis === null) {
    return null;
  }

  const position = tradeThesis["position"];
  const outcomeName = readString(tradeThesis["outcomeName"]);
  const edgeCents = readNumber(tradeThesis["edgeCents"]);
  const contractPriceCents = readNumber(tradeThesis["contractPriceCents"]);
  const fairValueCents = readNumber(tradeThesis["fairValueCents"]);
  const conviction = tradeThesis["conviction"];
  const mispricingSummary = readString(tradeThesis["mispricingSummary"]);
  const counterpartySummary = readString(tradeThesis["counterpartySummary"]);

  if (
    (position !== "bet_yes" && position !== "bet_no" && position !== "hold") ||
    outcomeName === null ||
    edgeCents === null ||
    contractPriceCents === null ||
    fairValueCents === null ||
    (conviction !== "fragile" &&
      conviction !== "tradable" &&
      conviction !== "strong") ||
    mispricingSummary === null ||
    counterpartySummary === null
  ) {
    return null;
  }

  return {
    position,
    outcomeName,
    edgeCents,
    contractPriceCents,
    fairValueCents,
    conviction,
    mispricingSummary,
    counterpartySummary,
  };
}

function extractSocialMetadata(
  checkpointType: CheckpointType,
  scorePayload: JsonObject,
): ActiveIplValuationReportItem["social"] {
  const socialAdjustment = readRecord(scorePayload["socialAdjustment"]);
  if (
    socialAdjustment !== null &&
    typeof socialAdjustment["note"] === "string"
  ) {
    const note = socialAdjustment["note"];
    return {
      applied: socialAdjustment["status"] === "applied",
      mode: readString(socialAdjustment["mode"]),
      status: readString(socialAdjustment["status"]),
      note,
      sourceProviderKey: readStringOrNull(
        socialAdjustment["sourceProviderKey"],
      ),
      sourceId: readStringOrNull(socialAdjustment["sourceId"]),
    };
  }

  if (typeof scorePayload["socialAdjustmentApplied"] === "boolean") {
    const applied = scorePayload["socialAdjustmentApplied"];
    return {
      applied,
      mode: applied ? "enabled" : "disabled",
      status: applied ? "applied" : "not_applicable",
      note: "Post-toss valuation uses the toss-only adjustment surface.",
      sourceProviderKey: null,
      sourceId: null,
    };
  }

  if (typeof scorePayload["socialApplied"] === "boolean") {
    const applied = scorePayload["socialApplied"];
    const socialAdjustmentValue = readNumber(scorePayload["socialAdjustment"]);
    return {
      applied,
      mode: applied ? "enabled" : "disabled",
      status: applied ? "applied" : "not_applicable",
      note:
        applied && socialAdjustmentValue !== null
          ? `Innings-break social adjustment ${formatSignedSpread(socialAdjustmentValue)}.`
          : "Innings-break valuation without social adjustment.",
      sourceProviderKey: null,
      sourceId: null,
    };
  }

  return {
    applied: null,
    mode: checkpointType === "pre_match" ? "enabled" : "disabled",
    status: checkpointType === "pre_match" ? "applied" : "not_applicable",
    note: "No social metadata available in the stored valuation payload.",
    sourceProviderKey: null,
    sourceId: null,
  };
}

function formatSocialSummary(
  social: ActiveIplValuationReportItem["social"],
): string {
  const mode = social.mode ?? "unknown";
  const status = social.status ?? "unknown";
  return `${mode}/${status}`;
}

function formatProbability(value: number): string {
  return `${toPercent(value)}%`;
}

function formatOptionalProbability(value: number | null): string {
  return value === null ? "n/a" : `${toPercent(value)}%`;
}

function formatSpread(value: number | null): string {
  return value === null ? "n/a" : `${formatSignedSpread(value)}pp`;
}

function formatSignedSpread(value: number): string {
  const points = toPercent(value);
  return `${value > 0 ? "+" : ""}${points}`;
}

function formatTradePosition(position: "bet_yes" | "bet_no" | "hold"): string {
  if (position === "bet_yes") {
    return "BET YES";
  }

  if (position === "bet_no") {
    return "BET NO";
  }

  return "HOLD";
}

function formatCents(value: number): string {
  return `${value.toFixed(1)}c`;
}

function toPercent(value: number): string {
  return (value * 100).toFixed(1);
}

function readRecord(value: JsonValue | undefined): JsonObject | null {
  return isRecord(value) ? (value as JsonObject) : null;
}

function readString(value: JsonValue | undefined): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readStringOrNull(value: JsonValue | undefined): string | null {
  if (value === null) {
    return null;
  }

  return readString(value);
}

function readNumber(value: JsonValue | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
