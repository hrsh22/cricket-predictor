import type { RecurringRunSummary } from "../orchestration/index.js";

export function formatRecurringRunSummaryReport(
  summary: RecurringRunSummary,
): string {
  const lines = [
    "Live IPL scoring run",
    `Run key: ${summary.runKey}`,
    `Checkpoint: ${summary.checkpointType}`,
    `Triggered by: ${summary.triggeredBy}`,
    `Persisted: ${summary.ingest.marketsPersisted} market snapshots, ${summary.ingest.cricketSnapshotsPersisted} cricket snapshots`,
    `Mappings: ${summary.map.resolvedCount} resolved / ${summary.map.ambiguousCount} ambiguous / ${summary.map.unresolvedCount} unresolved`,
    `Scores: ${summary.score.scoredCount} scored / ${summary.score.skippedCount} skipped`,
  ];

  if (summary.report.noData || summary.report.rows.length === 0) {
    lines.push("", "No scored valuations produced for this run.");
    return lines.join("\n");
  }

  for (const row of summary.report.rows) {
    lines.push("");
    lines.push(...formatValuationCard(row));
  }

  return lines.join("\n");
}

interface ReportRow {
  checkpointType: string;
  matchSlug: string;
  teamAName: string;
  teamBName: string;
  yesOutcomeName: string;
  fairWinProbability: number;
  marketImpliedProbability: number | null;
  spread: number | null;
  modelKey: string;
  modelVersion: string;
  note: string;
  scoredAt: string;
}

function formatValuationCard(row: ReportRow): string[] {
  const yesTeam = row.yesOutcomeName;
  const noTeam = yesTeam === row.teamAName ? row.teamBName : row.teamAName;

  const yesTeamShort = shortenTeamName(yesTeam);
  const noTeamShort = shortenTeamName(noTeam);

  const yesModelProb = row.fairWinProbability;
  const noModelProb = 1 - yesModelProb;

  const yesMarketProb = row.marketImpliedProbability;
  const noMarketProb = yesMarketProb !== null ? 1 - yesMarketProb : null;

  const yesEdge = row.spread;
  const noEdge = yesEdge !== null ? -yesEdge : null;

  const col1Width = 8;
  const col2Width = 8;
  const col3Width = 8;

  const header = `${yesTeam.toUpperCase()} vs ${noTeam.toUpperCase()}`;
  const divider = "━".repeat(Math.max(header.length, 40));

  const lines: string[] = [
    divider,
    header,
    divider,
    "",
    `         │ ${padCenter(yesTeamShort, col2Width)} │ ${padCenter(noTeamShort, col3Width)}`,
    `─────────┼──────────┼──────────`,
    `Market   │ ${padCenter(formatProb(yesMarketProb), col2Width)} │ ${padCenter(formatProb(noMarketProb), col3Width)}`,
    `Model    │ ${padCenter(formatProb(yesModelProb), col2Width)} │ ${padCenter(formatProb(noModelProb), col3Width)}`,
    `─────────┼──────────┼──────────`,
    `Edge     │ ${padCenter(formatEdge(yesEdge), col2Width)} │ ${padCenter(formatEdge(noEdge), col3Width)}`,
    "",
  ];

  const recommendation = buildRecommendation(
    yesTeam,
    noTeam,
    yesEdge,
    yesMarketProb,
  );
  lines.push(...recommendation);

  lines.push("");
  lines.push(`Model: ${row.modelKey} | Checkpoint: ${row.checkpointType}`);
  if (row.note && !row.note.includes("not applied")) {
    lines.push(`Note: ${row.note}`);
  }
  lines.push(divider);

  return lines;
}

function buildRecommendation(
  yesTeam: string,
  noTeam: string,
  yesEdge: number | null,
  yesMarketProb: number | null,
): string[] {
  if (yesEdge === null || yesMarketProb === null) {
    return ["⚠️  NO RECOMMENDATION: Missing market data"];
  }

  const edgePp = Math.abs(yesEdge * 100).toFixed(1);

  if (Math.abs(yesEdge) < 0.01) {
    return ["➖ NO EDGE: Model and market agree (within 1pp)"];
  }

  if (yesEdge > 0) {
    return [
      `✅ RECOMMENDATION: BET ${yesTeam.toUpperCase()}`,
      `   Underpriced by ${edgePp}pp`,
      `   → On Polymarket: Bet YES on "${yesTeam} to win"`,
    ];
  } else {
    return [
      `✅ RECOMMENDATION: BET ${noTeam.toUpperCase()}`,
      `   Underpriced by ${edgePp}pp`,
      `   → On Polymarket: Bet NO on "${yesTeam} to win"`,
    ];
  }
}

function shortenTeamName(name: string): string {
  const abbrevs: Record<string, string> = {
    "Chennai Super Kings": "CSK",
    "Mumbai Indians": "MI",
    "Royal Challengers Bengaluru": "RCB",
    "Royal Challengers Bangalore": "RCB",
    "Kolkata Knight Riders": "KKR",
    "Rajasthan Royals": "RR",
    "Delhi Capitals": "DC",
    "Punjab Kings": "PBKS",
    "Sunrisers Hyderabad": "SRH",
    "Gujarat Titans": "GT",
    "Lucknow Super Giants": "LSG",
  };
  return abbrevs[name] ?? name.slice(0, 4).toUpperCase();
}

function formatProb(value: number | null): string {
  if (value === null) return "n/a";
  return `${(value * 100).toFixed(1)}%`;
}

function formatEdge(value: number | null): string {
  if (value === null) return "n/a";
  const pp = (value * 100).toFixed(1);
  return `${value > 0 ? "+" : ""}${pp}pp`;
}

function padCenter(str: string, width: number): string {
  const padding = width - str.length;
  if (padding <= 0) return str;
  const left = Math.floor(padding / 2);
  const right = padding - left;
  return " ".repeat(left) + str + " ".repeat(right);
}
