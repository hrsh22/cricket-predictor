import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { generateBallOddsTimeline } from "./ball-odds-timeline.js";

interface CliOptions {
  eventSlug: string;
  commentaryUrl: string;
  outputPath: string;
  allowPartial: boolean;
}

interface ReactionRow {
  inning: number;
  ball: string;
  battingTeam: string;
  bowlingTeam: string;
  event: string;
  signal: string;
  previousEvent: string;
  eventPair: string;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
  wicketKind: string | null;
  isBoundary: boolean;
  isFour: boolean;
  isSix: boolean;
  isDotBall: boolean;
  isExtra: boolean;
  boundaryStreakBefore: number;
  boundaryStreakAfter: number;
  sixStreakBefore: number;
  sixStreakAfter: number;
  dotStreakBefore: number;
  dotStreakAfter: number;
  ballsSincePreviousWicket: number | null;
  ballsSincePreviousBoundary: number | null;
  battingTeamBeforePct: number | null;
  battingTeamAfterPct: number | null;
  battingTeamDeltaPct: number | null;
  battingTeamAbsDeltaPct: number | null;
  fieldingTeamBeforePct: number | null;
  fieldingTeamAfterPct: number | null;
  fieldingTeamDeltaPct: number | null;
  pricingSourceBefore: string | null;
  pricingSourceAfter: string | null;
  timestamp: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const timeline = await generateBallOddsTimeline({
    eventSlug: options.eventSlug,
    commentaryUrl: options.commentaryUrl,
    allowPartial: options.allowPartial,
  });

  const rows = buildReactionRows(timeline.rows);
  await mkdir(dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, toCsv(rows), "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        outputPath: options.outputPath,
        eventSlug: options.eventSlug,
        deliverySourceMode: timeline.deliverySourceMode,
        rowCount: rows.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let eventSlug: string | null = null;
  let commentaryUrl: string | null = null;
  let outputPath: string | null = null;
  let allowPartial = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--event-slug") {
      eventSlug = argv[index + 1] ?? null;
      index += 1;
      continue;
    }
    if (argument === "--commentary-url") {
      commentaryUrl = argv[index + 1] ?? null;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? null;
      index += 1;
      continue;
    }
    if (argument === "--allow-partial") {
      allowPartial = true;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --event-slug, --commentary-url, optional --output, --allow-partial.`,
    );
  }

  if (eventSlug === null || eventSlug.trim().length === 0) {
    throw new Error("--event-slug is required.");
  }
  if (commentaryUrl === null || commentaryUrl.trim().length === 0) {
    throw new Error("--commentary-url is required.");
  }

  return {
    eventSlug: eventSlug.trim(),
    commentaryUrl: commentaryUrl.trim(),
    outputPath: outputPath?.trim().length
      ? outputPath.trim()
      : `data/${eventSlug.trim()}-reaction.csv`,
    allowPartial,
  };
}

function buildReactionRows(
  rows: Awaited<ReturnType<typeof generateBallOddsTimeline>>["rows"],
): ReactionRow[] {
  const reactionRows: ReactionRow[] = [];
  let currentInning: number | null = null;
  let previousEvent = "";
  let boundaryStreak = 0;
  let sixStreak = 0;
  let dotStreak = 0;
  let lastWicketIndex: number | null = null;
  let lastBoundaryIndex: number | null = null;

  for (const [index, row] of rows.entries()) {
    if (currentInning !== row.inning) {
      currentInning = row.inning;
      previousEvent = "";
      boundaryStreak = 0;
      sixStreak = 0;
      dotStreak = 0;
      lastWicketIndex = null;
      lastBoundaryIndex = null;
    }

    const isBoundary = row.batsmanRuns === 4 || row.batsmanRuns === 6;
    const isFour = row.batsmanRuns === 4;
    const isSix = row.batsmanRuns === 6;
    const isDotBall = row.runsTotal === 0;
    const isExtra = row.wides + row.noballs + row.byes + row.legbyes > 0;

    const boundaryStreakBefore = boundaryStreak;
    const sixStreakBefore = sixStreak;
    const dotStreakBefore = dotStreak;

    boundaryStreak = isBoundary ? boundaryStreak + 1 : 0;
    sixStreak = isSix ? sixStreak + 1 : 0;
    dotStreak = isDotBall ? dotStreak + 1 : 0;

    const battingBefore =
      row.primaryTeam === row.battingTeam
        ? row.primaryBefore
        : row.secondaryBefore;
    const battingAfter =
      row.primaryTeam === row.battingTeam
        ? row.primaryAfter
        : row.secondaryAfter;
    const battingDelta =
      row.primaryTeam === row.battingTeam
        ? row.primaryDelta
        : row.secondaryDelta;
    const fieldingBefore =
      row.primaryTeam === row.battingTeam
        ? row.secondaryBefore
        : row.primaryBefore;
    const fieldingAfter =
      row.primaryTeam === row.battingTeam
        ? row.secondaryAfter
        : row.primaryAfter;
    const fieldingDelta =
      row.primaryTeam === row.battingTeam
        ? row.secondaryDelta
        : row.primaryDelta;

    reactionRows.push({
      inning: row.inning,
      ball: row.ball,
      battingTeam: row.battingTeam,
      bowlingTeam: row.bowlingTeam,
      event: row.event,
      signal: classifySignal({
        isWicket: row.isWicket,
        isSix,
        isFour,
        isBoundary,
        isDotBall,
        isExtra,
        runsTotal: row.runsTotal,
      }),
      previousEvent,
      eventPair:
        previousEvent.length === 0
          ? row.event
          : `${previousEvent}>${row.event}`,
      runsTotal: row.runsTotal,
      batsmanRuns: row.batsmanRuns,
      wides: row.wides,
      noballs: row.noballs,
      byes: row.byes,
      legbyes: row.legbyes,
      isWicket: row.isWicket,
      wicketKind: row.wicketKind,
      isBoundary,
      isFour,
      isSix,
      isDotBall,
      isExtra,
      boundaryStreakBefore,
      boundaryStreakAfter: boundaryStreak,
      sixStreakBefore,
      sixStreakAfter: sixStreak,
      dotStreakBefore,
      dotStreakAfter: dotStreak,
      ballsSincePreviousWicket:
        lastWicketIndex === null ? null : index - lastWicketIndex,
      ballsSincePreviousBoundary:
        lastBoundaryIndex === null ? null : index - lastBoundaryIndex,
      battingTeamBeforePct: battingBefore,
      battingTeamAfterPct: battingAfter,
      battingTeamDeltaPct: battingDelta,
      battingTeamAbsDeltaPct:
        battingDelta === null ? null : Math.abs(battingDelta),
      fieldingTeamBeforePct: fieldingBefore,
      fieldingTeamAfterPct: fieldingAfter,
      fieldingTeamDeltaPct: fieldingDelta,
      pricingSourceBefore: row.pricingSourceBefore,
      pricingSourceAfter: row.pricingSourceAfter,
      timestamp: row.timestamp,
    });

    if (row.isWicket) {
      lastWicketIndex = index;
    }
    if (isBoundary) {
      lastBoundaryIndex = index;
    }
    previousEvent = row.event;
  }

  return reactionRows;
}

function classifySignal(input: {
  isWicket: boolean;
  isSix: boolean;
  isFour: boolean;
  isBoundary: boolean;
  isDotBall: boolean;
  isExtra: boolean;
  runsTotal: number;
}): string {
  if (input.isWicket) {
    return "wicket";
  }
  if (input.isSix) {
    return "six";
  }
  if (input.isFour) {
    return "four";
  }
  if (input.isBoundary) {
    return "boundary";
  }
  if (input.isDotBall) {
    return "dot";
  }
  if (input.isExtra) {
    return "extra";
  }
  if (input.runsTotal === 1) {
    return "single";
  }
  if (input.runsTotal === 2) {
    return "double";
  }
  if (input.runsTotal === 3) {
    return "triple";
  }
  return "other";
}

function toCsv(rows: readonly ReactionRow[]): string {
  const header = [
    "inning",
    "ball",
    "batting_team",
    "bowling_team",
    "event",
    "signal",
    "previous_event",
    "event_pair",
    "runs_total",
    "batsman_runs",
    "wides",
    "noballs",
    "byes",
    "legbyes",
    "is_wicket",
    "wicket_kind",
    "is_boundary",
    "is_four",
    "is_six",
    "is_dot_ball",
    "is_extra",
    "boundary_streak_before",
    "boundary_streak_after",
    "six_streak_before",
    "six_streak_after",
    "dot_streak_before",
    "dot_streak_after",
    "balls_since_previous_wicket",
    "balls_since_previous_boundary",
    "batting_team_before_pct",
    "batting_team_after_pct",
    "batting_team_delta_pct",
    "batting_team_abs_delta_pct",
    "fielding_team_before_pct",
    "fielding_team_after_pct",
    "fielding_team_delta_pct",
    "pricing_source_before",
    "pricing_source_after",
    "timestamp",
  ];

  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.inning,
        row.ball,
        row.battingTeam,
        row.bowlingTeam,
        row.event,
        row.signal,
        row.previousEvent,
        row.eventPair,
        row.runsTotal,
        row.batsmanRuns,
        row.wides,
        row.noballs,
        row.byes,
        row.legbyes,
        row.isWicket,
        row.wicketKind ?? "",
        row.isBoundary,
        row.isFour,
        row.isSix,
        row.isDotBall,
        row.isExtra,
        row.boundaryStreakBefore,
        row.boundaryStreakAfter,
        row.sixStreakBefore,
        row.sixStreakAfter,
        row.dotStreakBefore,
        row.dotStreakAfter,
        row.ballsSincePreviousWicket ?? "",
        row.ballsSincePreviousBoundary ?? "",
        row.battingTeamBeforePct ?? "",
        row.battingTeamAfterPct ?? "",
        row.battingTeamDeltaPct ?? "",
        row.battingTeamAbsDeltaPct ?? "",
        row.fieldingTeamBeforePct ?? "",
        row.fieldingTeamAfterPct ?? "",
        row.fieldingTeamDeltaPct ?? "",
        row.pricingSourceBefore ?? "",
        row.pricingSourceAfter ?? "",
        row.timestamp,
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  if (!/[",\n]/u.test(text)) {
    return text;
  }
  return `"${text.replace(/"/gu, '""')}"`;
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
