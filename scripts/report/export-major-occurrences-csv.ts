import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

interface CliOptions {
  inputPath: string;
  outputPath: string;
  summaryPath: string;
}

interface SeasonBallRow {
  season: number;
  matchDate: string;
  sourceMatchId: string;
  matchSlug: string;
  eventSlug: string;
  commentaryUrl: string;
  deliverySourceMode: string;
  inning: 1 | 2;
  battingTeam: string;
  bowlingTeam: string;
  ball: string;
  event: string;
  commentary: string | null;
  timestamp: string;
  timestampSource: string;
  originalIndex: number;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
  wicketKind: string | null;
}

interface OccurrenceRow {
  season: number;
  matchDate: string;
  sourceMatchId: string;
  matchSlug: string;
  eventSlug: string;
  commentaryUrl: string;
  granularity: "ball";
  occurrenceTypes: string;
  inning: number;
  phase: string;
  ball: string;
  over: number;
  battingTeam: string;
  bowlingTeam: string;
  event: string;
  signal: string;
  previousEvent: string;
  eventPair: string;
  commentary: string;
  eventWindow: string;
  timestamp: string;
  timestampSource: string;
  previous2Ball: string;
  previous2EventCode: string;
  previous2Signal: string;
  previous2Timestamp: string;
  previous2Commentary: string;
  previousBall: string;
  previousEventCode: string;
  previousSignal: string;
  previousTimestamp: string;
  previousCommentary: string;
  nextBall: string;
  nextEventCode: string;
  nextSignal: string;
  nextTimestamp: string;
  nextCommentary: string;
  next2Ball: string;
  next2EventCode: string;
  next2Signal: string;
  next2Timestamp: string;
  next2Commentary: string;
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  wicketKind: string;
  cumulativeRunsBefore: number;
  cumulativeRunsAfter: number;
  wicketsBefore: number;
  wicketsAfter: number;
  legalBallsBowledBefore: number;
  legalBallsBowledAfter: number;
  ballsRemainingBefore: number;
  ballsRemainingAfter: number;
  target: string;
  runsRequiredBefore: string;
  runsRequiredAfter: string;
  requiredRunRateBefore: string;
  requiredRunRateAfter: string;
  currentRunRateBefore: string;
  currentRunRateAfter: string;
  boundaryStreakBefore: number;
  boundaryStreakAfter: number;
  sixStreakBefore: number;
  sixStreakAfter: number;
  dotStreakBefore: number;
  dotStreakAfter: number;
  ballsSincePreviousWicket: string;
  ballsSincePreviousBoundary: string;
  overRunsCompleted: string;
  overWicketsCompleted: string;
  overBoundariesCompleted: string;
  overSixesCompleted: string;
  overDotsCompleted: string;
}

interface SummaryExample {
  count: number;
  matchSlug: string;
  inning: number;
  ball: string;
  event: string;
  commentary: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const seasonRows = await loadSeasonRows(options.inputPath);
  const majorOccurrences = extractMajorOccurrences(seasonRows);
  const summary = buildSummary(seasonRows, majorOccurrences);

  await mkdir(dirname(options.outputPath), { recursive: true });
  await mkdir(dirname(options.summaryPath), { recursive: true });
  await writeFile(options.outputPath, toCsv(majorOccurrences), "utf8");
  await writeFile(options.summaryPath, summary, "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        summaryPath: options.summaryPath,
        scannedRows: seasonRows.length,
        matchCount: new Set(seasonRows.map((row) => row.sourceMatchId)).size,
        majorOccurrenceRows: majorOccurrences.length,
      },
      null,
      2,
    )}\n`,
  );
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let inputPath = "data/polymarket-ball-odds-ipl-2025.csv";
  let outputPath = "data/ipl-2025-major-occurrences.csv";
  let summaryPath = "data/ipl-2025-major-occurrences-summary.md";

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--input") {
      inputPath = argv[index + 1] ?? inputPath;
      index += 1;
      continue;
    }
    if (argument === "--output") {
      outputPath = argv[index + 1] ?? outputPath;
      index += 1;
      continue;
    }
    if (argument === "--summary") {
      summaryPath = argv[index + 1] ?? summaryPath;
      index += 1;
      continue;
    }

    throw new Error(
      `Unknown argument "${argument}". Expected --input, --output, optional --summary.`,
    );
  }

  return { inputPath, outputPath, summaryPath };
}

async function loadSeasonRows(inputPath: string): Promise<SeasonBallRow[]> {
  const content = await readFile(resolve(process.cwd(), inputPath), "utf8");
  const records = parseCsvRecords(content);
  const [header, ...rows] = records;
  if (header === undefined) {
    throw new Error("CSV file is empty.");
  }

  const columnIndex = new Map<string, number>();
  for (const [index, column] of header.entries()) {
    columnIndex.set(column, index);
  }

  return rows.map((record, index) =>
    parseSeasonRow(record, columnIndex, index),
  );
}

function parseSeasonRow(
  record: readonly string[],
  columnIndex: ReadonlyMap<string, number>,
  index: number,
): SeasonBallRow {
  const row = (name: string): string => {
    const position = columnIndex.get(name);
    if (position === undefined) {
      throw new Error(`Missing required CSV column: ${name}`);
    }
    return record[position] ?? "";
  };

  const event = row("event").trim();
  const parsedEvent = parseEventBreakdown(event);

  return {
    season: parseIntegerField(row("season"), "season", index),
    matchDate: row("match_date"),
    sourceMatchId: row("source_match_id"),
    matchSlug: row("match_slug"),
    eventSlug: row("event_slug"),
    commentaryUrl: row("commentary_url"),
    deliverySourceMode: row("delivery_source_mode"),
    inning: parseInningField(row("inning"), index),
    battingTeam: row("batting_team"),
    bowlingTeam: row("bowling_team"),
    ball: row("ball"),
    event,
    commentary: emptyToNull(row("commentary")),
    timestamp: row("timestamp"),
    timestampSource: row("timestamp_source"),
    originalIndex: index,
    runsTotal: parsedEvent.runsTotal,
    batsmanRuns: parsedEvent.batsmanRuns,
    wides: parsedEvent.wides,
    noballs: parsedEvent.noballs,
    byes: parsedEvent.byes,
    legbyes: parsedEvent.legbyes,
    isWicket: parsedEvent.isWicket,
    wicketKind: null,
  };
}

function parseIntegerField(
  value: string,
  field: string,
  index: number,
): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`Row ${index} field ${field} must be an integer.`);
  }
  return parsed;
}

function parseInningField(value: string, index: number): 1 | 2 {
  const parsed = Number.parseInt(value, 10);
  if (parsed !== 1 && parsed !== 2) {
    throw new Error(`Row ${index} inning must be 1 or 2.`);
  }
  return parsed;
}

function emptyToNull(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function extractMajorOccurrences(
  rows: readonly SeasonBallRow[],
): OccurrenceRow[] {
  const matches = new Map<string, SeasonBallRow[]>();
  for (const row of rows) {
    const existing = matches.get(row.sourceMatchId) ?? [];
    existing.push(row);
    matches.set(row.sourceMatchId, existing);
  }

  const occurrenceRows: OccurrenceRow[] = [];

  for (const matchRows of matches.values()) {
    matchRows.sort(compareSeasonRows);
    const firstInningsTotal = matchRows
      .filter((row) => row.inning === 1)
      .reduce((sum, row) => sum + row.runsTotal, 0);
    const target = firstInningsTotal > 0 ? firstInningsTotal + 1 : null;

    const rowsByInning = new Map<1 | 2, SeasonBallRow[]>();
    for (const row of matchRows) {
      const existing = rowsByInning.get(row.inning) ?? [];
      existing.push(row);
      rowsByInning.set(row.inning, existing);
    }

    for (const inning of [1, 2] as const) {
      const inningRows = rowsByInning.get(inning) ?? [];
      occurrenceRows.push(
        ...extractInningOccurrences(inningRows, target, firstInningsTotal),
      );
    }
  }

  return occurrenceRows;
}

function extractInningOccurrences(
  rows: readonly SeasonBallRow[],
  matchTarget: number | null,
  firstInningsTotal: number,
): OccurrenceRow[] {
  const occurrences: OccurrenceRow[] = [];
  let cumulativeRuns = 0;
  let wickets = 0;
  let legalBalls = 0;
  let boundaryStreak = 0;
  let sixStreak = 0;
  let dotStreak = 0;
  let previousEvent = "";
  let lastWicketIndex: number | null = null;
  let lastBoundaryIndex: number | null = null;

  let currentOver = -1;
  let overRuns = 0;
  let overWickets = 0;
  let overBoundaries = 0;
  let overSixes = 0;
  let overDots = 0;
  let overLegalBalls = 0;

  for (const [index, row] of rows.entries()) {
    const { over } = parseBallToken(row.ball);
    const previous2Row = rows[index - 2];
    const previousRow = rows[index - 1];
    const nextRow = rows[index + 1];
    const next2Row = rows[index + 2];
    const isBoundary = row.batsmanRuns === 4 || row.batsmanRuns === 6;
    const isFour = row.batsmanRuns === 4;
    const isSix = row.batsmanRuns === 6;
    const isDotBall = row.runsTotal === 0 && !row.isWicket;
    const isLegalBall = row.wides === 0 && row.noballs === 0;
    const signal = classifySignal(row);

    if (over !== currentOver) {
      currentOver = over;
      overRuns = 0;
      overWickets = 0;
      overBoundaries = 0;
      overSixes = 0;
      overDots = 0;
      overLegalBalls = 0;
    }

    const cumulativeRunsBefore = cumulativeRuns;
    const wicketsBefore = wickets;
    const legalBallsBefore = legalBalls;
    const ballsRemainingBefore = Math.max(0, 120 - legalBallsBefore);
    const target = row.inning === 2 ? matchTarget : null;
    const runsRequiredBefore =
      target === null ? null : Math.max(target - cumulativeRunsBefore, 0);
    const requiredRunRateBefore =
      target === null || ballsRemainingBefore === 0
        ? null
        : (runsRequiredBefore ?? 0) / (ballsRemainingBefore / 6);
    const currentRunRateBefore =
      legalBallsBefore === 0
        ? null
        : cumulativeRunsBefore / (legalBallsBefore / 6);

    const boundaryStreakBefore = boundaryStreak;
    const sixStreakBefore = sixStreak;
    const dotStreakBefore = dotStreak;
    const ballsSincePreviousWicket =
      lastWicketIndex === null ? null : index - lastWicketIndex;
    const ballsSincePreviousBoundary =
      lastBoundaryIndex === null ? null : index - lastBoundaryIndex;

    boundaryStreak = isBoundary ? boundaryStreak + 1 : 0;
    sixStreak = isSix ? sixStreak + 1 : 0;
    dotStreak = isDotBall ? dotStreak + 1 : 0;

    cumulativeRuns += row.runsTotal;
    wickets += row.isWicket ? 1 : 0;
    legalBalls += isLegalBall ? 1 : 0;

    overRuns += row.runsTotal;
    overWickets += row.isWicket ? 1 : 0;
    overBoundaries += isBoundary ? 1 : 0;
    overSixes += isSix ? 1 : 0;
    overDots += isDotBall ? 1 : 0;
    overLegalBalls += isLegalBall ? 1 : 0;

    const ballsRemainingAfter = Math.max(0, 120 - legalBalls);
    const runsRequiredAfter =
      target === null ? null : Math.max(target - cumulativeRuns, 0);
    const requiredRunRateAfter =
      target === null || ballsRemainingAfter === 0
        ? null
        : (runsRequiredAfter ?? 0) / (ballsRemainingAfter / 6);
    const currentRunRateAfter =
      legalBalls === 0 ? null : cumulativeRuns / (legalBalls / 6);

    const occurrenceTypes = new Set<string>();
    const phase = determinePhase(legalBallsBefore);
    const isLastOver = ballsRemainingBefore <= 6;
    const isFinalTwoOvers = ballsRemainingBefore <= 12;
    const isFinalFiveOvers = ballsRemainingBefore <= 30;
    const isCloseFinish =
      row.inning === 2 &&
      runsRequiredBefore !== null &&
      runsRequiredBefore <= 24;
    const isHighPressureChase =
      row.inning === 2 &&
      requiredRunRateBefore !== null &&
      requiredRunRateBefore >= 10;
    const isVeryHighPressureChase =
      row.inning === 2 &&
      requiredRunRateBefore !== null &&
      requiredRunRateBefore >= 12;

    if (row.isWicket) {
      occurrenceTypes.add("wicket");
    }
    if (isSix) {
      occurrenceTypes.add("six");
    }
    if (isFour) {
      occurrenceTypes.add("four");
    }
    if (boundaryStreak >= 2) {
      occurrenceTypes.add("boundary_streak_2plus");
    }
    if (boundaryStreak >= 3) {
      occurrenceTypes.add("boundary_streak_3plus");
    }
    if (sixStreak >= 2) {
      occurrenceTypes.add("six_streak_2plus");
    }
    if (dotStreak >= 3) {
      occurrenceTypes.add("dot_streak_3plus");
    }
    if (dotStreak >= 4) {
      occurrenceTypes.add("dot_streak_4plus");
    }
    if (
      row.isWicket &&
      ballsSincePreviousWicket !== null &&
      ballsSincePreviousWicket <= 6
    ) {
      occurrenceTypes.add("collapse_wicket_6balls");
    }
    if (
      row.isWicket &&
      ballsSincePreviousWicket !== null &&
      ballsSincePreviousWicket <= 2
    ) {
      occurrenceTypes.add("back_to_back_wicket_window");
    }
    if (isBoundary && dotStreakBefore >= 3) {
      occurrenceTypes.add("boundary_after_dot_pressure");
    }
    if (isSix && dotStreakBefore >= 2) {
      occurrenceTypes.add("six_after_dot_pressure");
    }
    if (row.isWicket && dotStreakBefore >= 2) {
      occurrenceTypes.add("wicket_after_dot_pressure");
    }
    if (
      isBoundary &&
      ballsSincePreviousWicket !== null &&
      ballsSincePreviousWicket <= 3
    ) {
      occurrenceTypes.add("boundary_after_recent_wicket");
    }
    if (row.isWicket && boundaryStreakBefore >= 2) {
      occurrenceTypes.add("wicket_after_boundary_burst");
    }
    if (row.isWicket && phase === "powerplay") {
      occurrenceTypes.add("powerplay_wicket");
    }
    if (isSix && phase === "powerplay") {
      occurrenceTypes.add("powerplay_six");
    }
    if (row.isWicket && phase === "death") {
      occurrenceTypes.add("death_over_wicket");
    }
    if (isBoundary && phase === "death") {
      occurrenceTypes.add("death_over_boundary");
    }
    if (isSix && phase === "death") {
      occurrenceTypes.add("death_over_six");
    }
    if (isDotBall && phase === "death") {
      occurrenceTypes.add("death_over_dot");
    }

    if (row.inning === 2) {
      if (isBoundary && isHighPressureChase) {
        occurrenceTypes.add("chase_boundary_high_rrr");
      }
      if (isSix && isVeryHighPressureChase) {
        occurrenceTypes.add("chase_six_very_high_rrr");
      }
      if (row.isWicket && isFinalFiveOvers) {
        occurrenceTypes.add("chase_wicket_final5");
      }
      if (isBoundary && isFinalFiveOvers) {
        occurrenceTypes.add("chase_boundary_final5");
      }
      if (isDotBall && isFinalFiveOvers) {
        occurrenceTypes.add("chase_dot_final5");
      }
      if (row.isWicket && isFinalTwoOvers) {
        occurrenceTypes.add("last_two_overs_wicket");
      }
      if (isBoundary && isFinalTwoOvers) {
        occurrenceTypes.add("last_two_overs_boundary");
      }
      if (isLastOver && isBoundary) {
        occurrenceTypes.add("last_over_boundary");
      }
      if (isLastOver && isSix) {
        occurrenceTypes.add("last_over_six");
      }
      if (isLastOver && isDotBall) {
        occurrenceTypes.add("last_over_dot");
      }
      if (isLastOver && row.isWicket) {
        occurrenceTypes.add("last_over_wicket");
      }
      if (isCloseFinish && isBoundary) {
        occurrenceTypes.add("close_finish_boundary");
      }
      if (isCloseFinish && row.isWicket) {
        occurrenceTypes.add("close_finish_wicket");
      }
      if (isCloseFinish && isDotBall) {
        occurrenceTypes.add("close_finish_dot");
      }
      if (
        target !== null &&
        cumulativeRunsBefore < target &&
        cumulativeRuns >= target
      ) {
        occurrenceTypes.add("winning_shot");
        if (isFour) {
          occurrenceTypes.add("winning_four");
        }
        if (isSix) {
          occurrenceTypes.add("winning_six");
        }
      }
    }

    const isOverComplete =
      nextRow === undefined || parseBallToken(nextRow.ball).over !== over;
    let overRunsCompleted: number | null = null;
    let overWicketsCompleted: number | null = null;
    let overBoundariesCompleted: number | null = null;
    let overSixesCompleted: number | null = null;
    let overDotsCompleted: number | null = null;

    if (isOverComplete) {
      overRunsCompleted = overRuns;
      overWicketsCompleted = overWickets;
      overBoundariesCompleted = overBoundaries;
      overSixesCompleted = overSixes;
      overDotsCompleted = overDots;

      if (overLegalBalls === 6 && overRuns === 0) {
        occurrenceTypes.add("maiden_over");
      }
      if (overLegalBalls === 6 && overRuns === 0 && overWickets >= 1) {
        occurrenceTypes.add("wicket_maiden");
      }
      if (overWickets >= 2) {
        occurrenceTypes.add("double_wicket_over");
      }
      if (overBoundaries >= 3) {
        occurrenceTypes.add("boundary_burst_over");
      }
      if (overSixes >= 2) {
        occurrenceTypes.add("six_burst_over");
      }
      if (overRuns >= 12) {
        occurrenceTypes.add("big_over_12plus");
      }
      if (overRuns >= 15) {
        occurrenceTypes.add("big_over_15plus");
      }
      if (phase === "death" && overRuns >= 15) {
        occurrenceTypes.add("death_big_over_15plus");
      }
    }

    if (occurrenceTypes.size > 0) {
      occurrences.push({
        season: row.season,
        matchDate: row.matchDate,
        sourceMatchId: row.sourceMatchId,
        matchSlug: row.matchSlug,
        eventSlug: row.eventSlug,
        commentaryUrl: row.commentaryUrl,
        granularity: "ball",
        occurrenceTypes: [...occurrenceTypes].sort().join(";"),
        inning: row.inning,
        phase,
        ball: row.ball,
        over,
        battingTeam: row.battingTeam,
        bowlingTeam: row.bowlingTeam,
        event: row.event,
        signal,
        previousEvent,
        eventPair:
          previousEvent.length === 0
            ? row.event
            : `${previousEvent}>${row.event}`,
        commentary: row.commentary ?? "",
        eventWindow: buildEventWindow({
          previous2Ball: previous2Row?.ball ?? null,
          previous2EventCode: previous2Row?.event ?? null,
          previousBall: previousRow?.ball ?? null,
          previousEventCode: previousRow?.event ?? null,
          eventBall: row.ball,
          eventCode: row.event,
          nextBall: nextRow?.ball ?? null,
          nextEventCode: nextRow?.event ?? null,
          next2Ball: next2Row?.ball ?? null,
          next2EventCode: next2Row?.event ?? null,
        }),
        timestamp: row.timestamp,
        timestampSource: row.timestampSource,
        previous2Ball: previous2Row?.ball ?? "",
        previous2EventCode: previous2Row?.event ?? "",
        previous2Signal:
          previous2Row === undefined ? "" : classifySignal(previous2Row),
        previous2Timestamp: previous2Row?.timestamp ?? "",
        previous2Commentary: previous2Row?.commentary ?? "",
        previousBall: previousRow?.ball ?? "",
        previousEventCode: previousRow?.event ?? "",
        previousSignal:
          previousRow === undefined ? "" : classifySignal(previousRow),
        previousTimestamp: previousRow?.timestamp ?? "",
        previousCommentary: previousRow?.commentary ?? "",
        nextBall: nextRow?.ball ?? "",
        nextEventCode: nextRow?.event ?? "",
        nextSignal: nextRow === undefined ? "" : classifySignal(nextRow),
        nextTimestamp: nextRow?.timestamp ?? "",
        nextCommentary: nextRow?.commentary ?? "",
        next2Ball: next2Row?.ball ?? "",
        next2EventCode: next2Row?.event ?? "",
        next2Signal: next2Row === undefined ? "" : classifySignal(next2Row),
        next2Timestamp: next2Row?.timestamp ?? "",
        next2Commentary: next2Row?.commentary ?? "",
        runsTotal: row.runsTotal,
        batsmanRuns: row.batsmanRuns,
        wides: row.wides,
        noballs: row.noballs,
        byes: row.byes,
        legbyes: row.legbyes,
        wicketKind: row.wicketKind ?? "",
        cumulativeRunsBefore,
        cumulativeRunsAfter: cumulativeRuns,
        wicketsBefore,
        wicketsAfter: wickets,
        legalBallsBowledBefore: legalBallsBefore,
        legalBallsBowledAfter: legalBalls,
        ballsRemainingBefore,
        ballsRemainingAfter,
        target: nullableNumber(target),
        runsRequiredBefore: nullableNumber(runsRequiredBefore),
        runsRequiredAfter: nullableNumber(runsRequiredAfter),
        requiredRunRateBefore: nullableRate(requiredRunRateBefore),
        requiredRunRateAfter: nullableRate(requiredRunRateAfter),
        currentRunRateBefore: nullableRate(currentRunRateBefore),
        currentRunRateAfter: nullableRate(currentRunRateAfter),
        boundaryStreakBefore,
        boundaryStreakAfter: boundaryStreak,
        sixStreakBefore,
        sixStreakAfter: sixStreak,
        dotStreakBefore,
        dotStreakAfter: dotStreak,
        ballsSincePreviousWicket: nullableNumber(ballsSincePreviousWicket),
        ballsSincePreviousBoundary: nullableNumber(ballsSincePreviousBoundary),
        overRunsCompleted: nullableNumber(overRunsCompleted),
        overWicketsCompleted: nullableNumber(overWicketsCompleted),
        overBoundariesCompleted: nullableNumber(overBoundariesCompleted),
        overSixesCompleted: nullableNumber(overSixesCompleted),
        overDotsCompleted: nullableNumber(overDotsCompleted),
      });
    }

    if (row.isWicket) {
      lastWicketIndex = index;
    }
    if (isBoundary) {
      lastBoundaryIndex = index;
    }
    previousEvent = row.event;

    if (isOverComplete) {
      currentOver = -1;
    }
  }

  if (
    rows.length > 0 &&
    rows[0]?.inning === 2 &&
    matchTarget !== null &&
    firstInningsTotal > 0
  ) {
    const lastRow = rows.at(-1);
    if (lastRow !== undefined && cumulativeRuns < matchTarget) {
      occurrences.push({
        season: lastRow.season,
        matchDate: lastRow.matchDate,
        sourceMatchId: lastRow.sourceMatchId,
        matchSlug: lastRow.matchSlug,
        eventSlug: lastRow.eventSlug,
        commentaryUrl: lastRow.commentaryUrl,
        granularity: "ball",
        occurrenceTypes: "failed_chase_end_state",
        inning: lastRow.inning,
        phase: determinePhase(Math.max(0, legalBalls - 1)),
        ball: lastRow.ball,
        over: parseBallToken(lastRow.ball).over,
        battingTeam: lastRow.battingTeam,
        bowlingTeam: lastRow.bowlingTeam,
        event: lastRow.event,
        signal: classifySignal(lastRow),
        previousEvent: "",
        eventPair: lastRow.event,
        commentary: lastRow.commentary ?? "",
        eventWindow: buildEventWindow({
          previous2Ball: rows.at(-3)?.ball ?? null,
          previous2EventCode: rows.at(-3)?.event ?? null,
          previousBall: rows.at(-2)?.ball ?? null,
          previousEventCode: rows.at(-2)?.event ?? null,
          eventBall: lastRow.ball,
          eventCode: lastRow.event,
          nextBall: null,
          nextEventCode: null,
          next2Ball: null,
          next2EventCode: null,
        }),
        timestamp: lastRow.timestamp,
        timestampSource: lastRow.timestampSource,
        previous2Ball: rows.at(-3)?.ball ?? "",
        previous2EventCode: rows.at(-3)?.event ?? "",
        previous2Signal:
          rows.at(-3) === undefined
            ? ""
            : classifySignal(rows.at(-3) as SeasonBallRow),
        previous2Timestamp: rows.at(-3)?.timestamp ?? "",
        previous2Commentary: rows.at(-3)?.commentary ?? "",
        previousBall: rows.at(-2)?.ball ?? "",
        previousEventCode: rows.at(-2)?.event ?? "",
        previousSignal:
          rows.at(-2) === undefined
            ? ""
            : classifySignal(rows.at(-2) as SeasonBallRow),
        previousTimestamp: rows.at(-2)?.timestamp ?? "",
        previousCommentary: rows.at(-2)?.commentary ?? "",
        nextBall: "",
        nextEventCode: "",
        nextSignal: "",
        nextTimestamp: "",
        nextCommentary: "",
        next2Ball: "",
        next2EventCode: "",
        next2Signal: "",
        next2Timestamp: "",
        next2Commentary: "",
        runsTotal: lastRow.runsTotal,
        batsmanRuns: lastRow.batsmanRuns,
        wides: lastRow.wides,
        noballs: lastRow.noballs,
        byes: lastRow.byes,
        legbyes: lastRow.legbyes,
        wicketKind: lastRow.wicketKind ?? "",
        cumulativeRunsBefore: Math.max(0, cumulativeRuns - lastRow.runsTotal),
        cumulativeRunsAfter: cumulativeRuns,
        wicketsBefore: Math.max(0, wickets - (lastRow.isWicket ? 1 : 0)),
        wicketsAfter: wickets,
        legalBallsBowledBefore: Math.max(
          0,
          legalBalls - (lastRow.wides === 0 && lastRow.noballs === 0 ? 1 : 0),
        ),
        legalBallsBowledAfter: legalBalls,
        ballsRemainingBefore: Math.min(120, 120 - Math.max(0, legalBalls - 1)),
        ballsRemainingAfter: Math.max(0, 120 - legalBalls),
        target: String(matchTarget),
        runsRequiredBefore: String(
          Math.max(matchTarget - (cumulativeRuns - lastRow.runsTotal), 0),
        ),
        runsRequiredAfter: String(Math.max(matchTarget - cumulativeRuns, 0)),
        requiredRunRateBefore: "",
        requiredRunRateAfter: "",
        currentRunRateBefore: "",
        currentRunRateAfter: "",
        boundaryStreakBefore: 0,
        boundaryStreakAfter: 0,
        sixStreakBefore: 0,
        sixStreakAfter: 0,
        dotStreakBefore: 0,
        dotStreakAfter: 0,
        ballsSincePreviousWicket: "",
        ballsSincePreviousBoundary: "",
        overRunsCompleted: "",
        overWicketsCompleted: "",
        overBoundariesCompleted: "",
        overSixesCompleted: "",
        overDotsCompleted: "",
      });
    }
  }

  return occurrences;
}

function compareSeasonRows(left: SeasonBallRow, right: SeasonBallRow): number {
  if (left.inning !== right.inning) {
    return left.inning - right.inning;
  }

  const leftBall = parseBallToken(left.ball);
  const rightBall = parseBallToken(right.ball);
  if (leftBall.over !== rightBall.over) {
    return leftBall.over - rightBall.over;
  }
  if (leftBall.ballInOver !== rightBall.ballInOver) {
    return leftBall.ballInOver - rightBall.ballInOver;
  }

  return left.originalIndex - right.originalIndex;
}

function parseBallToken(token: string): { over: number; ballInOver: number } {
  const [overText, ballText] = token.split(".");
  const over = Number.parseInt(overText ?? "", 10);
  const ballInOver = Number.parseInt(ballText ?? "0", 10);
  if (!Number.isInteger(over) || !Number.isInteger(ballInOver)) {
    throw new Error(`Invalid ball token: ${token}`);
  }
  return { over, ballInOver };
}

function determinePhase(
  legalBallsBefore: number,
): "powerplay" | "middle" | "death" {
  if (legalBallsBefore < 36) {
    return "powerplay";
  }
  if (legalBallsBefore < 96) {
    return "middle";
  }
  return "death";
}

function classifySignal(row: SeasonBallRow): string {
  if (row.isWicket) {
    return "wicket";
  }
  if (row.batsmanRuns === 6) {
    return "six";
  }
  if (row.batsmanRuns === 4) {
    return "four";
  }
  if (row.runsTotal === 0) {
    return "dot";
  }
  if (row.wides > 0 || row.noballs > 0 || row.byes > 0 || row.legbyes > 0) {
    return "extra";
  }
  if (row.runsTotal === 1) {
    return "single";
  }
  if (row.runsTotal === 2) {
    return "double";
  }
  if (row.runsTotal === 3) {
    return "triple";
  }
  return "other";
}

function parseEventBreakdown(event: string): {
  runsTotal: number;
  batsmanRuns: number;
  wides: number;
  noballs: number;
  byes: number;
  legbyes: number;
  isWicket: boolean;
} {
  const normalized = event.trim().toLowerCase();
  const tokens = normalized.length === 0 ? [] : normalized.split("+");

  let runsTotal = 0;
  let batsmanRuns = 0;
  let wides = 0;
  let noballs = 0;
  let byes = 0;
  let legbyes = 0;
  let isWicket = false;

  for (const token of tokens) {
    if (token === "w") {
      isWicket = true;
      continue;
    }
    if (token === "wd") {
      wides += 1;
      runsTotal += 1;
      continue;
    }
    if (token.endsWith("wd")) {
      const count = parseLeadingNumber(token, "wd");
      wides += count;
      runsTotal += count;
      continue;
    }
    if (token === "nb") {
      noballs += 1;
      runsTotal += 1;
      continue;
    }
    if (token.startsWith("nb") && token.length > 2) {
      const count = parseTrailingNumber(token, "nb");
      noballs += 1;
      batsmanRuns += count;
      runsTotal += 1 + count;
      continue;
    }
    if (token === "b") {
      byes += 1;
      runsTotal += 1;
      continue;
    }
    if (token.endsWith("b") && !token.endsWith("lb")) {
      const count = parseLeadingNumber(token, "b");
      byes += count;
      runsTotal += count;
      continue;
    }
    if (token === "lb") {
      legbyes += 1;
      runsTotal += 1;
      continue;
    }
    if (token.endsWith("lb")) {
      const count = parseLeadingNumber(token, "lb");
      legbyes += count;
      runsTotal += count;
      continue;
    }
    if (/^\d+$/u.test(token)) {
      const count = Number.parseInt(token, 10);
      batsmanRuns += count;
      runsTotal += count;
      continue;
    }

    throw new Error(`Unsupported event token: ${event}`);
  }

  return {
    runsTotal,
    batsmanRuns,
    wides,
    noballs,
    byes,
    legbyes,
    isWicket,
  };
}

function parseLeadingNumber(token: string, suffix: string): number {
  const value = Number.parseInt(token.slice(0, -suffix.length), 10);
  if (!Number.isInteger(value)) {
    throw new Error(`Invalid event token: ${token}`);
  }
  return value;
}

function parseTrailingNumber(token: string, prefix: string): number {
  const value = Number.parseInt(token.slice(prefix.length), 10);
  if (!Number.isInteger(value)) {
    throw new Error(`Invalid event token: ${token}`);
  }
  return value;
}

function nullableNumber(value: number | null): string {
  return value === null ? "" : String(value);
}

function nullableRate(value: number | null): string {
  return value === null ? "" : value.toFixed(2);
}

function buildSummary(
  seasonRows: readonly SeasonBallRow[],
  occurrences: readonly OccurrenceRow[],
): string {
  const seasons = [...new Set(seasonRows.map((row) => row.season))].sort(
    (left, right) => left - right,
  );
  const summaryTitle =
    seasons.length === 1
      ? `# IPL ${seasons[0]} major occurrences summary`
      : `# IPL ${seasons.join(", ")} major occurrences summary`;

  const counts = new Map<string, number>();
  const examples = new Map<string, SummaryExample>();

  for (const row of occurrences) {
    for (const occurrenceType of row.occurrenceTypes.split(";")) {
      const current = counts.get(occurrenceType) ?? 0;
      counts.set(occurrenceType, current + 1);
      if (!examples.has(occurrenceType)) {
        examples.set(occurrenceType, {
          count: 1,
          matchSlug: row.matchSlug,
          inning: row.inning,
          ball: row.ball,
          event: row.event,
          commentary: row.commentary,
        });
      }
    }
  }

  const sortedCounts = [...counts.entries()].sort((left, right) => {
    if (right[1] !== left[1]) {
      return right[1] - left[1];
    }
    return left[0].localeCompare(right[0]);
  });

  const lines = [
    summaryTitle,
    "",
    `- Source rows scanned: ${seasonRows.length}`,
    `- Matches scanned: ${new Set(seasonRows.map((row) => row.sourceMatchId)).size}`,
    `- Major occurrence rows: ${occurrences.length}`,
    `- Unique occurrence types: ${sortedCounts.length}`,
    "",
    "## Occurrence type counts",
    "",
    "| occurrence_type | count | sample |",
    "| --- | ---: | --- |",
  ];

  for (const [occurrenceType, count] of sortedCounts) {
    const example = examples.get(occurrenceType);
    const sample =
      example === undefined
        ? ""
        : `${example.matchSlug} · ${example.inning}.${example.ball} · ${example.event}${example.commentary.length > 0 ? ` · ${example.commentary}` : ""}`;
    lines.push(`| ${occurrenceType} | ${count} | ${escapeMarkdown(sample)} |`);
  }

  return `${lines.join("\n")}\n`;
}

function escapeMarkdown(value: string): string {
  return value.replace(/\|/gu, "\\|");
}

function buildEventWindow(input: {
  previous2Ball: string | null;
  previous2EventCode: string | null;
  previousBall: string | null;
  previousEventCode: string | null;
  eventBall: string;
  eventCode: string;
  nextBall: string | null;
  nextEventCode: string | null;
  next2Ball: string | null;
  next2EventCode: string | null;
}): string {
  return [
    formatWindowPart("prev2", input.previous2Ball, input.previous2EventCode),
    formatWindowPart("prev1", input.previousBall, input.previousEventCode),
    formatWindowPart("event", input.eventBall, input.eventCode),
    formatWindowPart("next1", input.nextBall, input.nextEventCode),
    formatWindowPart("next2", input.next2Ball, input.next2EventCode),
  ].join(" ; ");
}

function formatWindowPart(
  label: string,
  ball: string | null,
  eventCode: string | null,
): string {
  if (
    ball === null ||
    eventCode === null ||
    ball.length === 0 ||
    eventCode.length === 0
  ) {
    return `${label}=NA`;
  }

  return `${label}=${ball}:${eventCode}`;
}

function toCsv(rows: readonly OccurrenceRow[]): string {
  const header = [
    "season",
    "match_date",
    "source_match_id",
    "match_slug",
    "event_slug",
    "commentary_url",
    "granularity",
    "occurrence_types",
    "inning",
    "phase",
    "ball",
    "over",
    "batting_team",
    "bowling_team",
    "event",
    "signal",
    "previous_event",
    "event_pair",
    "commentary",
    "event_window",
    "timestamp",
    "timestamp_source",
    "previous_2_ball",
    "previous_2_event_code",
    "previous_2_signal",
    "previous_2_timestamp",
    "previous_2_commentary",
    "previous_ball",
    "previous_event_code",
    "previous_signal",
    "previous_timestamp",
    "previous_commentary",
    "next_ball",
    "next_event_code",
    "next_signal",
    "next_timestamp",
    "next_commentary",
    "next_2_ball",
    "next_2_event_code",
    "next_2_signal",
    "next_2_timestamp",
    "next_2_commentary",
    "runs_total",
    "batsman_runs",
    "wides",
    "noballs",
    "byes",
    "legbyes",
    "wicket_kind",
    "cumulative_runs_before",
    "cumulative_runs_after",
    "wickets_before",
    "wickets_after",
    "legal_balls_bowled_before",
    "legal_balls_bowled_after",
    "balls_remaining_before",
    "balls_remaining_after",
    "target",
    "runs_required_before",
    "runs_required_after",
    "required_run_rate_before",
    "required_run_rate_after",
    "current_run_rate_before",
    "current_run_rate_after",
    "boundary_streak_before",
    "boundary_streak_after",
    "six_streak_before",
    "six_streak_after",
    "dot_streak_before",
    "dot_streak_after",
    "balls_since_previous_wicket",
    "balls_since_previous_boundary",
    "over_runs_completed",
    "over_wickets_completed",
    "over_boundaries_completed",
    "over_sixes_completed",
    "over_dots_completed",
  ];

  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.season,
        row.matchDate,
        row.sourceMatchId,
        row.matchSlug,
        row.eventSlug,
        row.commentaryUrl,
        row.granularity,
        row.occurrenceTypes,
        row.inning,
        row.phase,
        row.ball,
        row.over,
        row.battingTeam,
        row.bowlingTeam,
        row.event,
        row.signal,
        row.previousEvent,
        row.eventPair,
        row.commentary,
        row.eventWindow,
        row.timestamp,
        row.timestampSource,
        row.previous2Ball,
        row.previous2EventCode,
        row.previous2Signal,
        row.previous2Timestamp,
        row.previous2Commentary,
        row.previousBall,
        row.previousEventCode,
        row.previousSignal,
        row.previousTimestamp,
        row.previousCommentary,
        row.nextBall,
        row.nextEventCode,
        row.nextSignal,
        row.nextTimestamp,
        row.nextCommentary,
        row.next2Ball,
        row.next2EventCode,
        row.next2Signal,
        row.next2Timestamp,
        row.next2Commentary,
        row.runsTotal,
        row.batsmanRuns,
        row.wides,
        row.noballs,
        row.byes,
        row.legbyes,
        row.wicketKind,
        row.cumulativeRunsBefore,
        row.cumulativeRunsAfter,
        row.wicketsBefore,
        row.wicketsAfter,
        row.legalBallsBowledBefore,
        row.legalBallsBowledAfter,
        row.ballsRemainingBefore,
        row.ballsRemainingAfter,
        row.target,
        row.runsRequiredBefore,
        row.runsRequiredAfter,
        row.requiredRunRateBefore,
        row.requiredRunRateAfter,
        row.currentRunRateBefore,
        row.currentRunRateAfter,
        row.boundaryStreakBefore,
        row.boundaryStreakAfter,
        row.sixStreakBefore,
        row.sixStreakAfter,
        row.dotStreakBefore,
        row.dotStreakAfter,
        row.ballsSincePreviousWicket,
        row.ballsSincePreviousBoundary,
        row.overRunsCompleted,
        row.overWicketsCompleted,
        row.overBoundariesCompleted,
        row.overSixesCompleted,
        row.overDotsCompleted,
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

function parseCsvRecords(text: string): string[][] {
  const records: string[][] = [];
  let currentRecord: string[] = [];
  let currentField = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index] ?? "";
    const nextCharacter = text[index + 1] ?? "";

    if (inQuotes) {
      if (character === '"' && nextCharacter === '"') {
        currentField += '"';
        index += 1;
        continue;
      }
      if (character === '"') {
        inQuotes = false;
        continue;
      }
      currentField += character;
      continue;
    }

    if (character === '"') {
      inQuotes = true;
      continue;
    }
    if (character === ",") {
      currentRecord.push(currentField);
      currentField = "";
      continue;
    }
    if (character === "\n") {
      currentRecord.push(currentField);
      currentField = "";
      if (currentRecord.some((field) => field.length > 0)) {
        records.push(currentRecord);
      }
      currentRecord = [];
      continue;
    }
    if (character === "\r") {
      continue;
    }

    currentField += character;
  }

  if (currentField.length > 0 || currentRecord.length > 0) {
    currentRecord.push(currentField);
    if (currentRecord.some((field) => field.length > 0)) {
      records.push(currentRecord);
    }
  }

  return records;
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
