import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { generateBallOddsTimeline } from "./ball-odds-timeline.js";

interface CliOptions {
  eventSlug: string;
  commentaryUrl: string;
  outputPath: string;
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const result = await generateBallOddsTimeline({
    eventSlug: options.eventSlug,
    commentaryUrl: options.commentaryUrl,
    allowPartial: false,
  });

  const csv = toCsv(result.rows);
  await mkdir(dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, csv, "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        outputPath: options.outputPath,
        eventSlug: options.eventSlug,
        deliverySourceMode: result.deliverySourceMode,
        rowCount: result.rows.length,
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

    throw new Error(
      `Unknown argument "${argument}". Expected --event-slug, --commentary-url, --output.`,
    );
  }

  if (eventSlug === null || commentaryUrl === null || outputPath === null) {
    throw new Error(
      "--event-slug, --commentary-url, and --output are all required.",
    );
  }

  return {
    eventSlug,
    commentaryUrl,
    outputPath,
  };
}

function toCsv(
  rows: Awaited<ReturnType<typeof generateBallOddsTimeline>>["rows"],
): string {
  const header = [
    "inning",
    "ball",
    "event",
    "batting_team",
    "bowling_team",
    "team_1",
    "team_1_before_pct",
    "team_1_after_pct",
    "team_1_delta_pct",
    "team_2",
    "team_2_before_pct",
    "team_2_after_pct",
    "team_2_delta_pct",
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.inning,
        row.ball,
        row.event,
        row.battingTeam,
        row.bowlingTeam,
        row.primaryTeam,
        row.primaryBefore ?? "",
        row.primaryAfter ?? "",
        row.primaryDelta ?? "",
        row.secondaryTeam,
        row.secondaryBefore ?? "",
        row.secondaryAfter ?? "",
        row.secondaryDelta ?? "",
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
