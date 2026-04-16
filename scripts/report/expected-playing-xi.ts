import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface CliOptions {
  season: number;
  teamNames: string[];
  asOf: Date;
}

interface ExpectedXiRow {
  team_name: string;
  player_name: string;
  squad_role: string | null;
  player_role: string | null;
  batting_hand: string | null;
  bowling_type_group: string | null;
  selection_count: number;
  last_selected_at: Date | null;
  source: "final_team_squad" | "historical_selection";
}

async function main(): Promise<void> {
  const options = parseCliArgs(process.argv.slice(2));
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const output: Record<string, unknown> = {};
    for (const teamName of options.teamNames) {
      const rows =
        (await inferFinalTeamSquad(pool, {
          season: options.season,
          teamName,
          asOf: options.asOf,
        })) ??
        (await inferExpectedXi(pool, {
          season: options.season,
          teamName,
          asOf: options.asOf,
        }));
      output[teamName] = rows.map((row, index) => ({
        rank: index + 1,
        playerName: row.player_name,
        squadRole: row.squad_role,
        playerRole: row.player_role,
        battingHand: row.batting_hand,
        bowlingTypeGroup: row.bowling_type_group,
        selectionCount: row.selection_count,
        lastSelectedAt: row.last_selected_at?.toISOString() ?? null,
        source: row.source,
      }));
    }

    process.stdout.write(
      `${JSON.stringify(
        {
          season: options.season,
          asOf: options.asOf.toISOString(),
          teams: output,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function inferFinalTeamSquad(
  pool: ReturnType<typeof createPgPool>,
  input: { season: number; teamName: string; asOf: Date },
): Promise<ExpectedXiRow[] | null> {
  const result = await pool.query<{ payload: Record<string, unknown> }>(
    `
      select rcs.payload
      from raw_cricket_snapshots rcs
      join canonical_matches cm on cm.source_match_id = rcs.source_match_id
      where cm.season = $1
        and cm.scheduled_start > $2
        and (cm.team_a_name = $3 or cm.team_b_name = $3)
      order by cm.scheduled_start asc, rcs.snapshot_time desc
      limit 10
    `,
    [input.season, input.asOf, input.teamName],
  );

  for (const row of result.rows) {
    const espn = isRecord(row.payload["espn"]) ? row.payload["espn"] : null;
    const teams = Array.isArray(espn?.["team"]) ? espn["team"] : [];
    for (const team of teams) {
      if (!isRecord(team)) {
        continue;
      }
      if (readString(team, ["team_name"]) !== input.teamName) {
        continue;
      }

      const squad = Array.isArray(team["squad"]) ? team["squad"] : [];
      const finalSquad = squad.filter(
        (player) =>
          isRecord(player) &&
          readString(player, ["squad_type_name"])?.toLowerCase() ===
            "final team squad",
      ) as Record<string, unknown>[];
      if (finalSquad.length === 0) {
        continue;
      }

      return finalSquad.slice(0, 11).map((player, index) => ({
        team_name: input.teamName,
        player_name:
          readString(player, [
            "known_as",
            "popular_name",
            "card_short",
            "mobile_name",
          ]) ?? `player-${index + 1}`,
        squad_role: readString(player, ["squad_type_name"]),
        player_role: null,
        batting_hand: null,
        bowling_type_group: null,
        selection_count: 1,
        last_selected_at: null,
        source: "final_team_squad",
      }));
    }
  }

  const fixtureRows = await inferFinalTeamSquadFromEspnFixtures(input);
  if (fixtureRows !== null) {
    return fixtureRows;
  }

  return null;
}

async function inferFinalTeamSquadFromEspnFixtures(input: {
  season: number;
  teamName: string;
  asOf: Date;
}): Promise<ExpectedXiRow[] | null> {
  const fixtureHtml = await fetch(
    "https://www.espncricinfo.com/ci/engine/series/1510719.html?view=fixtures",
    {
      headers: {
        accept: "text/html,application/xhtml+xml",
        "user-agent": "Mozilla/5.0",
      },
    },
  ).then((response) => response.text());

  const matchIds = Array.from(
    new Set(
      Array.from(
        fixtureHtml.matchAll(/\/series\/\d+\/(?:game|scorecard)\/(\d+)\//gu),
      )
        .map((match) => match[1])
        .filter((value): value is string => value !== undefined),
    ),
  );

  for (const matchId of matchIds) {
    const payload = (await fetch(
      `https://www.espncricinfo.com/ci/engine/match/${matchId}.json`,
      {
        headers: {
          accept: "application/json,text/plain",
          "user-agent": "Mozilla/5.0",
        },
      },
    ).then((response) => response.json())) as Record<string, unknown>;

    const match = isRecord(payload["match"]) ? payload["match"] : null;
    const teams = Array.isArray(payload["team"]) ? payload["team"] : [];
    const scheduledStart = normalizeEspnDateTime(
      match === null
        ? null
        : readString(match, ["start_datetime_gmt_raw", "start_datetime_gmt"]),
    );
    if (
      scheduledStart === null ||
      Date.parse(scheduledStart) <= input.asOf.getTime()
    ) {
      continue;
    }

    for (const team of teams) {
      if (!isRecord(team)) {
        continue;
      }
      if (readString(team, ["team_name"]) !== input.teamName) {
        continue;
      }

      const squad = Array.isArray(team["squad"]) ? team["squad"] : [];
      const finalSquad = squad.filter(
        (player) =>
          isRecord(player) &&
          readString(player, ["squad_type_name"])?.toLowerCase() ===
            "final team squad",
      ) as Record<string, unknown>[];
      if (finalSquad.length === 0) {
        continue;
      }

      return finalSquad.slice(0, 11).map((player, index) => ({
        team_name: input.teamName,
        player_name:
          readString(player, [
            "known_as",
            "popular_name",
            "card_short",
            "mobile_name",
          ]) ?? `player-${index + 1}`,
        squad_role: readString(player, ["squad_type_name"]),
        player_role: null,
        batting_hand: null,
        bowling_type_group: null,
        selection_count: 1,
        last_selected_at: null,
        source: "final_team_squad",
      }));
    }
  }

  return null;
}

async function inferExpectedXi(
  pool: ReturnType<typeof createPgPool>,
  input: { season: number; teamName: string; asOf: Date },
): Promise<ExpectedXiRow[]> {
  const result = await pool.query<ExpectedXiRow>(
    `
      with first_completed_snapshot as (
        select
          source_match_id,
          min(snapshot_time) as result_known_at
        from raw_cricket_snapshots
        where (
          match_status is not null
          and (
            lower(match_status) in ('completed', 'complete', 'result', 'finished', 'end', 'ended')
            or lower(match_status) like '%won%'
            or lower(match_status) like '%draw%'
            or lower(match_status) like '%tie%'
            or lower(match_status) like '%abandon%'
            or lower(match_status) like '%no result%'
          )
        )
        or (
          payload ->> 'status' is not null
          and (
            lower(payload ->> 'status') in ('completed', 'complete', 'result', 'finished', 'end', 'ended')
            or lower(payload ->> 'status') like '%won%'
            or lower(payload ->> 'status') like '%draw%'
            or lower(payload ->> 'status') like '%tie%'
            or lower(payload ->> 'status') like '%abandon%'
            or lower(payload ->> 'status') like '%no result%'
          )
        )
        group by source_match_id
      ),
      season_selection as (
        select
          mpa.team_name,
          mpa.player_registry_id,
          mpa.source_player_name,
          count(*)::int as selection_count,
          max(coalesce(fcs.result_known_at, cm.scheduled_start + interval '12 hours')) as last_selected_at
        from match_player_appearances mpa
        join canonical_matches cm on cm.id = mpa.canonical_match_id
        left join first_completed_snapshot fcs on fcs.source_match_id = cm.source_match_id
        where cm.competition = 'IPL'
          and cm.season = $1
          and mpa.team_name = $2
          and coalesce(fcs.result_known_at, cm.scheduled_start + interval '12 hours') < $3
        group by mpa.team_name, mpa.player_registry_id, mpa.source_player_name
      )
      select
        tss.team_name,
        tss.source_player_name as player_name,
        tss.squad_role,
        coalesce(psp.player_role, pr.player_role) as player_role,
        psp.batting_hand,
        psp.bowling_type_group,
        coalesce(ss.selection_count, 0) as selection_count,
        ss.last_selected_at,
        'historical_selection' as source
      from team_season_squads tss
      left join season_selection ss
        on (
          ss.player_registry_id is not distinct from tss.player_registry_id
          or (
            ss.player_registry_id is null
            and tss.player_registry_id is null
            and lower(ss.source_player_name) = lower(tss.source_player_name)
          )
        )
      left join player_registry pr on pr.id = tss.player_registry_id
      left join player_style_profiles psp on psp.player_registry_id = tss.player_registry_id
      where tss.season = $1
        and tss.team_name = $2
      order by
        coalesce(ss.selection_count, 0) desc,
        ss.last_selected_at desc nulls last,
        case lower(coalesce(tss.squad_role, ''))
          when 'wk-batter' then 1
          when 'batter' then 2
          when 'all-rounder' then 3
          when 'bowler' then 4
          else 5
        end,
        tss.source_player_name asc
      limit 11
    `,
    [input.season, input.teamName, input.asOf],
  );

  return result.rows;
}

function readString(
  record: Record<string, unknown>,
  keys: readonly string[],
): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseCliArgs(argv: readonly string[]): CliOptions {
  let season = new Date().getUTCFullYear();
  let asOf = new Date();
  const teamNames: string[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--season") {
      season = parseIntegerArg(argument, argv[index + 1]);
      index += 1;
      continue;
    }
    if (argument === "--as-of") {
      const value = argv[index + 1];
      const parsed = value === undefined ? Number.NaN : Date.parse(value);
      if (Number.isNaN(parsed)) {
        throw new Error("--as-of requires an ISO timestamp.");
      }
      asOf = new Date(parsed);
      index += 1;
      continue;
    }
    if (argument === "--team") {
      const value = argv[index + 1]?.trim();
      if (!value) {
        throw new Error("--team requires a non-empty team name.");
      }
      teamNames.push(value);
      index += 1;
      continue;
    }
    throw new Error(
      `Unknown argument "${argument}". Expected --season, --as-of, and one or more --team values.`,
    );
  }

  if (teamNames.length === 0) {
    throw new Error("Provide at least one --team argument.");
  }

  return { season, teamNames, asOf };
}

function parseIntegerArg(flag: string, value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${flag} requires an integer value.`);
  }
  return parsed;
}

function normalizeEspnDateTime(value: string | null): string | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().replace(" ", "T");
  return normalized.endsWith("Z") ? normalized : `${normalized}Z`;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Expected XI inference failed: ${message}`);
  process.exitCode = 1;
});
