import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface SeasonCoverageRow {
  season: number;
  total_matches: string | number;
  completed_wins: string | number;
  ties: string | number;
  no_results: string | number;
  abandoned: string | number;
  distinct_team_count: string | number;
  duplicate_slug_count: string | number;
}

const EXPECTED_MATCH_COUNTS: Record<number, number> = {
  2008: 58,
  2009: 57,
  2010: 60,
  2011: 73,
  2012: 74,
  2013: 76,
  2014: 60,
  2015: 59,
  2016: 60,
  2017: 59,
  2018: 60,
  2019: 60,
  2020: 60,
  2021: 60,
  2022: 74,
  2023: 74,
  2024: 71,
  2025: 74,
};

async function main(): Promise<void> {
  const config = loadAppConfig();
  const pool = createPgPool(config.databaseUrl);

  try {
    const seasons = parseSeasonArgs(process.argv.slice(2));
    const report = await loadCoverageReport(pool, seasons);

    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  } finally {
    await closePgPool(pool);
  }
}

function parseSeasonArgs(argv: readonly string[]): number[] {
  if (argv.length === 0) {
    return [
      2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019,
      2020, 2021, 2022, 2023, 2024, 2025,
    ];
  }

  const parsed = argv.map((value) => Number.parseInt(value, 10));
  for (const season of parsed) {
    if (!Number.isInteger(season)) {
      throw new Error(
        `Season argument must be integer, received: ${String(season)}`,
      );
    }
  }

  return parsed;
}

async function loadCoverageReport(
  pool: ReturnType<typeof createPgPool>,
  seasons: readonly number[],
): Promise<{
  seasons: number[];
  coverage: Array<{
    season: number;
    totalMatches: number;
    completedWins: number;
    ties: number;
    noResults: number;
    abandoned: number;
    distinctTeamCount: number;
    duplicateSlugCount: number;
    expectedMatches: number | null;
    expectedMatchCountDelta: number | null;
    hasExpectedMatchCount: boolean;
    completenessFlag: "complete_win_only" | "partial_or_mixed_results";
  }>;
}> {
  const result = await pool.query<SeasonCoverageRow>(
    `
      with selected as (
        select *
        from canonical_matches
        where competition = 'IPL'
          and season = any($1::int[])
      ),
      distinct_teams as (
        select season, team_a_name as team_name from selected
        union
        select season, team_b_name as team_name from selected
      ),
      team_counts as (
        select season, count(distinct team_name) as distinct_team_count
        from distinct_teams
        group by season
      )
      select
        s.season,
        count(*) as total_matches,
        count(*) filter (
          where status = 'completed'
            and result_type = 'win'
            and winning_team_name is not null
        ) as completed_wins,
        count(*) filter (where result_type = 'tie') as ties,
        count(*) filter (where result_type = 'no_result') as no_results,
        count(*) filter (where result_type = 'abandoned') as abandoned,
        coalesce(tc.distinct_team_count, 0) as distinct_team_count,
        count(*) - count(distinct match_slug) as duplicate_slug_count
      from selected s
      left join team_counts tc on tc.season = s.season
      group by s.season, tc.distinct_team_count
      order by s.season asc
    `,
    [seasons],
  );

  return {
    seasons: [...seasons],
    coverage: result.rows.map((row) => {
      const totalMatches = Number(row.total_matches);
      const completedWins = Number(row.completed_wins);
      const ties = Number(row.ties);
      const noResults = Number(row.no_results);
      const abandoned = Number(row.abandoned);
      const expectedMatches = EXPECTED_MATCH_COUNTS[row.season] ?? null;
      const expectedMatchCountDelta =
        expectedMatches === null ? null : totalMatches - expectedMatches;

      return {
        season: row.season,
        totalMatches,
        completedWins,
        ties,
        noResults,
        abandoned,
        distinctTeamCount: Number(row.distinct_team_count),
        duplicateSlugCount: Number(row.duplicate_slug_count),
        expectedMatches,
        expectedMatchCountDelta,
        hasExpectedMatchCount:
          expectedMatches !== null && totalMatches === expectedMatches,
        completenessFlag:
          totalMatches > 0 && completedWins === totalMatches
            ? "complete_win_only"
            : "partial_or_mixed_results",
      };
    }),
  };
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Season coverage validation failed: ${message}`);
  process.exitCode = 1;
});
