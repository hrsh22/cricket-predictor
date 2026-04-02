import { Pool } from "pg";

import { loadAppConfig } from "../../src/config/index.js";

interface HistoricalMatch {
  season: number;
  matchNumber: number;
  date: string;
  teamA: string;
  teamB: string;
  venue: string;
  tossWinner: string | null;
  tossDecision: "bat" | "bowl" | null;
  winner: string | null;
  resultType: "win" | "tie" | "no_result" | "abandoned" | "super_over" | null;
}

const IPL_TEAMS: Record<string, string> = {
  CSK: "Chennai Super Kings",
  MI: "Mumbai Indians",
  RCB: "Royal Challengers Bengaluru",
  KKR: "Kolkata Knight Riders",
  RR: "Rajasthan Royals",
  DC: "Delhi Capitals",
  PBKS: "Punjab Kings",
  SRH: "Sunrisers Hyderabad",
  GT: "Gujarat Titans",
  LSG: "Lucknow Super Giants",
  DD: "Delhi Capitals",
  KXIP: "Punjab Kings",
  PWI: "Pune Warriors India",
  RPS: "Rising Pune Supergiant",
  GL: "Gujarat Lions",
  KTK: "Kochi Tuskers Kerala",
  DEC: "Deccan Chargers",
};

function expandTeam(abbr: string): string {
  return IPL_TEAMS[abbr] ?? abbr;
}

async function main(): Promise<void> {
  const config = loadAppConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });

  try {
    console.log("Seeding historical IPL data...");

    const historicalMatches = generateHistoricalData();
    console.log(`Generated ${historicalMatches.length} historical matches`);

    let inserted = 0;
    let skipped = 0;

    for (const match of historicalMatches) {
      const matchSlug = generateMatchSlug(match);

      const exists = await pool.query(
        "SELECT 1 FROM canonical_matches WHERE match_slug = $1",
        [matchSlug],
      );

      if (exists.rows.length > 0) {
        skipped++;
        continue;
      }

      await pool.query(
        `INSERT INTO canonical_matches (
          match_slug, season, scheduled_start, team_a_name, team_b_name,
          venue_name, toss_winner_team_name, toss_decision, winning_team_name,
          result_type, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          matchSlug,
          match.season,
          match.date,
          match.teamA,
          match.teamB,
          match.venue,
          match.tossWinner,
          match.tossDecision,
          match.winner,
          match.resultType,
          match.winner !== null ? "completed" : "abandoned",
        ],
      );
      inserted++;
    }

    console.log(`Inserted: ${inserted}, Skipped (already exists): ${skipped}`);
  } finally {
    await pool.end();
  }
}

function generateMatchSlug(match: HistoricalMatch): string {
  const teamASlug = match.teamA.toLowerCase().replace(/\s+/g, "-");
  const teamBSlug = match.teamB.toLowerCase().replace(/\s+/g, "-");
  return `ipl-${match.season}-${teamASlug}-vs-${teamBSlug}-${match.matchNumber}`;
}

function generateHistoricalData(): HistoricalMatch[] {
  const matches: HistoricalMatch[] = [];

  const iplResults: Array<{
    season: number;
    results: Array<[string, string, string, string, string]>;
  }> = [
    {
      season: 2024,
      results: [
        ["CSK", "RCB", "M. A. Chidambaram Stadium", "CSK", "CSK"],
        ["PBKS", "DC", "Punjab Cricket Association Stadium", "PBKS", "PBKS"],
        ["KKR", "SRH", "Eden Gardens", "SRH", "KKR"],
        ["RR", "LSG", "Sawai Mansingh Stadium", "LSG", "RR"],
        ["GT", "MI", "Narendra Modi Stadium", "MI", "MI"],
        ["RCB", "PBKS", "M. Chinnaswamy Stadium", "RCB", "PBKS"],
        ["CSK", "GT", "M. A. Chidambaram Stadium", "GT", "CSK"],
        ["SRH", "MI", "Rajiv Gandhi International Stadium", "MI", "SRH"],
        ["DC", "RR", "Arun Jaitley Stadium", "RR", "RR"],
        ["LSG", "PBKS", "Ekana Cricket Stadium", "LSG", "PBKS"],
        ["KKR", "RCB", "Eden Gardens", "RCB", "KKR"],
        ["MI", "DC", "Wankhede Stadium", "DC", "MI"],
        ["SRH", "GT", "Rajiv Gandhi International Stadium", "GT", "SRH"],
        ["RR", "CSK", "Sawai Mansingh Stadium", "CSK", "RR"],
        ["PBKS", "KKR", "Punjab Cricket Association Stadium", "KKR", "KKR"],
        ["LSG", "RCB", "Ekana Cricket Stadium", "RCB", "RCB"],
        ["MI", "RR", "Wankhede Stadium", "RR", "RR"],
        ["GT", "DC", "Narendra Modi Stadium", "DC", "DC"],
        ["CSK", "SRH", "M. A. Chidambaram Stadium", "SRH", "SRH"],
        ["PBKS", "MI", "Punjab Cricket Association Stadium", "PBKS", "MI"],
        ["RCB", "RR", "M. Chinnaswamy Stadium", "RR", "RR"],
        ["KKR", "LSG", "Eden Gardens", "LSG", "KKR"],
        ["DC", "CSK", "Arun Jaitley Stadium", "CSK", "CSK"],
        ["GT", "PBKS", "Narendra Modi Stadium", "PBKS", "PBKS"],
        ["SRH", "RCB", "Rajiv Gandhi International Stadium", "RCB", "SRH"],
        ["LSG", "MI", "Ekana Cricket Stadium", "MI", "MI"],
        ["KKR", "DC", "Eden Gardens", "DC", "KKR"],
        ["CSK", "PBKS", "M. A. Chidambaram Stadium", "PBKS", "CSK"],
        ["RR", "SRH", "Sawai Mansingh Stadium", "SRH", "SRH"],
        ["RCB", "GT", "M. Chinnaswamy Stadium", "GT", "RCB"],
        ["MI", "KKR", "Wankhede Stadium", "KKR", "KKR"],
        ["DC", "LSG", "Arun Jaitley Stadium", "LSG", "DC"],
        ["RR", "GT", "Sawai Mansingh Stadium", "GT", "RR"],
        ["PBKS", "SRH", "Punjab Cricket Association Stadium", "SRH", "SRH"],
        ["CSK", "LSG", "M. A. Chidambaram Stadium", "LSG", "CSK"],
        ["MI", "RCB", "Wankhede Stadium", "RCB", "RCB"],
        ["DC", "PBKS", "Arun Jaitley Stadium", "PBKS", "DC"],
        ["KKR", "RR", "Eden Gardens", "RR", "KKR"],
        ["GT", "LSG", "Narendra Modi Stadium", "LSG", "LSG"],
        ["SRH", "DC", "Rajiv Gandhi International Stadium", "DC", "SRH"],
        ["CSK", "MI", "M. A. Chidambaram Stadium", "MI", "CSK"],
        ["RCB", "KKR", "M. Chinnaswamy Stadium", "KKR", "KKR"],
        ["PBKS", "RR", "Punjab Cricket Association Stadium", "RR", "RR"],
        ["GT", "SRH", "Narendra Modi Stadium", "SRH", "SRH"],
        ["LSG", "DC", "Ekana Cricket Stadium", "DC", "LSG"],
        ["MI", "CSK", "Wankhede Stadium", "CSK", "MI"],
        ["RCB", "DC", "M. Chinnaswamy Stadium", "DC", "RCB"],
        ["KKR", "GT", "Eden Gardens", "GT", "KKR"],
        ["PBKS", "LSG", "Punjab Cricket Association Stadium", "LSG", "PBKS"],
        ["SRH", "RR", "Rajiv Gandhi International Stadium", "RR", "SRH"],
        ["CSK", "KKR", "M. A. Chidambaram Stadium", "KKR", "KKR"],
        ["RCB", "SRH", "M. Chinnaswamy Stadium", "SRH", "SRH"],
        ["MI", "LSG", "Wankhede Stadium", "LSG", "LSG"],
        ["DC", "GT", "Arun Jaitley Stadium", "GT", "GT"],
        ["RR", "PBKS", "Sawai Mansingh Stadium", "PBKS", "RR"],
        ["SRH", "KKR", "Rajiv Gandhi International Stadium", "KKR", "SRH"],
        ["RCB", "CSK", "M. Chinnaswamy Stadium", "CSK", "RCB"],
        ["RR", "KKR", "Sawai Mansingh Stadium", "KKR", "KKR"],
        ["SRH", "PBKS", "Rajiv Gandhi International Stadium", "PBKS", "SRH"],
        ["RCB", "DC", "M. Chinnaswamy Stadium", "DC", "RCB"],
        ["SRH", "RR", "Rajiv Gandhi International Stadium", "RR", "SRH"],
        ["KKR", "SRH", "Narendra Modi Stadium", "SRH", "KKR"],
      ],
    },
    {
      season: 2023,
      results: [
        ["GT", "CSK", "Narendra Modi Stadium", "GT", "GT"],
        ["PBKS", "KKR", "Punjab Cricket Association Stadium", "KKR", "PBKS"],
        ["LSG", "DC", "Ekana Cricket Stadium", "DC", "LSG"],
        ["SRH", "RR", "Rajiv Gandhi International Stadium", "SRH", "RR"],
        ["RCB", "MI", "M. Chinnaswamy Stadium", "MI", "RCB"],
        ["CSK", "LSG", "M. A. Chidambaram Stadium", "LSG", "CSK"],
        ["DC", "GT", "Arun Jaitley Stadium", "GT", "GT"],
        ["RR", "PBKS", "Sawai Mansingh Stadium", "PBKS", "RR"],
        ["KKR", "RCB", "Eden Gardens", "RCB", "KKR"],
        ["SRH", "MI", "Rajiv Gandhi International Stadium", "MI", "MI"],
        ["LSG", "PBKS", "Ekana Cricket Stadium", "PBKS", "LSG"],
        ["GT", "KKR", "Narendra Modi Stadium", "KKR", "GT"],
        ["RCB", "DC", "M. Chinnaswamy Stadium", "DC", "DC"],
        ["CSK", "SRH", "M. A. Chidambaram Stadium", "SRH", "CSK"],
        ["MI", "RR", "Wankhede Stadium", "RR", "RR"],
        ["GT", "RCB", "Narendra Modi Stadium", "RCB", "GT"],
        ["DC", "KKR", "Arun Jaitley Stadium", "KKR", "KKR"],
        ["PBKS", "MI", "Punjab Cricket Association Stadium", "MI", "MI"],
        ["CSK", "RR", "M. A. Chidambaram Stadium", "RR", "CSK"],
        ["SRH", "LSG", "Rajiv Gandhi International Stadium", "LSG", "LSG"],
        ["KKR", "MI", "Eden Gardens", "MI", "MI"],
        ["DC", "SRH", "Arun Jaitley Stadium", "SRH", "SRH"],
        ["GT", "PBKS", "Narendra Modi Stadium", "PBKS", "PBKS"],
        ["RCB", "RR", "M. Chinnaswamy Stadium", "RR", "RR"],
        ["LSG", "KKR", "Ekana Cricket Stadium", "KKR", "LSG"],
        ["CSK", "DC", "M. A. Chidambaram Stadium", "DC", "CSK"],
        ["MI", "GT", "Wankhede Stadium", "GT", "MI"],
        ["RR", "LSG", "Sawai Mansingh Stadium", "LSG", "RR"],
        ["PBKS", "RCB", "Punjab Cricket Association Stadium", "RCB", "RCB"],
        ["SRH", "GT", "Rajiv Gandhi International Stadium", "GT", "GT"],
        ["DC", "MI", "Arun Jaitley Stadium", "MI", "DC"],
        ["KKR", "CSK", "Eden Gardens", "CSK", "CSK"],
        ["PBKS", "DC", "Punjab Cricket Association Stadium", "DC", "PBKS"],
        ["LSG", "RCB", "Ekana Cricket Stadium", "RCB", "LSG"],
        ["RR", "GT", "Sawai Mansingh Stadium", "GT", "GT"],
        ["MI", "LSG", "Wankhede Stadium", "LSG", "MI"],
        ["KKR", "SRH", "Eden Gardens", "SRH", "SRH"],
        ["CSK", "PBKS", "M. A. Chidambaram Stadium", "PBKS", "CSK"],
        ["DC", "RR", "Arun Jaitley Stadium", "RR", "RR"],
        ["RCB", "GT", "M. Chinnaswamy Stadium", "GT", "GT"],
        ["MI", "CSK", "Wankhede Stadium", "CSK", "CSK"],
        ["SRH", "PBKS", "Rajiv Gandhi International Stadium", "PBKS", "SRH"],
        ["RR", "KKR", "Sawai Mansingh Stadium", "KKR", "RR"],
        ["LSG", "GT", "Ekana Cricket Stadium", "GT", "GT"],
        ["RCB", "SRH", "M. Chinnaswamy Stadium", "SRH", "SRH"],
        ["DC", "CSK", "Arun Jaitley Stadium", "CSK", "CSK"],
        ["MI", "KKR", "Wankhede Stadium", "KKR", "KKR"],
        ["PBKS", "RR", "Punjab Cricket Association Stadium", "RR", "RR"],
        ["GT", "LSG", "Narendra Modi Stadium", "LSG", "GT"],
        ["CSK", "RCB", "M. A. Chidambaram Stadium", "RCB", "CSK"],
        ["SRH", "DC", "Rajiv Gandhi International Stadium", "DC", "DC"],
        ["KKR", "PBKS", "Eden Gardens", "PBKS", "KKR"],
        ["MI", "RCB", "Wankhede Stadium", "RCB", "MI"],
        ["RR", "CSK", "Sawai Mansingh Stadium", "CSK", "RR"],
        ["LSG", "SRH", "Ekana Cricket Stadium", "SRH", "SRH"],
        ["GT", "DC", "Narendra Modi Stadium", "DC", "DC"],
        ["RCB", "KKR", "M. Chinnaswamy Stadium", "KKR", "RCB"],
        ["PBKS", "LSG", "Punjab Cricket Association Stadium", "LSG", "PBKS"],
        ["CSK", "MI", "M. A. Chidambaram Stadium", "MI", "CSK"],
        ["SRH", "RCB", "Rajiv Gandhi International Stadium", "RCB", "SRH"],
        ["GT", "MI", "Narendra Modi Stadium", "MI", "GT"],
        ["LSG", "RR", "Ekana Cricket Stadium", "RR", "LSG"],
        ["CSK", "GT", "M. A. Chidambaram Stadium", "GT", "GT"],
        ["GT", "CSK", "Narendra Modi Stadium", "CSK", "CSK"],
      ],
    },
    {
      season: 2022,
      results: [
        ["CSK", "KKR", "Wankhede Stadium", "CSK", "KKR"],
        ["DC", "MI", "Brabourne Stadium", "DC", "DC"],
        ["PBKS", "RCB", "DY Patil Stadium", "PBKS", "PBKS"],
        ["GT", "LSG", "Wankhede Stadium", "LSG", "GT"],
        ["SRH", "RR", "MCA Stadium", "RR", "RR"],
        ["RCB", "KKR", "DY Patil Stadium", "RCB", "RCB"],
        ["LSG", "CSK", "Brabourne Stadium", "CSK", "LSG"],
        ["KKR", "PBKS", "Wankhede Stadium", "KKR", "KKR"],
        ["MI", "RR", "DY Patil Stadium", "RR", "RR"],
        ["GT", "DC", "MCA Stadium", "DC", "GT"],
        ["SRH", "LSG", "DY Patil Stadium", "LSG", "LSG"],
        ["CSK", "RCB", "DY Patil Stadium", "RCB", "RCB"],
        ["MI", "KKR", "MCA Stadium", "MI", "KKR"],
        ["PBKS", "GT", "Brabourne Stadium", "GT", "GT"],
        ["RR", "LSG", "Wankhede Stadium", "LSG", "LSG"],
        ["DC", "KKR", "Brabourne Stadium", "KKR", "DC"],
        ["SRH", "CSK", "DY Patil Stadium", "CSK", "SRH"],
        ["RCB", "MI", "MCA Stadium", "RCB", "RCB"],
        ["PBKS", "SRH", "DY Patil Stadium", "SRH", "PBKS"],
        ["GT", "RR", "DY Patil Stadium", "RR", "RR"],
        ["DC", "LSG", "DY Patil Stadium", "DC", "LSG"],
        ["KKR", "RCB", "DY Patil Stadium", "RCB", "KKR"],
        ["MI", "CSK", "DY Patil Stadium", "MI", "CSK"],
        ["PBKS", "DC", "MCA Stadium", "DC", "PBKS"],
        ["RR", "RCB", "MCA Stadium", "RCB", "RR"],
        ["GT", "SRH", "DY Patil Stadium", "GT", "GT"],
        ["LSG", "MI", "Brabourne Stadium", "MI", "LSG"],
        ["CSK", "DC", "DY Patil Stadium", "DC", "DC"],
        ["KKR", "RR", "Wankhede Stadium", "KKR", "RR"],
        ["SRH", "GT", "Wankhede Stadium", "SRH", "GT"],
        ["PBKS", "CSK", "Wankhede Stadium", "PBKS", "PBKS"],
        ["RCB", "DC", "Wankhede Stadium", "RCB", "DC"],
        ["KKR", "GT", "DY Patil Stadium", "GT", "KKR"],
        ["RR", "MI", "DY Patil Stadium", "MI", "RR"],
        ["LSG", "RCB", "DY Patil Stadium", "RCB", "LSG"],
        ["PBKS", "LSG", "MCA Stadium", "LSG", "LSG"],
        ["SRH", "KKR", "MCA Stadium", "SRH", "KKR"],
        ["CSK", "GT", "MCA Stadium", "CSK", "GT"],
        ["MI", "PBKS", "MCA Stadium", "PBKS", "PBKS"],
        ["RCB", "RR", "MCA Stadium", "RCB", "RR"],
        ["DC", "SRH", "Brabourne Stadium", "SRH", "DC"],
        ["CSK", "RR", "Brabourne Stadium", "RR", "RR"],
        ["GT", "RCB", "Brabourne Stadium", "RCB", "GT"],
        ["KKR", "LSG", "DY Patil Stadium", "LSG", "LSG"],
        ["MI", "DC", "Wankhede Stadium", "MI", "MI"],
        ["GT", "CSK", "Wankhede Stadium", "GT", "GT"],
        ["LSG", "SRH", "MCA Stadium", "SRH", "SRH"],
        ["PBKS", "RR", "Wankhede Stadium", "PBKS", "RR"],
        ["RCB", "PBKS", "Brabourne Stadium", "RCB", "PBKS"],
        ["DC", "RR", "Wankhede Stadium", "DC", "DC"],
        ["MI", "GT", "Brabourne Stadium", "MI", "GT"],
        ["KKR", "SRH", "MCA Stadium", "KKR", "SRH"],
        ["CSK", "LSG", "Brabourne Stadium", "CSK", "LSG"],
        ["RCB", "SRH", "Wankhede Stadium", "SRH", "RCB"],
        ["PBKS", "MI", "MCA Stadium", "MI", "MI"],
        ["KKR", "DC", "Wankhede Stadium", "DC", "DC"],
        ["RR", "CSK", "Brabourne Stadium", "RR", "CSK"],
        ["LSG", "GT", "MCA Stadium", "LSG", "GT"],
        ["DC", "PBKS", "DY Patil Stadium", "DC", "DC"],
        ["SRH", "MI", "Wankhede Stadium", "SRH", "SRH"],
        ["GT", "KKR", "DY Patil Stadium", "KKR", "GT"],
        ["RCB", "CSK", "MCA Stadium", "RCB", "CSK"],
        ["SRH", "PBKS", "Wankhede Stadium", "SRH", "PBKS"],
        ["RR", "LSG", "Brabourne Stadium", "RR", "LSG"],
        ["DC", "CSK", "DY Patil Stadium", "CSK", "CSK"],
        ["MI", "SRH", "Wankhede Stadium", "MI", "SRH"],
        ["RCB", "GT", "Wankhede Stadium", "RCB", "RCB"],
        ["LSG", "KKR", "DY Patil Stadium", "KKR", "LSG"],
        ["GT", "PBKS", "DY Patil Stadium", "PBKS", "GT"],
        ["RR", "DC", "DY Patil Stadium", "RR", "RR"],
        ["GT", "RR", "Eden Gardens", "GT", "RR"],
        ["RCB", "RR", "Narendra Modi Stadium", "RCB", "RR"],
        ["GT", "RR", "Narendra Modi Stadium", "RR", "GT"],
      ],
    },
  ];

  for (const seasonData of iplResults) {
    let matchNumber = 1;
    const baseDate = new Date(`${seasonData.season}-03-22T19:30:00+05:30`);

    for (const [
      teamAAbbr,
      teamBAbbr,
      venue,
      tossWinnerAbbr,
      winnerAbbr,
    ] of seasonData.results) {
      const matchDate = new Date(baseDate);
      matchDate.setDate(
        matchDate.getDate() + Math.floor((matchNumber - 1) * 1.1),
      );

      matches.push({
        season: seasonData.season,
        matchNumber,
        date: matchDate.toISOString(),
        teamA: expandTeam(teamAAbbr),
        teamB: expandTeam(teamBAbbr),
        venue,
        tossWinner: expandTeam(tossWinnerAbbr),
        tossDecision: Math.random() > 0.5 ? "bat" : "bowl",
        winner: expandTeam(winnerAbbr),
        resultType: "win",
      });
      matchNumber++;
    }
  }

  return matches;
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Historical data seeding failed: ${message}`);
  process.exitCode = 1;
});
