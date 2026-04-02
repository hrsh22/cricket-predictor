export const noResultCricapiPayload = {
  id: "ipl-2026-002",
  name: "Kolkata Knight Riders vs Rajasthan Royals",
  matchType: "t20",
  status: "No result due to rain",
  venue: "Eden Gardens, Kolkata",
  date: "2026-04-01T14:00:00.000Z",
  teams: ["Kolkata Knight Riders", "Rajasthan Royals"],
  teamInfo: [
    { name: "Kolkata Knight Riders", shortname: "KKR" },
    { name: "Rajasthan Royals", shortname: "RR" },
  ],
  score: [],
  tossWinner: "Rajasthan Royals",
  tossChoice: "batting",
  matchWinner: null,
} as const;

export const dlsCricapiPayload = {
  id: "ipl-2026-003",
  name: "Delhi Capitals vs Sunrisers Hyderabad",
  matchType: "t20",
  status: "Delhi Capitals won by 12 runs (DLS method)",
  venue: "Arun Jaitley Stadium, Delhi",
  date: "2026-04-02T14:00:00.000Z",
  teams: ["Delhi Capitals", "Sunrisers Hyderabad"],
  teamInfo: [
    { name: "Delhi Capitals", shortname: "DC" },
    { name: "Sunrisers Hyderabad", shortname: "SRH" },
  ],
  score: [
    { r: 168, w: 7, o: 20, inning: "Delhi Capitals Inning 1" },
    { r: 123, w: 8, o: 14.2, inning: "Sunrisers Hyderabad Inning 1" },
  ],
  tossWinner: "Sunrisers Hyderabad",
  tossChoice: "bowling",
  matchWinner: "Delhi Capitals",
} as const;

export const superOverCricapiPayload = {
  id: "ipl-2026-004",
  name: "Punjab Kings vs Gujarat Titans",
  matchType: "t20",
  status: "Gujarat Titans won in Super Over",
  venue: "PCA Stadium, Mohali",
  date: "2026-04-03T14:00:00.000Z",
  teams: ["Punjab Kings", "Gujarat Titans"],
  teamInfo: [
    { name: "Punjab Kings", shortname: "PBKS" },
    { name: "Gujarat Titans", shortname: "GT" },
  ],
  score: [
    { r: 182, w: 6, o: 20, inning: "Punjab Kings Inning 1" },
    { r: 182, w: 8, o: 20, inning: "Gujarat Titans Inning 1" },
  ],
  tossWinner: "Punjab Kings",
  tossChoice: "batting",
  matchWinner: "Gujarat Titans",
} as const;
