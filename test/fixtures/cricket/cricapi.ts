const baseCricapiMatchPayload = {
  id: "ipl-2026-001",
  name: "Chennai Super Kings vs Mumbai Indians",
  matchType: "t20",
  status: "Match not started",
  venue: "MA Chidambaram Stadium, Chennai",
  date: "2026-03-29T14:00:00.000Z",
  teams: ["Chennai Super Kings", "Mumbai Indians"],
  teamInfo: [
    { name: "Chennai Super Kings", shortname: "CSK" },
    { name: "Mumbai Indians", shortname: "MI" },
  ],
  score: [],
  tossWinner: null,
  tossChoice: null,
  matchWinner: null,
} as const;

export const preMatchCricapiPayload = baseCricapiMatchPayload;

export const postTossCricapiPayload = {
  ...baseCricapiMatchPayload,
  status: "Toss won by Mumbai Indians and elected to bowl",
  tossWinner: "Mumbai Indians",
  tossChoice: "bowling",
} as const;

export const inningsBreakCricapiPayload = {
  ...postTossCricapiPayload,
  status: "Innings break",
  score: [{ r: 176, w: 6, o: 20, inning: "Chennai Super Kings Inning 1" }],
} as const;

export const finalResultCricapiPayload = {
  ...inningsBreakCricapiPayload,
  status: "Mumbai Indians won by 4 wickets",
  score: [
    { r: 176, w: 6, o: 20, inning: "Chennai Super Kings Inning 1" },
    { r: 177, w: 6, o: 19.2, inning: "Mumbai Indians Inning 1" },
  ],
  matchWinner: "Mumbai Indians",
} as const;

export const noResultCricapiPayload = {
  ...postTossCricapiPayload,
  id: "ipl-2026-002",
  name: "Kolkata Knight Riders vs Rajasthan Royals",
  venue: "Eden Gardens, Kolkata",
  teams: ["Kolkata Knight Riders", "Rajasthan Royals"],
  teamInfo: [
    { name: "Kolkata Knight Riders", shortname: "KKR" },
    { name: "Rajasthan Royals", shortname: "RR" },
  ],
  status: "No result due to rain",
  tossWinner: "Rajasthan Royals",
  tossChoice: "batting",
  matchWinner: null,
  score: [],
} as const;

export const dlsCricapiPayload = {
  ...finalResultCricapiPayload,
  id: "ipl-2026-003",
  name: "Delhi Capitals vs Sunrisers Hyderabad",
  venue: "Arun Jaitley Stadium, Delhi",
  teams: ["Delhi Capitals", "Sunrisers Hyderabad"],
  teamInfo: [
    { name: "Delhi Capitals", shortname: "DC" },
    { name: "Sunrisers Hyderabad", shortname: "SRH" },
  ],
  status: "Delhi Capitals won by 12 runs (DLS method)",
  tossWinner: "Sunrisers Hyderabad",
  tossChoice: "bowling",
  matchWinner: "Delhi Capitals",
  score: [
    { r: 168, w: 7, o: 20, inning: "Delhi Capitals Inning 1" },
    { r: 123, w: 8, o: 14.2, inning: "Sunrisers Hyderabad Inning 1" },
  ],
} as const;

export const superOverCricapiPayload = {
  ...finalResultCricapiPayload,
  id: "ipl-2026-004",
  name: "Punjab Kings vs Gujarat Titans",
  venue: "PCA Stadium, Mohali",
  teams: ["Punjab Kings", "Gujarat Titans"],
  teamInfo: [
    { name: "Punjab Kings", shortname: "PBKS" },
    { name: "Gujarat Titans", shortname: "GT" },
  ],
  status: "Gujarat Titans won in Super Over",
  tossWinner: "Punjab Kings",
  tossChoice: "batting",
  matchWinner: "Gujarat Titans",
  score: [
    { r: 182, w: 6, o: 20, inning: "Punjab Kings Inning 1" },
    { r: 182, w: 8, o: 20, inning: "Gujarat Titans Inning 1" },
  ],
} as const;

export const incompleteTossCricapiPayload = {
  ...baseCricapiMatchPayload,
  status: "Toss update available",
  tossWinner: "Mumbai Indians",
  tossChoice: null,
} as const;
