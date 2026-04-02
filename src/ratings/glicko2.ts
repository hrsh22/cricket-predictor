const SCALE = 173.7178;
const TAU = 0.6;
const INITIAL_RATING = 1500;
const INITIAL_RD = 300;
const INITIAL_VOLATILITY = 0.06;
const MIN_RD = 30;
const MAX_RD = 350;
const CONVERGENCE_TOLERANCE = 0.000001;

export interface Glicko2Rating {
  rating: number;
  rd: number;
  volatility: number;
}

export interface MatchOutcome {
  opponentRating: number;
  opponentRD: number;
  score: number;
}

export function createInitialRating(): Glicko2Rating {
  return {
    rating: INITIAL_RATING,
    rd: INITIAL_RD,
    volatility: INITIAL_VOLATILITY,
  };
}

export function computeGlicko2Ratings(
  matches: Array<{
    teamA: string;
    teamB: string;
    winner: string | null;
    scheduledStart: Date;
  }>,
  asOfDate: Date,
): Record<string, Glicko2Rating> {
  const ratings: Record<string, Glicko2Rating> = {};
  const pendingGames: Record<string, MatchOutcome[]> = {};
  let lastProcessedDate: Date | null = null;

  for (const match of matches) {
    if (match.winner === null) continue;
    if (match.scheduledStart >= asOfDate) continue;

    const matchDate = new Date(match.scheduledStart);
    matchDate.setUTCHours(0, 0, 0, 0);

    if (
      lastProcessedDate !== null &&
      matchDate.getTime() !== lastProcessedDate.getTime()
    ) {
      processRatingPeriod(ratings, pendingGames, lastProcessedDate, matchDate);
    }

    if (!ratings[match.teamA]) ratings[match.teamA] = createInitialRating();
    if (!ratings[match.teamB]) ratings[match.teamB] = createInitialRating();

    const teamARating = ratings[match.teamA]!;
    const teamBRating = ratings[match.teamB]!;

    if (!pendingGames[match.teamA]) pendingGames[match.teamA] = [];
    if (!pendingGames[match.teamB]) pendingGames[match.teamB] = [];

    const scoreA = match.winner === match.teamA ? 1 : 0;
    const scoreB = 1 - scoreA;

    pendingGames[match.teamA]!.push({
      opponentRating: teamBRating.rating,
      opponentRD: teamBRating.rd,
      score: scoreA,
    });

    pendingGames[match.teamB]!.push({
      opponentRating: teamARating.rating,
      opponentRD: teamARating.rd,
      score: scoreB,
    });

    lastProcessedDate = matchDate;
  }

  if (lastProcessedDate !== null) {
    processRatingPeriod(ratings, pendingGames, lastProcessedDate, asOfDate);
  }

  return ratings;
}

function processRatingPeriod(
  ratings: Record<string, Glicko2Rating>,
  pendingGames: Record<string, MatchOutcome[]>,
  periodEnd: Date,
  nextPeriodStart: Date,
): void {
  const daysBetween = Math.floor(
    (nextPeriodStart.getTime() - periodEnd.getTime()) / (1000 * 60 * 60 * 24),
  );

  for (const [team, games] of Object.entries(pendingGames)) {
    if (games.length === 0) continue;

    const current = ratings[team];
    if (!current) continue;

    const updated = updateRating(current, games);
    ratings[team] = updated;
  }

  for (const key of Object.keys(pendingGames)) {
    pendingGames[key] = [];
  }

  if (daysBetween > 1) {
    for (const team of Object.keys(ratings)) {
      ratings[team] = applyRDDecay(ratings[team]!, daysBetween);
    }
  }
}

function applyRDDecay(rating: Glicko2Rating, days: number): Glicko2Rating {
  const decayFactor = Math.pow(1.02, Math.min(days, 180));
  const newRD = Math.min(MAX_RD, rating.rd * decayFactor);

  return {
    rating: rating.rating,
    rd: newRD,
    volatility: rating.volatility,
  };
}

function updateRating(
  player: Glicko2Rating,
  games: MatchOutcome[],
): Glicko2Rating {
  if (games.length === 0) {
    return player;
  }

  const mu = toGlicko2Scale(player.rating);
  const phi = player.rd / SCALE;

  let vInverse = 0;
  let deltaSum = 0;

  for (const game of games) {
    const muj = toGlicko2Scale(game.opponentRating);
    const phij = game.opponentRD / SCALE;

    const gPhij = g(phij);
    const E = expectedScore(mu, muj, gPhij);

    vInverse += gPhij * gPhij * E * (1 - E);
    deltaSum += gPhij * (game.score - E);
  }

  const v = 1 / vInverse;
  const delta = v * deltaSum;

  const newVolatility = computeNewVolatility(player.volatility, delta, phi, v);

  const phiStar = Math.sqrt(phi * phi + newVolatility * newVolatility);

  const newPhi = 1 / Math.sqrt(1 / (phiStar * phiStar) + vInverse);

  const newMu = mu + newPhi * newPhi * deltaSum;

  const newRating = fromGlicko2Scale(newMu);
  const newRD = clamp(newPhi * SCALE, MIN_RD, MAX_RD);

  return {
    rating: roundTo(newRating, 6),
    rd: roundTo(newRD, 6),
    volatility: roundTo(newVolatility, 8),
  };
}

function computeNewVolatility(
  sigma: number,
  delta: number,
  phi: number,
  v: number,
): number {
  const a = Math.log(sigma * sigma);
  const deltaSq = delta * delta;
  const phiSq = phi * phi;

  const f = (x: number): number => {
    const expX = Math.exp(x);
    const denom = 2 * Math.pow(phiSq + v + expX, 2);
    const term1 = (expX * (deltaSq - phiSq - v - expX)) / denom;
    const term2 = (x - a) / (TAU * TAU);
    return term1 - term2;
  };

  let A = a;
  let B: number;

  if (deltaSq > phiSq + v) {
    B = Math.log(deltaSq - phiSq - v);
  } else {
    let k = 1;
    while (f(a - k * TAU) < 0) {
      k++;
      if (k > 100) break;
    }
    B = a - k * TAU;
  }

  let fA = f(A);
  let fB = f(B);

  for (let i = 0; i < 100; i++) {
    const C = A + ((A - B) * fA) / (fB - fA);
    const fC = f(C);

    if (Math.abs(fC) < CONVERGENCE_TOLERANCE) {
      return Math.exp(C / 2);
    }

    if (fC * fB < 0) {
      A = B;
      fA = fB;
    } else {
      fA = fA / 2;
    }

    B = C;
    fB = fC;
  }

  return Math.exp(A / 2);
}

function g(phi: number): number {
  return 1 / Math.sqrt(1 + (3 * phi * phi) / (Math.PI * Math.PI));
}

function expectedScore(mu: number, muj: number, gPhi: number): number {
  return 1 / (1 + Math.exp(-gPhi * (mu - muj)));
}

function toGlicko2Scale(rating: number): number {
  return (rating - 1500) / SCALE;
}

function fromGlicko2Scale(mu: number): number {
  return mu * SCALE + 1500;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function roundTo(value: number, decimals: number): number {
  return Number(value.toFixed(decimals));
}

export function glicko2WinProbability(
  teamA: Glicko2Rating,
  teamB: Glicko2Rating,
): number {
  const muA = toGlicko2Scale(teamA.rating);
  const muB = toGlicko2Scale(teamB.rating);
  const phiA = teamA.rd / SCALE;
  const phiB = teamB.rd / SCALE;

  const combinedPhi = Math.sqrt(phiA * phiA + phiB * phiB);
  const gCombined = g(combinedPhi);

  return expectedScore(muA, muB, gCombined);
}
