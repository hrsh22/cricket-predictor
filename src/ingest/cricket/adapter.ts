import type { CanonicalMatch, MatchResultType } from "../../domain/index.js";

export type CricketProviderKey = "cricapi";

export type CricketLifecycleState =
  | "pre_match"
  | "post_toss"
  | "innings_break"
  | "final_result";

export interface AdapterIssue {
  path: string;
  message: string;
}

export type AdapterRetrievalResult<T> =
  | {
      status: "available";
      value: T;
    }
  | {
      status: "unavailable";
      reason: string;
    }
  | {
      status: "degraded";
      issues: readonly AdapterIssue[];
    };

export interface CricketTossState {
  tossWinnerTeamName: string;
  tossDecision: "bat" | "bowl";
}

export interface CricketInningsState {
  inningsNumber: 1 | 2;
  battingTeamName: string;
  bowlingTeamName: string;
  runs: number;
  wickets: number;
  overs: number;
  targetRuns: number;
  currentRunRate: number;
  requiredRunRate: number;
}

export interface CricketFinalResult {
  winningTeamName: string | null;
  resultType: MatchResultType;
}

export interface CricketDataProviderAdapter {
  readonly providerKey: CricketProviderKey;
  getFixture(payload: unknown): AdapterRetrievalResult<CanonicalMatch>;
  getToss(payload: unknown): AdapterRetrievalResult<CricketTossState>;
  getInningsState(
    payload: unknown,
  ): AdapterRetrievalResult<CricketInningsState>;
  getFinalResult(payload: unknown): AdapterRetrievalResult<CricketFinalResult>;
  getLifecycleState(
    payload: unknown,
  ): AdapterRetrievalResult<CricketLifecycleState>;
}
