import { describe, expect, it } from "vitest";

import { cricapiAdapter } from "../../src/index.js";
import {
  dlsCricapiPayload,
  noResultCricapiPayload,
  superOverCricapiPayload,
} from "../fixtures/cricket/cricapi-edge-cases.js";
import {
  finalResultCricapiPayload,
  incompleteTossCricapiPayload,
  inningsBreakCricapiPayload,
  postTossCricapiPayload,
  preMatchCricapiPayload,
} from "../fixtures/cricket/cricapi.js";

describe("cricket provider adapter contract", () => {
  it("maps a pre-match payload to fixture data and pre_match lifecycle", () => {
    const fixture = cricapiAdapter.getFixture(preMatchCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(preMatchCricapiPayload);
    const toss = cricapiAdapter.getToss(preMatchCricapiPayload);

    expect(fixture.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "pre_match" });
    expect(toss).toEqual({
      status: "unavailable",
      reason: "toss_not_reported",
    });

    if (fixture.status !== "available") {
      throw new Error("expected fixture to be available");
    }

    expect(fixture.value.competition).toBe("IPL");
    expect(fixture.value.status).toBe("scheduled");
    expect(fixture.value.tossWinnerTeamName).toBeNull();
    expect(fixture.value.tossDecision).toBeNull();
  });

  it("maps a post-toss payload to toss data and post_toss lifecycle", () => {
    const toss = cricapiAdapter.getToss(postTossCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(postTossCricapiPayload);

    expect(toss.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "post_toss" });

    if (toss.status !== "available") {
      throw new Error("expected toss to be available");
    }

    expect(toss.value.tossWinnerTeamName).toBe("Mumbai Indians");
    expect(toss.value.tossDecision).toBe("bowl");
  });

  it("maps an innings-break payload to innings state and innings_break lifecycle", () => {
    const inningsState = cricapiAdapter.getInningsState(
      inningsBreakCricapiPayload,
    );
    const lifecycle = cricapiAdapter.getLifecycleState(
      inningsBreakCricapiPayload,
    );

    expect(inningsState.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "innings_break" });

    if (inningsState.status !== "available") {
      throw new Error("expected innings state to be available");
    }

    expect(inningsState.value.inningsNumber).toBe(1);
    expect(inningsState.value.battingTeamName).toBe("Chennai Super Kings");
    expect(inningsState.value.runs).toBe(176);
    expect(inningsState.value.targetRuns).toBe(177);
    expect(inningsState.value.requiredRunRate).toBe(8.85);
  });

  it("maps a completed payload to final-result retrieval and final_result lifecycle", () => {
    const result = cricapiAdapter.getFinalResult(finalResultCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(
      finalResultCricapiPayload,
    );
    const fixture = cricapiAdapter.getFixture(finalResultCricapiPayload);

    expect(result.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "final_result" });
    expect(fixture.status).toBe("available");

    if (result.status !== "available") {
      throw new Error("expected final result to be available");
    }

    expect(result.value.resultType).toBe("win");
    expect(result.value.winningTeamName).toBe("Mumbai Indians");
  });

  it("maps a no-result payload to an explicit no_result final state", () => {
    const result = cricapiAdapter.getFinalResult(noResultCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(noResultCricapiPayload);

    expect(result.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "final_result" });

    if (result.status !== "available") {
      throw new Error("expected no-result payload to resolve");
    }

    expect(result.value.resultType).toBe("no_result");
    expect(result.value.winningTeamName).toBeNull();
  });

  it("maps a super-over payload to an explicit super_over final state", () => {
    const result = cricapiAdapter.getFinalResult(superOverCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(superOverCricapiPayload);

    expect(result.status).toBe("available");
    expect(lifecycle).toEqual({ status: "available", value: "final_result" });

    if (result.status !== "available") {
      throw new Error("expected super-over payload to resolve");
    }

    expect(result.value.resultType).toBe("super_over");
    expect(result.value.winningTeamName).toBe("Gujarat Titans");
  });

  it("still exposes DLS payloads as final-result provider data for later normalization decisions", () => {
    const result = cricapiAdapter.getFinalResult(dlsCricapiPayload);

    expect(result.status).toBe("available");

    if (result.status !== "available") {
      throw new Error("expected DLS payload to resolve at adapter level");
    }

    expect(result.value.resultType).toBe("win");
    expect(result.value.winningTeamName).toBe("Delhi Capitals");
  });

  it("degrades explicitly when payload is incomplete for toss retrieval", () => {
    const toss = cricapiAdapter.getToss(incompleteTossCricapiPayload);
    const lifecycle = cricapiAdapter.getLifecycleState(
      incompleteTossCricapiPayload,
    );

    expect(toss.status).toBe("degraded");
    expect(lifecycle.status).toBe("degraded");

    if (toss.status !== "degraded") {
      throw new Error("expected toss to be degraded");
    }

    expect(toss.issues.map((issue) => issue.path)).toEqual(["cricapi.toss"]);
  });
});
