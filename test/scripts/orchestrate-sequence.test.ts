import { describe, expect, it } from "vitest";

import { resolveCheckpointSequence } from "../../scripts/orchestrate.js";

describe("orchestrate checkpoint sequencing", () => {
  it("returns single checkpoint sequence when --checkpoint is used", () => {
    expect(
      resolveCheckpointSequence({
        checkpointType: "pre_match",
        startFrom: null,
      }),
    ).toEqual(["pre_match"]);
  });

  it("returns full sequence when --start-from pre_match is used", () => {
    expect(
      resolveCheckpointSequence({
        checkpointType: null,
        startFrom: "pre_match",
      }),
    ).toEqual(["pre_match", "post_toss", "innings_break"]);
  });

  it("returns post-toss onward sequence when --start-from post_toss is used", () => {
    expect(
      resolveCheckpointSequence({
        checkpointType: null,
        startFrom: "post_toss",
      }),
    ).toEqual(["post_toss", "innings_break"]);
  });

  it("rejects invalid combination of --checkpoint and --start-from", () => {
    expect(() =>
      resolveCheckpointSequence({
        checkpointType: "pre_match",
        startFrom: "post_toss",
      }),
    ).toThrow(/either --checkpoint or --start-from/i);
  });

  it("rejects missing checkpoint selection", () => {
    expect(() =>
      resolveCheckpointSequence({
        checkpointType: null,
        startFrom: null,
      }),
    ).toThrow(/Missing required argument/i);
  });
});
