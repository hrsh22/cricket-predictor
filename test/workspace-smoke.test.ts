import { describe, expect, it } from "vitest";

import { getWorkspaceStatus, workspaceReady } from "../src/index.js";

describe("workspace smoke", () => {
  it("boots the empty workspace", () => {
    expect(workspaceReady).toBe(true);
    expect(getWorkspaceStatus()).toBe("ipl-predictor-workspace-ready");
  });
});
