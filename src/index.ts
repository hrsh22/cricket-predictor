export const workspaceReady = true;

export function getWorkspaceStatus(): string {
  return "ipl-predictor-workspace-ready";
}

export * from "./domain/index.js";
export * from "./ingest/cricket/index.js";
export * from "./ingest/polymarket/index.js";
export * from "./matching/index.js";
export * from "./repositories/index.js";
export * from "./scoring/index.js";
export * from "./social/index.js";
