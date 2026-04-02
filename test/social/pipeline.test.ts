import { describe, expect, it } from "vitest";

import type { SocialSignalSource } from "../../src/social/contract.js";
import { buildSocialSignalCandidate, extractFeatures } from "../../src/social/pipeline.js";

const mockSource: SocialSignalSource = {
  providerKey: "test",
  sourceType: "analyst_note",
  sourceId: "123",
  sourceLabel: "Test Analyst",
  sourceQuality: "trusted",
  capturedAt: new Date().toISOString(),
  publishedAt: null,
  provenanceUrl: null,
};

const context = {
  matchSlug: "csk-vs-mi-2024-04-01",
  checkpointType: "pre_match" as const,
  targetTeamName: "CSK",
  opponentTeamName: "MI",
};

describe("Social Analysis Pipeline", () => {
  it("should extract features for positive target hype", () => {
    const text = "CSK will dominate this match. Strong form and unstoppable momentum.";
    const features = extractFeatures(text, context);
    
    expect(features.isEmptyOrNoisy).toBe(false);
    expect(features.hypeImbalanceScore).toBeGreaterThan(0);
    expect(features.lineupRiskMentions).toBe(0);
  });

  it("should extract features for opponent hype", () => {
    const text = "MI looks unstoppable today, a strong favorite to win.";
    const features = extractFeatures(text, context);
    
    expect(features.isEmptyOrNoisy).toBe(false);
    expect(features.hypeImbalanceScore).toBeLessThan(0);
  });

  it("should handle empty or whitespace text neutrally", () => {
    const candidate = buildSocialSignalCandidate("   ", mockSource, context);
    expect(candidate).toBeNull();
  });

  it("should build a valid candidate with explicit provenance for clear signals", () => {
    const text = "CSK has great momentum and will win.";
    const candidate = buildSocialSignalCandidate(text, mockSource, context);
    
    expect(candidate).not.toBeNull();
    expect(candidate?.competition).toBe("IPL");
    expect(candidate?.matchSlug).toBe(context.matchSlug);
    expect(candidate?.source.providerKey).toBe("test");
    expect(candidate?.confidence).toBeGreaterThan(0.5);
    expect(candidate?.requestedAdjustment).toBeGreaterThan(0);
    expect(candidate?.summary).toContain("Positive hype detected for CSK");
  });

  it("should penalize confidence for lineup risk and uncertainty", () => {
    const text = "CSK might win, but injury to their captain makes it a toss-up and tricky.";
    const candidate = buildSocialSignalCandidate(text, mockSource, context);
    
    expect(candidate).not.toBeNull();
    // Risk + uncertainty reduces confidence
    expect(candidate!.confidence).toBeLessThan(0.6);
    expect(candidate!.summary).toContain("Lineup risk");
  });

  it("should return null for contradictory or noisy inputs", () => {
    const text = "CSK and MI are both unstoppable and strong and will win.";
    // Both teams mentioned equally, high hype terms -> contradictory
    const features = extractFeatures(text, context);
    expect(features.isContradictory).toBe(true);

    const candidate = buildSocialSignalCandidate(text, mockSource, context);
    expect(candidate).toBeNull();
  });
});
