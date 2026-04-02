import { describe, expect, it } from "vitest";

import {
  clampSocialAdjustment,
  defaultSocialSignalPolicy,
  parseSocialSignalCandidate,
  resolveSocialSignal,
} from "../../src/social/index.js";
import {
  emptySummarySocialSignalCandidate,
  lowConfidenceSocialSignalCandidate,
  noisySocialSignalCandidate,
  socialSignalContext,
  validSocialSignalCandidate,
} from "../fixtures/social/signal-payloads.js";

describe("social signal policy", () => {
  it("parses an auditable social signal candidate with provenance fields", () => {
    const candidate = parseSocialSignalCandidate(validSocialSignalCandidate);

    expect(candidate.source.providerKey).toBe("mirofish-inspired-manual");
    expect(candidate.source.sourceId).toBe("note-001");
    expect(candidate.summary).toContain("batting depth");
    expect(candidate.confidence).toBe(0.74);
    expect(candidate.requestedAdjustment).toBe(0.08);
  });

  it("clamps social adjustments to the configured cap in both directions", () => {
    expect(clampSocialAdjustment(0.08, 0.05)).toBe(0.05);
    expect(clampSocialAdjustment(-0.09, 0.05)).toBe(-0.05);
    expect(clampSocialAdjustment(0.03, 0.05)).toBe(0.03);
  });

  it("applies a bounded social adjustment without allowing unbounded override", () => {
    const signal = resolveSocialSignal(socialSignalContext, parseSocialSignalCandidate(validSocialSignalCandidate));

    expect(signal.status).toBe("applied");
    expect(signal.reason).toBe("clamped_to_cap");
    expect(signal.adjustmentCap).toBe(defaultSocialSignalPolicy.adjustmentCap);
    expect(signal.requestedAdjustment).toBe(0.08);
    expect(signal.boundedAdjustment).toBe(0.05);
  });

  it("returns a neutral no-op signal when the social layer is disabled", () => {
    const signal = resolveSocialSignal(socialSignalContext, parseSocialSignalCandidate(validSocialSignalCandidate), {
      enabled: false,
    });

    expect(signal.status).toBe("disabled");
    expect(signal.reason).toBe("disabled_by_policy");
    expect(signal.boundedAdjustment).toBe(0);
    expect(signal.summary).toContain("batting depth");
  });

  it("returns a neutral fallback for missing input", () => {
    const signal = resolveSocialSignal(socialSignalContext, null);

    expect(signal.status).toBe("fallback");
    expect(signal.reason).toBe("missing_signal");
    expect(signal.source).toBeNull();
    expect(signal.boundedAdjustment).toBe(0);
  });

  it("returns a neutral fallback for whitespace-only summaries", () => {
    const signal = resolveSocialSignal(
      socialSignalContext,
      parseSocialSignalCandidate(emptySummarySocialSignalCandidate),
    );

    expect(signal.status).toBe("fallback");
    expect(signal.reason).toBe("empty_summary");
    expect(signal.boundedAdjustment).toBe(0);
  });

  it("returns a neutral fallback for low-confidence input", () => {
    const signal = resolveSocialSignal(
      socialSignalContext,
      parseSocialSignalCandidate(lowConfidenceSocialSignalCandidate),
    );

    expect(signal.status).toBe("fallback");
    expect(signal.reason).toBe("low_confidence");
    expect(signal.boundedAdjustment).toBe(0);
  });

  it("returns a neutral fallback for noisy social input", () => {
    const signal = resolveSocialSignal(socialSignalContext, parseSocialSignalCandidate(noisySocialSignalCandidate));

    expect(signal.status).toBe("fallback");
    expect(signal.reason).toBe("source_quality_rejected");
    expect(signal.boundedAdjustment).toBe(0);
  });
});
