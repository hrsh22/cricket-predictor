import type { CheckpointType } from "../domain/checkpoint.js";
import type { SocialSignalCandidate, SocialSignalSource } from "./contract.js";

export interface SocialAnalysisContext {
  matchSlug: string;
  checkpointType: CheckpointType;
  targetTeamName: string;
  opponentTeamName: string;
}

export interface StructuredSocialFeatures {
  hypeImbalanceScore: number;
  lineupRiskMentions: number;
  uncertaintyMentions: number;
  isContradictory: boolean;
  isEmptyOrNoisy: boolean;
}

export function extractFeatures(text: string, context: SocialAnalysisContext): StructuredSocialFeatures {
  const normalized = text.trim().toLowerCase();

  if (normalized.length === 0) {
    return {
      hypeImbalanceScore: 0,
      lineupRiskMentions: 0,
      uncertaintyMentions: 0,
      isContradictory: false,
      isEmptyOrNoisy: true,
    };
  }

  const targetName = context.targetTeamName.toLowerCase();
  const opponentName = context.opponentTeamName.toLowerCase();

  const targetMentions = countOccurrences(normalized, targetName);
  const opponentMentions = countOccurrences(normalized, opponentName);

  const hypeWords = ["dominate", "strong", "win", "favorite", "unstoppable", "momentum", "form", "smash"];
  const riskWords = ["injury", "missing", "replaced", "doubtful", "out", "bench", "niggle", "drop"];
  const uncertaintyWords = ["maybe", "unclear", "toss-up", "unpredictable", "50-50", "weather", "rain", "tricky"];

  const totalHypeMentions = countAnyMatch(normalized, hypeWords);
  const lineupRiskMentions = countAnyMatch(normalized, riskWords);
  const uncertaintyMentions = countAnyMatch(normalized, uncertaintyWords);

  // Approximate attribute attribution to target vs opponent
  // In a real NLP pipeline, this would use dependency parsing.
  // Here we use simple co-occurrence proxies.
  let hypeImbalanceScore = 0;
  if (targetMentions > opponentMentions) {
    hypeImbalanceScore = totalHypeMentions;
  } else if (opponentMentions > targetMentions) {
    hypeImbalanceScore = -totalHypeMentions;
  }

  // Contradictory if both teams mentioned equally with high hype
  const isContradictory =
    targetMentions > 0 && opponentMentions > 0 && targetMentions === opponentMentions && totalHypeMentions > 1;

  // Noisy if too much uncertainty, or long text with no team mentions
  const isEmptyOrNoisy =
    uncertaintyMentions >= 3 || (targetMentions === 0 && opponentMentions === 0 && normalized.length > 50);

  return {
    hypeImbalanceScore,
    lineupRiskMentions,
    uncertaintyMentions,
    isContradictory,
    isEmptyOrNoisy,
  };
}

export function buildSocialSignalCandidate(
  text: string,
  source: SocialSignalSource,
  context: SocialAnalysisContext,
): SocialSignalCandidate | null {
  const features = extractFeatures(text, context);

  // Safe neutral fallback for empty, contradictory, or noisy inputs
  if (features.isEmptyOrNoisy || features.isContradictory) {
    return null;
  }

  // Calculate requested adjustment based on hype and risk
  // Bounded and explainable mapping
  let adjustment = 0;
  let confidence = 0.5;
  const summaryParts: string[] = [];

  if (features.hypeImbalanceScore > 0) {
    adjustment += 0.02 * Math.min(features.hypeImbalanceScore, 3);
    summaryParts.push(`Positive hype detected for ${context.targetTeamName}.`);
    confidence += 0.1;
  } else if (features.hypeImbalanceScore < 0) {
    adjustment -= 0.02 * Math.min(Math.abs(features.hypeImbalanceScore), 3);
    summaryParts.push(`Positive hype detected for ${context.opponentTeamName}.`);
    confidence += 0.1;
  }

  if (features.lineupRiskMentions > 0) {
    // Lineup risk introduces uncertainty, lower confidence slightly and apply a negative adjustment
    adjustment -= 0.01 * Math.min(features.lineupRiskMentions, 2);
    summaryParts.push(`Lineup risk or injury mentions found.`);
    confidence -= 0.1;
  }

  if (features.uncertaintyMentions > 0) {
    summaryParts.push(`Some uncertainty in the sentiment.`);
    confidence -= 0.15;
  }

  // If no clear signals were extracted despite not being pure noise, fallback
  if (adjustment === 0 && summaryParts.length === 0) {
    return null;
  }

  // Clamp confidence
  confidence = Math.max(0, Math.min(1, confidence));

  // Clamp adjustment tightly to reasonable bounds before policy
  adjustment = Math.max(-0.1, Math.min(0.1, adjustment));

  return {
    competition: "IPL",
    matchSlug: context.matchSlug,
    checkpointType: context.checkpointType,
    targetTeamName: context.targetTeamName,
    source,
    summary: summaryParts.join(" "),
    confidence,
    requestedAdjustment: Number.parseFloat(adjustment.toFixed(3)),
  };
}

function countOccurrences(text: string, term: string): number {
  if (term.length === 0) return 0;
  const regex = new RegExp(`\\b${term}\\b`, 'g');
  const matches = text.match(regex);
  return matches ? matches.length : 0;
}

function countAnyMatch(text: string, terms: string[]): number {
  return terms.reduce((acc, term) => acc + countOccurrences(text, term), 0);
}
