export type PitchType = "batting_friendly" | "balanced" | "spin_friendly";

const VENUE_PITCH_TYPE_BY_ALIAS: Record<string, PitchType> = {
  wankhede: "batting_friendly",
  chinnaswamy: "batting_friendly",
  bengaluru: "batting_friendly",
  bangalore: "batting_friendly",
  indore: "batting_friendly",
  rajkot: "batting_friendly",
  brabourne: "batting_friendly",
  dy: "batting_friendly",

  chepauk: "spin_friendly",
  chidambaram: "spin_friendly",
  chennai: "spin_friendly",
  lucknow: "spin_friendly",
  ekana: "spin_friendly",
  uppal: "spin_friendly",
  hyderabad: "spin_friendly",
  delhi: "spin_friendly",
  kotla: "spin_friendly",
  feroz: "spin_friendly",

  ahmedabad: "balanced",
  motera: "balanced",
  kolkata: "balanced",
  eden: "balanced",
  jaipur: "balanced",
  mohali: "balanced",
  chandigarh: "balanced",
  dharamsala: "balanced",
  guwahati: "balanced",
  visakhapatnam: "balanced",
  pune: "balanced",
  cuttack: "balanced",
  ranchi: "balanced",
  kanpur: "balanced",
  mullanpur: "balanced",
};

export interface PitchConditionFeatures {
  pitchType: PitchType;
  pitchBattingIndex: number;
  isSpinFriendly: boolean;
  isBattingFriendly: boolean;
}

export function computePitchConditionFeatures(
  venueName: string | null,
): PitchConditionFeatures {
  const pitchType = computePitchTypeForVenue(venueName);

  return {
    pitchType,
    pitchBattingIndex: pitchTypeToBattingIndex(pitchType),
    isSpinFriendly: pitchType === "spin_friendly",
    isBattingFriendly: pitchType === "batting_friendly",
  };
}

export function computePitchTypeForVenue(venueName: string | null): PitchType {
  if (!venueName) {
    return "balanced";
  }

  const normalized = venueName.toLowerCase();
  for (const [alias, pitchType] of Object.entries(VENUE_PITCH_TYPE_BY_ALIAS)) {
    if (normalized.includes(alias)) {
      return pitchType;
    }
  }

  return "balanced";
}

function pitchTypeToBattingIndex(pitchType: PitchType): number {
  if (pitchType === "batting_friendly") {
    return 1;
  }

  if (pitchType === "spin_friendly") {
    return -1;
  }

  return 0;
}
