import { loadAppConfig } from "../../src/config/index.js";
import { closePgPool, createPgPool } from "../../src/repositories/postgres.js";

interface StyleFields {
  battingHand: string | null;
  bowlingArm: string | null;
  bowlingStyle: string | null;
  bowlingTypeGroup: string | null;
  playerRole: string | null;
}

async function main(): Promise<void> {
  const pool = createPgPool(loadAppConfig().databaseUrl);

  try {
    const result = await pool.query<{
      id: number;
      canonical_name: string;
      cricbuzz_id: string;
    }>(
      `
        select
          id,
          canonical_name,
          metadata->'externalIds'->>'cricbuzzId' as cricbuzz_id
        from player_registry
        where metadata->'externalIds'->>'cricbuzzId' is not null
        order by canonical_name asc
      `,
    );

    let updated = 0;
    for (const row of result.rows) {
      const style = await fetchCricbuzzStyle(
        row.cricbuzz_id,
        row.canonical_name,
      );
      if (
        style.battingHand === null &&
        style.bowlingStyle === null &&
        style.playerRole === null
      ) {
        continue;
      }

      await pool.query(
        `
          update player_registry
          set
            batting_style = coalesce($2, batting_style),
            bowling_style = coalesce($3, bowling_style),
            bowling_type_group = coalesce($4, bowling_type_group),
            player_role = coalesce($5, player_role),
            metadata = jsonb_set(
              metadata::jsonb,
              '{styleSource}',
              to_jsonb($6::text),
              true
            ),
            updated_at = now()
          where id = $1
        `,
        [
          row.id,
          style.battingHand,
          style.bowlingStyle,
          style.bowlingTypeGroup,
          style.playerRole,
          "cricbuzz_profile",
        ],
      );
      updated += 1;
    }

    process.stdout.write(
      `${JSON.stringify({ scanned: result.rows.length, updated }, null, 2)}\n`,
    );
  } finally {
    await closePgPool(pool);
  }
}

async function fetchCricbuzzStyle(
  cricbuzzId: string,
  canonicalName: string,
): Promise<StyleFields> {
  const response = await fetch(
    `https://www.cricbuzz.com/profiles/${cricbuzzId}/${slugify(canonicalName)}`,
    {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "text/html",
      },
    },
  );
  if (!response.ok) {
    return {
      battingHand: null,
      bowlingArm: null,
      bowlingStyle: null,
      bowlingTypeGroup: null,
      playerRole: null,
    };
  }

  const html = await response.text();
  const role = extractLabeledField(html, "Role");
  const battingStyle = extractLabeledField(html, "Batting Style");
  const bowlingStyle = extractLabeledField(html, "Bowling Style");

  return {
    battingHand: normalizeBattingHand(battingStyle),
    bowlingArm: normalizeBowlingArm(bowlingStyle),
    bowlingStyle,
    bowlingTypeGroup: normalizeBowlingTypeGroup(bowlingStyle),
    playerRole: normalizeRole(role),
  };
}

function extractLabeledField(html: string, label: string): string | null {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const match = html.match(
    new RegExp(`${escaped}</div><div[^>]*>([^<]+)</div>`, "iu"),
  );
  const value = match?.[1]?.trim() ?? null;
  return value === null || value.length === 0 ? null : value;
}

function normalizeBattingHand(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (normalized.includes("left")) return "left";
  if (normalized.includes("right")) return "right";
  return null;
}

function normalizeBowlingArm(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (normalized.includes("left-arm") || normalized.includes("left arm"))
    return "left";
  if (normalized.includes("right-arm") || normalized.includes("right arm"))
    return "right";
  return null;
}

function normalizeBowlingTypeGroup(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.toLowerCase();
  if (
    normalized.includes("orthodox") ||
    normalized.includes("legbreak") ||
    normalized.includes("leg-break") ||
    normalized.includes("offbreak") ||
    normalized.includes("off-break") ||
    normalized.includes("slow left-arm") ||
    normalized.includes("chinaman")
  ) {
    return "spin";
  }
  if (
    normalized.includes("medium") ||
    normalized.includes("fast") ||
    normalized.includes("seam")
  ) {
    return "pace";
  }
  return null;
}

function normalizeRole(value: string | null): string | null {
  if (value === null) return null;
  const normalized = value.trim().toLowerCase();
  if (normalized.includes("wicket") && normalized.includes("batter")) {
    return "wicketkeeper_batter";
  }
  if (normalized.includes("allround") || normalized.includes("all round")) {
    return "all_rounder";
  }
  if (normalized.includes("bowler")) {
    return "bowler";
  }
  if (normalized.includes("batter") || normalized.includes("batsman")) {
    return "batter";
  }
  return null;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Cricbuzz style enrichment failed: ${message}`);
  process.exitCode = 1;
});
