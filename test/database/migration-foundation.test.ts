import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  DEFAULT_DATABASE_URL,
  MIGRATIONS_DIRECTORY,
  isSafeResetTarget,
  parseDatabaseUrl,
  withDatabaseName,
} from "../../database/config.js";
import { loadMigrations } from "../../database/migration-runner.js";
import { expectedCoreTables } from "../fixtures/database/expected-core-tables.js";

describe("database migration foundation", () => {
  it("loads paired up and down migration files", async () => {
    const migrations = await loadMigrations();

    expect(migrations).toHaveLength(6);
    expect(migrations[0]).toMatchObject({
      id: "001_initial_schema",
      upFilePath: `${MIGRATIONS_DIRECTORY}/001_initial_schema.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/001_initial_schema.down.sql`,
    });
    expect(migrations[1]).toMatchObject({
      id: "002_market_match_mappings",
      upFilePath: `${MIGRATIONS_DIRECTORY}/002_market_match_mappings.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/002_market_match_mappings.down.sql`,
    });
    expect(migrations[2]).toMatchObject({
      id: "003_player_foundation",
      upFilePath: `${MIGRATIONS_DIRECTORY}/003_player_foundation.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/003_player_foundation.down.sql`,
    });
    expect(migrations[3]).toMatchObject({
      id: "004_player_style_profiles",
      upFilePath: `${MIGRATIONS_DIRECTORY}/004_player_style_profiles.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/004_player_style_profiles.down.sql`,
    });
    expect(migrations[4]).toMatchObject({
      id: "005_polymarket_historical_odds",
      upFilePath: `${MIGRATIONS_DIRECTORY}/005_polymarket_historical_odds.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/005_polymarket_historical_odds.down.sql`,
    });
    expect(migrations[5]).toMatchObject({
      id: "006_team_season_squads",
      upFilePath: `${MIGRATIONS_DIRECTORY}/006_team_season_squads.up.sql`,
      downFilePath: `${MIGRATIONS_DIRECTORY}/006_team_season_squads.down.sql`,
    });
  });

  it("creates every required core table in the initial schema", async () => {
    const migrations = await loadMigrations();
    const sqlChunks = await Promise.all(
      migrations.map((migration) => readFile(migration.upFilePath, "utf8")),
    );
    const sql = sqlChunks.join("\n");

    for (const tableName of expectedCoreTables) {
      expect(sql).toContain(`create table ${tableName}`);
    }

    expect(sql).toContain(
      "create type checkpoint_type as enum ('pre_match', 'post_toss', 'innings_break')",
    );
  });

  it("parses the local PostgreSQL default database URL", () => {
    expect(parseDatabaseUrl(DEFAULT_DATABASE_URL).pathname).toBe(
      "/sports_predictor_mvp",
    );
  });

  it("rejects invalid reset targets unless they are local MVP or *_test databases", () => {
    expect(
      isSafeResetTarget(
        withDatabaseName(DEFAULT_DATABASE_URL, "sports_predictor_mvp"),
      ),
    ).toBe(true);
    expect(
      isSafeResetTarget(
        withDatabaseName(DEFAULT_DATABASE_URL, "sports_predictor_test"),
      ),
    ).toBe(true);
    expect(
      isSafeResetTarget(withDatabaseName(DEFAULT_DATABASE_URL, "postgres")),
    ).toBe(false);
  });
});
