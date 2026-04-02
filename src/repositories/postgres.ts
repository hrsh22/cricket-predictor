import { Pool, type PoolClient, type PoolConfig } from "pg";

export type SqlExecutor = Pick<Pool | PoolClient, "query">;

export function createPgPool(databaseUrl: string): Pool {
  const config: PoolConfig = {
    connectionString: databaseUrl,
  };

  return new Pool(config);
}

export async function closePgPool(pool: Pool): Promise<void> {
  await pool.end();
}
