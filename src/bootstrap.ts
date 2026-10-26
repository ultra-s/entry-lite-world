/** Entry-Lite World — schema bootstrap CLI. */
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Pool } from 'pg';

export async function bootstrap(connectionString?: string): Promise<void> {
  const cs =
    connectionString ??
    process.env.ENTRYLITE_WORLD_URL ??
    process.env.WORKFLOW_POSTGRES_URL ??
    process.env.DATABASE_URL ??
    'postgres://world:world@localhost:5432/workflow';
  const pool = new Pool({ connectionString: cs });
  try {
    const sqlPath = join(dirname(fileURLToPath(import.meta.url)), 'schema.sql');
    const sql = readFileSync(sqlPath, 'utf8');
    await pool.query(sql);
    console.log('[entrylite-world] schema created successfully');
  } finally {
    await pool.end();
  }
}

// CLI entry: `node dist/bootstrap.js [connection-string]`
if (process.argv[1] && process.argv[1].endsWith('bootstrap.js')) {
  bootstrap(process.argv[2]).then(
    () => process.exit(0),
    (err) => {
      console.error('[entrylite-world] bootstrap failed:', err);
      process.exit(1);
    },
  );
}
