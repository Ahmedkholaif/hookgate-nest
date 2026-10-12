import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

export const DATABASE_OPTIONS = Symbol('DATABASE_OPTIONS');

export interface DatabaseOptions {
  url: string;
  maxConnections?: number;
}

// int8 columns (counts, outbox ids) arrive as strings by default. Convert them
// to numbers, refusing anything outside the exactly-representable range.
pg.types.setTypeParser(pg.types.builtins.INT8, (v) => {
  const n = Number(v);
  if (!Number.isSafeInteger(n)) throw new RangeError(`int8 ${v} exceeds Number.MAX_SAFE_INTEGER`);
  return n;
});

// Same lock id and migration names as hookgate (Go), so either service can
// migrate a database and the other recognises it.
const MIGRATION_LOCK_ID = 727_300_001;
const MIGRATIONS_DIR = join(dirname(fileURLToPath(import.meta.url)), 'migrations');

@Injectable()
export class DatabaseService implements OnModuleInit, OnApplicationShutdown {
  readonly pool: pg.Pool;
  private readonly log = new Logger(DatabaseService.name);

  constructor(@Inject(DATABASE_OPTIONS) opts: DatabaseOptions) {
    this.pool = new pg.Pool({ connectionString: opts.url, max: opts.maxConnections ?? 20 });
  }

  async onModuleInit() {
    await this.migrate();
  }

  async onApplicationShutdown() {
    await this.pool.end();
  }

  query<R extends pg.QueryResultRow = any>(sql: string, params?: unknown[]) {
    return this.pool.query<R>(sql, params);
  }

  /** Runs fn in a transaction, committing on success and rolling back on any error. */
  async transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  private async migrate() {
    const client = await this.pool.connect();
    try {
      await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
      await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
        name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
      const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();
      for (const file of files) {
        const name = `migrations/${file}`;
        const { rowCount } = await client.query('SELECT 1 FROM schema_migrations WHERE name = $1', [name]);
        if (rowCount) continue;
        await client.query('BEGIN');
        try {
          await client.query(await readFile(join(MIGRATIONS_DIR, file), 'utf8'));
          await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [name]);
          await client.query('COMMIT');
          this.log.log(`applied ${name}`);
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
      }
    } finally {
      await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]).catch(() => undefined);
      client.release();
    }
  }
}
