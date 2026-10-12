export const APP_CONFIG = Symbol('APP_CONFIG');

export type Role = 'api' | 'relay' | 'processor' | 'sink';

/** Same environment variables and defaults as hookgate (Go). */
export interface AppConfig {
  databaseUrl: string;
  roles: Role[];
  eventsTopic: string;
  apiKey?: string;
  relay: { batchSize: number; pollMs: number; leaseMs: number };
  processor: { concurrency: number; maxAttempts: number; retryBaseMs: number; retryMaxMs: number; chaosFailTypes: string[] };
  sink: { topics: string[]; concurrency: number };
}

export function list(v: string | undefined): string[] {
  return (v ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

function int(v: string | undefined, def: number) {
  const n = Number(v);
  return Number.isInteger(n) && n > 0 ? n : def;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const roles = list(env.ROLES || 'api,relay,processor,sink');
  for (const r of roles) {
    if (!['api', 'relay', 'processor', 'sink'].includes(r)) throw new Error(`unknown role "${r}" (want api, relay, processor, sink)`);
  }
  return {
    databaseUrl: env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/hookgate',
    roles: roles as Role[],
    eventsTopic: env.EVENTS_TOPIC || 'hookgate-events',
    apiKey: env.API_KEY || undefined,
    relay: { batchSize: int(env.RELAY_BATCH, 100), pollMs: int(env.RELAY_POLL_MS, 200), leaseMs: 30_000 },
    processor: {
      concurrency: int(env.PROCESSOR_CONCURRENCY, 16),
      maxAttempts: int(env.MAX_ATTEMPTS, 5),
      retryBaseMs: int(env.RETRY_BASE_MS, 1000),
      retryMaxMs: int(env.RETRY_MAX_MS, 300_000),
      chaosFailTypes: list(env.CHAOS_FAIL_TYPES),
    },
    sink: { topics: list(env.SINK_TOPICS), concurrency: int(env.SINK_CONCURRENCY, 16) },
  };
}
