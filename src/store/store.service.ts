import { Injectable } from '@nestjs/common';
import { createHash, randomBytes } from 'node:crypto';
import type pg from 'pg';
import { HookgateError } from '../common/errors.js';
import { DatabaseService } from '../database/database.service.js';

/**
 * hookgate's PostgreSQL state. Every statement mirrors the Go store, so Go
 * and NestJS processes can share one database (and one outbox) and make
 * exactly the same state transitions.
 */

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SOURCE_NAME = /^[a-z0-9][a-z0-9_-]{0,62}$/;
const TOPIC_NAME = /^[A-Za-z][A-Za-z0-9_-]{0,79}$/;
const MESSAGE_NAMESPACE = '4b1f6c2e-7d3a-5e9b-8c1d-2f6a9e0b3c71';

/** UUIDv5(namespace, eventId + "/" + topic): the same id the Go service derives. */
export function messageId(eventId: string, topic: string): string {
  const ns = Buffer.from(MESSAGE_NAMESPACE.replace(/-/g, ''), 'hex');
  const h = createHash('sha1').update(ns).update(`${eventId}/${topic}`).digest();
  h[6] = (h[6] & 0x0f) | 0x50; // version 5
  h[8] = (h[8] & 0x3f) | 0x80; // RFC 4122 variant
  const x = h.subarray(0, 16).toString('hex');
  return `${x.slice(0, 8)}-${x.slice(8, 12)}-${x.slice(12, 16)}-${x.slice(16, 20)}-${x.slice(20)}`;
}

export interface Source {
  name: string;
  secret?: string;
  signature_header: string;
  id_header: string;
  type_field: string;
  created_at: Date;
}

export interface Route {
  id: string;
  source: string;
  event_type: string;
  topic: string;
  created_at: Date;
}

export interface OutboxItem {
  id: number;
  kind: 'event' | 'message';
  ref_id: string;
  topic: string;
  attempt: number;
  publishes: number;
  source: string;
  type: string;
  event_id: string;
  payload: unknown;
  /** Database time of the claim, before the publish. */
  claimed_at: Date;
}

export type Outcome = 'processed' | 'skipped' | 'retrying' | 'dead';

export interface EventRecord {
  id: string;
  source: string;
  type: string;
  payload: unknown;
  attempts: number;
}

export interface ProcessConfig {
  eventsTopic: string;
  maxAttempts: number;
  retryDelayMs: (attempt: number) => number;
  /** Runs inside the processing transaction before routing; throwing fails the attempt. */
  work?: (ev: EventRecord) => Promise<void>;
}

const SOURCE_COLS = 'name, secret, signature_header, id_header, type_field, created_at';
const ROUTE_COLS = 'id::text, source, event_type, topic, created_at';
const EVENT_COLS = 'id::text, source, dedup_key, type, status, attempts, duplicates, last_error, received_at, processed_at';
const MESSAGE_COLS = 'id::text, event_id::text, topic, status, created_at, published_at, delivered_at';

@Injectable()
export class StoreService {
  constructor(private readonly db: DatabaseService) {}

  // --------------------------------------------------------------- sources

  async createSource(input: Partial<Source>): Promise<Source> {
    if (!input.name || !SOURCE_NAME.test(input.name)) {
      throw HookgateError.invalid('name must match ^[a-z0-9][a-z0-9_-]{0,62}$');
    }
    const secret = input.secret || 'whsec_' + randomBytes(24).toString('base64url');
    try {
      const { rows } = await this.db.query<Source>(
        `INSERT INTO sources (name, secret, signature_header, id_header, type_field)
         VALUES ($1, $2, coalesce(nullif($3, ''), 'Hookgate-Signature'), coalesce(nullif($4, ''), 'Webhook-Id'), coalesce(nullif($5, ''), 'type'))
         RETURNING ${SOURCE_COLS}`,
        [input.name, secret, input.signature_header ?? '', input.id_header ?? '', input.type_field ?? ''],
      );
      return rows[0];
    } catch (err) {
      if (pgCode(err) === '23505') throw HookgateError.conflict(`source "${input.name}" already exists`);
      throw err;
    }
  }

  async getSource(name: string): Promise<Source> {
    const { rows } = await this.db.query<Source>(`SELECT ${SOURCE_COLS} FROM sources WHERE name = $1`, [name]);
    if (!rows[0]) throw HookgateError.notFound();
    return rows[0];
  }

  async listSources(): Promise<Source[]> {
    return (await this.db.query<Source>(`SELECT ${SOURCE_COLS} FROM sources ORDER BY name`)).rows;
  }

  // ---------------------------------------------------------------- routes

  async createRoute(source: string, eventType: string, topic: string): Promise<Route> {
    if (!eventType || eventType.length > 200) throw HookgateError.invalid('event_type is required');
    if (!TOPIC_NAME.test(topic ?? '')) {
      throw HookgateError.invalid('topic must match ^[A-Za-z][A-Za-z0-9_-]{0,79}$ (valid on RabbitMQ, SQS and Pub/Sub)');
    }
    try {
      const { rows } = await this.db.query<Route>(
        `INSERT INTO routes (source, event_type, topic) VALUES ($1, $2, $3) RETURNING ${ROUTE_COLS}`,
        [source, eventType, topic],
      );
      return rows[0];
    } catch (err) {
      if (pgCode(err) === '23505') throw HookgateError.conflict('route already exists');
      if (pgCode(err) === '23503') throw HookgateError.notFound(`source "${source}" does not exist`);
      throw err;
    }
  }

  async listRoutes(source = ''): Promise<Route[]> {
    const { rows } = await this.db.query<Route>(
      `SELECT ${ROUTE_COLS} FROM routes WHERE $1 = '' OR source = $1 ORDER BY source, event_type, topic`,
      [source],
    );
    return rows;
  }

  async deleteRoute(id: string) {
    const { rowCount } = await this.db.query(`DELETE FROM routes WHERE id::text = $1`, [id]);
    if (!rowCount) throw HookgateError.notFound();
  }

  // ---------------------------------------------------------------- ingest

  /** Records an inbound webhook and its outbox row atomically; a repeat of (source, dedupKey) creates nothing. */
  async ingest(source: string, dedupKey: string, type: string, payload: Buffer, eventsTopic: string, actor: string) {
    return this.db.transaction(async (c) => {
      // xmax = 0 only for a freshly inserted row; on conflict the UPDATE bumps
      // the duplicate counter and returns the existing row.
      const { rows } = await c.query<{ id: string; status: string; inserted: boolean }>(
        `INSERT INTO events (source, dedup_key, type, payload) VALUES ($1, $2, $3, $4)
         ON CONFLICT (source, dedup_key) DO UPDATE SET duplicates = events.duplicates + 1, updated_at = now()
         RETURNING id::text, status, xmax = 0 AS inserted`,
        [source, dedupKey, type, payload.toString('utf8')],
      );
      const { id, status, inserted } = rows[0];
      if (inserted) {
        await c.query(`INSERT INTO outbox (kind, ref_id, topic, attempt) VALUES ('event', $1, $2, 1)`, [id, eventsTopic]);
        await logStatus(c, id, 'received', '', actor);
      } else {
        await logStatus(c, id, 'duplicate_dropped', 'webhook re-sent with the same id', actor);
      }
      return { id, status, duplicate: !inserted };
    });
  }

  // ---------------------------------------------------------------- outbox

  /**
   * Leases up to `limit` ready rows. SKIP LOCKED lets any number of relays
   * (Go or NestJS) share the outbox; a relay that dies mid-publish loses its
   * lease and another relay re-publishes the same ids.
   */
  async claimOutbox(limit: number, leaseMs: number): Promise<OutboxItem[]> {
    const { rows } = await this.db.query<OutboxItem>(
      `WITH claimed AS (
         UPDATE outbox SET locked_until = now() + $2 * interval '1 millisecond', publishes = publishes + 1
         WHERE id IN (
           SELECT id FROM outbox
           WHERE available_at <= now() AND (locked_until IS NULL OR locked_until < now())
           ORDER BY available_at, id LIMIT $1 FOR UPDATE SKIP LOCKED)
         RETURNING id, kind, ref_id, topic, attempt, publishes)
       SELECT c.id, c.kind, c.ref_id::text, c.topic, c.attempt, c.publishes,
              coalesce(e.source, me.source, '') AS source, coalesce(e.type, me.type, '') AS type,
              coalesce(e.id, m.event_id)::text AS event_id, coalesce(e.payload, m.payload) AS payload,
              clock_timestamp() AS claimed_at
       FROM claimed c
       LEFT JOIN events e ON c.kind = 'event' AND e.id = c.ref_id
       LEFT JOIN messages m ON c.kind = 'message' AND m.id = c.ref_id
       LEFT JOIN events me ON me.id = m.event_id
       ORDER BY c.id`,
      [limit, leaseMs],
    );
    return rows.map((r) => ({ ...r, event_id: r.event_id ?? '' }));
  }

  /** Deletes confirmed rows and moves their records forward. */
  async markPublished(items: OutboxItem[], broker: string, actor: string) {
    const events = items.filter((i) => i.kind === 'event');
    const attempts = new Map(events.map((i) => [i.ref_id, i.attempt]));
    const messages = items.filter((i) => i.kind === 'message').map((i) => i.ref_id);
    await this.db.transaction(async (c) => {
      await c.query(`DELETE FROM outbox WHERE id = ANY($1)`, [items.map((i) => i.id)]);
      // Status only advances if the record hasn't moved on, but the publish is
      // always logged: a fast consumer may process or deliver it before this
      // transaction runs. The entry is stamped with the claim time (taken
      // before the publish), so the timeline still reads in causal order.
      await c.query(
        `UPDATE events SET status = 'queued', updated_at = now()
         WHERE id = ANY($1::uuid[]) AND status IN ('received', 'retrying')`,
        [events.map((i) => i.ref_id)],
      );
      await c.query(
        `UPDATE messages SET status = 'published', published_at = now()
         WHERE id = ANY($1::uuid[]) AND status = 'pending'`,
        [messages],
      );
      for (const it of items) {
        const [status, detail] =
          it.kind === 'event' ? ['queued', `attempt ${attempts.get(it.ref_id)} published to ${broker}`] : ['published', `published to ${broker}`];
        await c.query(`INSERT INTO status_log (ref_id, status, detail, actor, at) VALUES ($1, $2, $3, $4, $5)`, [
          it.ref_id,
          status,
          detail,
          actor,
          it.claimed_at,
        ]);
      }
    });
  }

  /** Returns rows whose publish failed, backing off up to a minute. */
  async releaseOutbox(ids: number[], cause: Error) {
    await this.db.query(
      `UPDATE outbox SET locked_until = NULL, last_error = $2,
              available_at = now() + least(power(2, publishes), 60) * interval '1 second'
       WHERE id = ANY($1)`,
      [ids, cause.message.slice(0, 500)],
    );
  }

  // ------------------------------------------------------------ processing

  /**
   * Handles one delivery of an inbound event, exactly once per attempt: the
   * event row is locked and the delivery's attempt must be the next one, so
   * redeliveries and stale copies are dropped. Messages get deterministic ids
   * and are written with their outbox rows in the same transaction.
   */
  async processEvent(eventId: string, attempt: number, cfg: ProcessConfig, actor: string): Promise<Outcome> {
    if (!UUID.test(eventId)) return 'skipped';
    let failure: Error | undefined;
    const outcome = await this.db.transaction<Outcome>(async (c) => {
      const { ev, status, skip } = await lockForAttempt(c, eventId, attempt);
      if (skip) {
        if (status) await logStatus(c, eventId, 'duplicate_dropped', skip, actor);
        return 'skipped';
      }
      try {
        await cfg.work?.(ev!);
      } catch (err) {
        failure = err instanceof Error ? err : new Error(String(err));
        return 'retrying'; // decided below, after this transaction commits nothing
      }
      const topics = (
        await c.query<{ topic: string }>(
          `SELECT DISTINCT topic FROM routes
           WHERE source = $1 AND (event_type = '*' OR event_type = $2
                 OR (right(event_type, 2) = '.*' AND starts_with($2, left(event_type, -1))))
           ORDER BY topic`,
          [ev!.source, ev!.type],
        )
      ).rows.map((r) => r.topic);
      let created = 0;
      for (const topic of topics) {
        const id = messageId(ev!.id, topic);
        const payload = { event_id: ev!.id, source: ev!.source, type: ev!.type, data: ev!.payload };
        const ins = await c.query(
          `INSERT INTO messages (id, event_id, topic, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
          [id, ev!.id, topic, JSON.stringify(payload)],
        );
        if (!ins.rowCount) continue; // already created by an earlier attempt or replay
        created++;
        await c.query(`INSERT INTO outbox (kind, ref_id, topic) VALUES ('message', $1, $2)`, [id, topic]);
        await logStatus(c, id, 'pending', `routed to ${topic}`, actor);
      }
      await c.query(
        `UPDATE events SET status = 'processed', attempts = $2, last_error = NULL, processed_at = now(), updated_at = now()
         WHERE id = $1`,
        [eventId, attempt],
      );
      await logStatus(c, eventId, 'processed', `attempt ${attempt}: ${created} message(s) to ${topics.length} route(s)`, actor);
      return 'processed';
    });
    if (failure) return this.failEvent(eventId, attempt, failure, cfg, actor);
    return outcome;
  }

  private async failEvent(eventId: string, attempt: number, cause: Error, cfg: ProcessConfig, actor: string): Promise<Outcome> {
    return this.db.transaction(async (c) => {
      const { skip } = await lockForAttempt(c, eventId, attempt);
      if (skip) return 'skipped'; // another consumer settled this attempt meanwhile
      const msg = cause.message.slice(0, 500);
      const outcome: Outcome = attempt >= cfg.maxAttempts ? 'dead' : 'retrying';
      await c.query(`UPDATE events SET status = $2, attempts = $3, last_error = $4, updated_at = now() WHERE id = $1`, [
        eventId,
        outcome,
        attempt,
        msg,
      ]);
      let detail = `attempt ${attempt}/${cfg.maxAttempts} failed: ${msg}`;
      if (outcome === 'retrying') {
        const delay = cfg.retryDelayMs(attempt);
        await c.query(
          `INSERT INTO outbox (kind, ref_id, topic, attempt, available_at)
           VALUES ('event', $1, $2, $3, now() + $4 * interval '1 millisecond')`,
          [eventId, cfg.eventsTopic, attempt + 1, delay],
        );
        detail += `; retry in ${formatDuration(delay)}`;
      }
      await logStatus(c, eventId, outcome, detail, actor);
      return outcome;
    });
  }

  /** Re-queues a dead event with fresh attempts; produced messages keep their ids, so nothing duplicates. */
  async replay(eventId: string, eventsTopic: string, actor: string) {
    if (!UUID.test(eventId)) throw HookgateError.notFound();
    await this.db.transaction(async (c) => {
      const { rows } = await c.query<{ status: string }>(`SELECT status FROM events WHERE id = $1 FOR UPDATE`, [eventId]);
      if (!rows[0]) throw HookgateError.notFound();
      if (rows[0].status !== 'dead') {
        throw HookgateError.conflict(`only dead events can be replayed (status is ${rows[0].status})`);
      }
      await c.query(`UPDATE events SET status = 'retrying', attempts = 0, updated_at = now() WHERE id = $1`, [eventId]);
      await c.query(`INSERT INTO outbox (kind, ref_id, topic, attempt) VALUES ('event', $1, $2, 1)`, [eventId, eventsTopic]);
      await logStatus(c, eventId, 'retrying', 'replayed manually', actor);
    });
  }

  // ----------------------------------------------------------------- inbox

  /**
   * The reference downstream consumer: records the message id in the inbox in
   * the same transaction as its side effect, so redeliveries are dropped.
   */
  async deliver(consumer: string, msgId: string, brokerId: string, actor: string): Promise<{ duplicate: boolean }> {
    if (!UUID.test(msgId)) throw HookgateError.invalid(`message id "${msgId}" is not a uuid`);
    return this.db.transaction(async (c) => {
      const ins = await c.query(`INSERT INTO inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [consumer, msgId]);
      if (!ins.rowCount) {
        await logStatus(c, msgId, 'duplicate_dropped', `${consumer} already consumed this message (broker id ${brokerId})`, actor);
        return { duplicate: true };
      }
      const upd = await c.query(
        `UPDATE messages SET status = 'delivered', delivered_at = now() WHERE id = $1 AND status <> 'delivered'`,
        [msgId],
      );
      if (upd.rowCount) await logStatus(c, msgId, 'delivered', `consumed by ${consumer}`, actor);
      return { duplicate: false };
    });
  }

  // -------------------------------------------------------------- tracking

  async listEvents(f: { status?: string; source?: string; type?: string; limit?: number }) {
    const limit = f.limit && f.limit > 0 && f.limit <= 200 ? f.limit : 50;
    const { rows } = await this.db.query(
      `SELECT ${EVENT_COLS} FROM events
       WHERE ($1 = '' OR status = $1) AND ($2 = '' OR source = $2) AND ($3 = '' OR type = $3)
       ORDER BY received_at DESC, id LIMIT $4`,
      [f.status ?? '', f.source ?? '', f.type ?? '', limit],
    );
    return rows;
  }

  async getEvent(id: string) {
    if (!UUID.test(id)) throw HookgateError.notFound();
    const { rows } = await this.db.query(`SELECT ${EVENT_COLS}, payload FROM events WHERE id = $1`, [id]);
    if (!rows[0]) throw HookgateError.notFound();
    const messages = (await this.db.query(`SELECT ${MESSAGE_COLS} FROM messages WHERE event_id = $1 ORDER BY topic`, [id])).rows;
    return { ...rows[0], messages, timeline: await this.timeline(id) };
  }

  async getMessage(id: string) {
    if (!UUID.test(id)) throw HookgateError.notFound();
    const { rows } = await this.db.query(`SELECT ${MESSAGE_COLS}, payload FROM messages WHERE id = $1`, [id]);
    if (!rows[0]) throw HookgateError.notFound();
    return { ...rows[0], timeline: await this.timeline(id) };
  }

  private async timeline(refId: string) {
    const { rows } = await this.db.query<{ status: string; detail: string | null; actor: string; at: Date }>(
      `SELECT status, detail, actor, at FROM status_log WHERE ref_id = $1 ORDER BY at, id`,
      [refId],
    );
    return rows.map(({ detail, ...r }) => (detail ? { status: r.status, detail, actor: r.actor, at: r.at } : r));
  }

  /**
   * Deletes status-log entries and inbox rows older than the retention, in
   * small batches. An advisory lock keeps it to one process at a time; events
   * and messages themselves are kept.
   */
  async prune(retentionDays: number): Promise<number> {
    const client = await this.db.pool.connect();
    try {
      const { rows } = await client.query('SELECT pg_try_advisory_lock($1) AS locked', [727_300_002]);
      if (!rows[0].locked) return 0;
      let total = 0;
      try {
        for (const q of [
          `DELETE FROM status_log WHERE id IN (SELECT id FROM status_log WHERE at < now() - $1 * interval '1 day' LIMIT 5000)`,
          `DELETE FROM inbox WHERE (consumer, message_id) IN (
             SELECT consumer, message_id FROM inbox WHERE processed_at < now() - $1 * interval '1 day' LIMIT 5000)`,
        ]) {
          for (;;) {
            const n = (await client.query(q, [retentionDays])).rowCount ?? 0;
            total += n;
            if (n < 5000) break;
          }
        }
      } finally {
        await client.query('SELECT pg_advisory_unlock($1)', [727_300_002]);
      }
      return total;
    } finally {
      client.release();
    }
  }

  async stats() {
    // Alphabetical, like Go's JSON encoding of maps, so both services return identical bodies.
    const events: Record<string, number> = { dead: 0, processed: 0, queued: 0, received: 0, retrying: 0 };
    const messages: Record<string, number> = { delivered: 0, pending: 0, published: 0 };
    for (const r of (await this.db.query(`SELECT status, count(*) AS n FROM events GROUP BY status`)).rows) events[r.status] = r.n;
    for (const r of (await this.db.query(`SELECT status, count(*) AS n FROM messages GROUP BY status`)).rows) messages[r.status] = r.n;
    const { rows } = await this.db.query(
      `SELECT (SELECT coalesce(sum(duplicates), 0) FROM events)::int8 AS ingest_duplicates,
              (SELECT count(*) FROM status_log WHERE status = 'duplicate_dropped' AND actor NOT LIKE '%/api') AS dropped,
              count(*) AS pending, count(*) FILTER (WHERE available_at <= now()) AS ready,
              coalesce(extract(epoch FROM now() - min(available_at) FILTER (WHERE available_at <= now())) * 1000, 0)::float8 AS age
       FROM outbox`,
    );
    const r = rows[0];
    return {
      events,
      messages,
      ingest_duplicates: r.ingest_duplicates,
      dropped_redeliveries: r.dropped,
      outbox: { pending: r.pending, ready: r.ready, oldest_ready_age_ms: r.age },
    };
  }
}

async function lockForAttempt(c: pg.PoolClient, eventId: string, attempt: number) {
  const { rows } = await c.query<EventRecord & { status: string }>(
    `SELECT id::text, source, type, payload, attempts, status FROM events WHERE id = $1 FOR UPDATE`,
    [eventId],
  );
  const row = rows[0];
  if (!row) return { status: '', skip: 'event no longer exists' };
  let skip = '';
  if (row.status === 'processed' || row.status === 'dead') {
    skip = `redelivery of attempt ${attempt} dropped: event already ${row.status}`;
  } else if (attempt !== row.attempts + 1) {
    skip = `stale delivery of attempt ${attempt} dropped: ${row.attempts} attempt(s) already ran`;
  }
  const { status, ...ev } = row;
  return { ev: ev as EventRecord, status, skip };
}

async function logStatus(c: pg.PoolClient, refId: string, status: string, detail: string, actor: string) {
  await c.query(`INSERT INTO status_log (ref_id, status, detail, actor) VALUES ($1, $2, nullif($3, ''), $4)`, [
    refId,
    status,
    detail,
    actor,
  ]);
}

function pgCode(err: unknown) {
  return (err as { code?: string })?.code;
}

/** Formats like Go's time.Duration.Round(time.Millisecond).String(), e.g. "1.234s". */
export function formatDuration(ms: number) {
  ms = Math.round(ms);
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${ms / 1000}s`;
  const m = Math.floor(ms / 60_000);
  return `${m}m${(ms % 60_000) / 1000}s`;
}
