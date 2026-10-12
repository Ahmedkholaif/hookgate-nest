import 'reflect-metadata';
import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, it } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureApp } from './app.module.js';
import { MemoryBroker } from './broker/memory.broker.js';
import { sign } from './common/signature.js';
import { AppConfig, loadConfig } from './config.js';
import { DatabaseService } from './database/database.service.js';
import { messageId, StoreService } from './store/store.service.js';
import { dedupKey, eventType } from './api/ingest.controller.js';

const DB = process.env.TEST_DATABASE_URL;

describe('pure helpers', () => {
  it('derives the same message id as the Go service', () => {
    assert.equal(messageId('0b0f3f3e-6c43-4d4c-9a51-1f0d5b5c2a10', 'billing'), '4236585a-caa3-50c9-b45b-78fc4e7a4727');
  });

  it('dedup key: provider id, else body hash', () => {
    assert.equal(dedupKey(' evt_1 ', Buffer.from('x')), 'id:evt_1');
    assert.equal(dedupKey('', Buffer.from('{"a":1}')), dedupKey(undefined, Buffer.from('{"a":1}')));
    assert.match(dedupKey('x'.repeat(300), Buffer.from('x')), /^sha256:/);
  });

  it('event type: dotted paths, string values only', () => {
    const p = { type: 'order.created', data: { object: { type: 'invoice.paid' } }, n: 3, empty: '' };
    assert.equal(eventType(p, 'type'), 'order.created');
    assert.equal(eventType(p, 'data.object.type'), 'invoice.paid');
    for (const path of ['missing', 'n', 'empty', 'type.deeper']) assert.equal(eventType(p, path), 'unknown');
  });
});

describe('hookgate-nest against PostgreSQL', { skip: !DB && 'TEST_DATABASE_URL not set' }, () => {
  let app: INestApplication;
  let url: string;
  let db: DatabaseService;
  let store: StoreService;
  const broker = new MemoryBroker(true); // every message delivered twice

  /** A second, worker-only app instance: three of these share the outbox and the queues. */
  const workerApps: INestApplication[] = [];

  async function startApp(cfg: AppConfig, b: MemoryBroker | null) {
    const a = configureApp(await NestFactory.create(AppModule.forRoot(cfg, b), { bodyParser: false, logger: false }));
    await a.listen(0);
    return a;
  }

  function config(overrides: Partial<AppConfig> & { roles?: AppConfig['roles'] } = {}): AppConfig {
    const base = loadConfig({ DATABASE_URL: DB });
    return {
      ...base,
      relay: { batchSize: 50, pollMs: 10, leaseMs: 5000 },
      processor: { ...base.processor, concurrency: 4, maxAttempts: 3, retryBaseMs: 5, retryMaxMs: 20 },
      sink: { topics: ['billing', 'crm', 'audit', 'ledger'], concurrency: 4 },
      ...overrides,
    };
  }

  before(async () => {
    app = await startApp(config({ roles: ['api'] }), null);
    url = await app.getUrl();
    db = app.get(DatabaseService);
    store = app.get(StoreService);
  });

  after(async () => {
    await Promise.all(workerApps.map((a) => a.close()));
    await app.close();
  });

  beforeEach(async () => {
    await Promise.all(workerApps.splice(0).map((a) => a.close()));
    await db.query('TRUNCATE sources, routes, events, messages, outbox, inbox, status_log CASCADE');
  });

  async function req(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
    const res = await fetch(url + path, {
      method,
      headers: { 'content-type': 'application/json', ...headers },
      body: body === undefined ? undefined : Buffer.isBuffer(body) ? new Uint8Array(body) : JSON.stringify(body),
    });
    const text = await res.text();
    return { status: res.status, body: text ? JSON.parse(text) : undefined };
  }

  async function source(name: string): Promise<string> {
    const r = await req('POST', '/v1/sources', { name });
    assert.equal(r.status, 201, JSON.stringify(r.body));
    return r.body.secret;
  }

  async function route(source: string, event_type: string, topic: string) {
    const r = await req('POST', '/v1/routes', { source, event_type, topic });
    assert.equal(r.status, 201, JSON.stringify(r.body));
  }

  function send(src: string, secret: string, id: string, payload: unknown) {
    const body = Buffer.from(JSON.stringify(payload));
    const h: Record<string, string> = { 'Hookgate-Signature': sign(body, new Date(), secret) };
    if (id) h['Webhook-Id'] = id;
    return req('POST', `/v1/ingest/${src}`, body, h);
  }

  async function workers(n: number, processor: Partial<AppConfig['processor']> = {}, b = broker) {
    for (let i = 0; i < n; i++) {
      const cfg = config({ roles: ['relay', 'processor', 'sink'] });
      cfg.processor = { ...cfg.processor, ...processor };
      workerApps.push(await startApp(cfg, b));
    }
  }

  async function waitFor(what: string, cond: (s: Awaited<ReturnType<StoreService['stats']>>) => boolean) {
    const deadline = Date.now() + 20_000;
    for (;;) {
      const s = await store.stats();
      if (cond(s)) return s;
      if (Date.now() > deadline) assert.fail(`timed out waiting for ${what}: ${JSON.stringify(s)}`);
      await sleep(20);
    }
  }

  const count = async (sql: string) => (await db.query(sql)).rows[0].count as number;

  it('rejects bad webhooks and stores nothing', async () => {
    const secret = await source('shop');
    assert.equal((await send('nope', secret, 'x', { type: 'a' })).status, 404);
    const bad = await send('shop', 'wrong-secret', 'x', { type: 'a' });
    assert.equal(bad.status, 401);
    assert.match(bad.body.error, /invalid signature/);
    const stale = Buffer.from('{"type":"a"}');
    const old = await req('POST', '/v1/ingest/shop', stale, { 'Hookgate-Signature': sign(stale, new Date(Date.now() - 3600_000), secret) });
    assert.equal(old.status, 401);
    const arr = Buffer.from('[1,2]');
    assert.equal((await req('POST', '/v1/ingest/shop', arr, { 'Hookgate-Signature': sign(arr, new Date(), secret) })).status, 400);
    // The raw parser accepts any content type, so the exact signed bytes are verified.
    const text = Buffer.from('{"type":"plain"}');
    const plain = await fetch(url + '/v1/ingest/shop', {
      method: 'POST',
      headers: { 'content-type': 'text/plain', 'Hookgate-Signature': sign(text, new Date(), secret) },
      body: new Uint8Array(text),
    });
    assert.equal(plain.status, 202);
    assert.equal(await count('SELECT count(*) FROM events'), 1);
  });

  it('dedupes by provider id, then by body hash', async () => {
    const secret = await source('shop');
    const first = await send('shop', secret, 'evt_1', { type: 'order.created', n: 1 });
    assert.equal(first.status, 202);
    assert.equal(first.body.duplicate, false);
    const again = await send('shop', secret, 'evt_1', { type: 'order.created', n: 1, retry: true });
    assert.equal(again.status, 200);
    assert.deepEqual([again.body.duplicate, again.body.id], [true, first.body.id]);
    assert.equal((await send('shop', secret, '', { type: 'ping' })).status, 202);
    const dup = await send('shop', secret, '', { type: 'ping' });
    assert.equal(dup.status, 200);
    assert.equal(await count('SELECT count(*) FROM events'), 2);
    assert.equal(await count('SELECT count(*) FROM outbox'), 2);
  });

  it('applies every outbound message exactly once with duplicate deliveries and 3 worker instances', async () => {
    const secret = await source('shop');
    await route('shop', 'order.*', 'billing');
    await route('shop', 'order.created', 'crm');
    await route('shop', '*', 'audit');
    const n = 60;
    for (let i = 0; i < n; i++) {
      const r = await send('shop', secret, `evt_${i}`, { type: i % 3 === 0 ? 'customer.updated' : 'order.created', i });
      assert.equal(r.status, 202);
    }
    const want = 20 * 1 + 40 * 3;
    await workers(3);
    const s = await waitFor('all delivered', (s) => s.events.processed === n && s.messages.delivered === want && s.outbox.pending === 0);
    assert.equal(await count('SELECT count(*) FROM messages'), want);
    assert.equal(await count('SELECT count(*) FROM inbox'), want);
    assert.equal(await count('SELECT count(*) FROM events WHERE attempts <> 1'), 0);
    assert.ok(s.dropped_redeliveries > 0, 'duplicate deliveries should be dropped and counted');

    const id = (await db.query(`SELECT id::text FROM events WHERE dedup_key = 'id:evt_1'`)).rows[0].id;
    const ev = (await req('GET', `/v1/events/${id}`)).body;
    assert.equal(ev.status, 'processed');
    assert.equal(ev.messages.length, 3);
    assert.equal(ev.timeline[0].status, 'received');
    assert.equal(ev.timeline[0].actor, 'nest/api');
    const order = ev.timeline.map((t: { status: string }) => t.status);
    assert.ok(order.indexOf('queued') > 0 && order.indexOf('queued') < order.indexOf('processed'), `timeline order: ${order}`);
    const msg = (await req('GET', `/v1/messages/${ev.messages[0].id}`)).body;
    assert.equal(msg.status, 'delivered');
    assert.equal(msg.id, messageId(id, msg.topic));
  });

  it('retries with backoff, dead-letters, and replays', async () => {
    const secret = await source('pay');
    await route('pay', '*', 'ledger');
    const id = (await send('pay', secret, 'evt_flaky', { type: 'payment.flaky' })).body.id;
    const b = new MemoryBroker();
    await workers(1, { chaosFailTypes: ['payment.flaky'] }, b);
    await waitFor('dead', (s) => s.events.dead === 1);
    await Promise.all(workerApps.splice(0).map((a) => a.close()));

    const ev = (await req('GET', `/v1/events/${id}`)).body;
    assert.equal(ev.attempts, 3);
    assert.ok(ev.last_error);
    assert.equal(ev.timeline.filter((t: { status: string }) => t.status === 'retrying').length, 2);
    assert.equal((await req('POST', `/v1/events/${id}/replay`)).status, 202);
    assert.equal((await req('POST', `/v1/events/${id}/replay`)).status, 409);

    await workers(1, { chaosFailTypes: [] }, b);
    await waitFor('replayed and delivered', (s) => s.events.processed === 1 && s.messages.delivered === 1);
  });

  it('prunes old timeline and inbox rows, keeping recent ones', async () => {
    await db.query(`INSERT INTO status_log (ref_id, status, actor, at) VALUES (gen_random_uuid(), 'old', 'nest/test', now() - interval '20 days'), (gen_random_uuid(), 'new', 'nest/test', now())`);
    await db.query(`INSERT INTO inbox (consumer, message_id, processed_at) VALUES ('c', 'old', now() - interval '20 days'), ('c', 'new', now())`);
    assert.equal(await store.prune(14), 2);
    assert.equal(await count(`SELECT count(*) FROM status_log WHERE status = 'new'`), 1);
    assert.equal(await count(`SELECT count(*) FROM inbox WHERE message_id = 'new'`), 1);
  });

  it('validates admin input like the Go service', async () => {
    assert.equal((await req('POST', '/v1/sources', { name: 'Bad Name' })).status, 400);
    assert.equal((await req('POST', '/v1/sources', { name: 'ok', bogus: 1 })).status, 400);
    await source('ok');
    assert.equal((await req('POST', '/v1/sources', { name: 'ok' })).status, 409);
    assert.equal((await req('POST', '/v1/routes', { source: 'ok', event_type: '*', topic: 'has.dot' })).status, 400);
    assert.equal((await req('POST', '/v1/routes', { source: 'missing', event_type: '*', topic: 't' })).status, 404);
    assert.equal((await req('GET', '/v1/events/not-a-uuid')).status, 404);
  });
});
