import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';
import { BROKER, Broker, BrokerMessage, decodeEnvelope, Envelope, Handler } from '../broker/broker.js';
import { HookgateError } from '../common/errors.js';
import { APP_CONFIG, type AppConfig } from '../config.js';
import { EventRecord, StoreService } from '../store/store.service.js';

const actor = (role: string) => `nest/${role}`;

/** Random delay in [0, min(max, base*2^(attempt-1))]: full jitter, as in Go. */
export function fullJitter(attempt: number, baseMs: number, maxMs: number) {
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(attempt - 1, 0));
  return Math.floor(Math.random() * (ceiling + 1));
}

/** Moves outbox rows to the broker: claim (SKIP LOCKED), publish, delete on confirm. */
@Injectable()
export class RelayWorker {
  private readonly log = new Logger('relay');
  constructor(
    private readonly store: StoreService,
    @Inject(BROKER) private readonly broker: Broker,
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
  ) {}

  async run(signal: AbortSignal) {
    while (!signal.aborted) {
      let n = 0;
      try {
        n = await this.tick();
      } catch (err) {
        if (!signal.aborted) this.log.error(`relay tick: ${(err as Error).message}`);
      }
      if (n < this.cfg.relay.batchSize) await sleep(this.cfg.relay.pollMs, undefined, { signal }).catch(() => undefined);
    }
  }

  /** Publishes one batch; returns how many rows were claimed. */
  async tick(): Promise<number> {
    const items = await this.store.claimOutbox(this.cfg.relay.batchSize, this.cfg.relay.leaseMs);
    if (items.length === 0) return 0;
    const msgs: BrokerMessage[] = items.map((it) => {
      const env: Envelope = {
        id: `ob-${it.id}`,
        kind: it.kind,
        ref_id: it.ref_id,
        attempt: it.attempt,
        source: it.source,
        type: it.type,
        topic: it.topic,
        event_id: it.event_id,
        payload: it.payload,
        producer: 'nest',
      };
      return { id: env.id, topic: it.topic, body: Buffer.from(JSON.stringify(env)) };
    });
    try {
      await this.broker.publish(msgs);
    } catch (err) {
      await this.store.releaseOutbox(
        items.map((i) => i.id),
        err as Error,
      );
      throw err;
    }
    await this.store.markPublished(items, this.broker.name, actor('relay'));
    return items.length;
  }
}

/**
 * Consumes inbound events. Processing failures are scheduled for retry through
 * the outbox and the delivery is acked; only infrastructure errors (database
 * unreachable) are thrown back to the broker.
 */
@Injectable()
export class ProcessorWorker {
  readonly log = new Logger('processor');
  constructor(
    private readonly store: StoreService,
    @Inject(BROKER) private readonly broker: Broker,
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
  ) {}

  run(signal: AbortSignal) {
    return consumeForever(this.broker, this.cfg.eventsTopic, this.cfg.processor.concurrency, (m) => this.handle(m), signal, this.log);
  }

  async handle(m: BrokerMessage) {
    let env: Envelope;
    try {
      env = decodeEnvelope(m.body);
    } catch (err) {
      this.log.warn(`dropping malformed message ${m.id}: ${(err as Error).message}`);
      return;
    }
    if (env.kind !== 'event') return;
    const p = this.cfg.processor;
    await this.store.processEvent(
      env.ref_id,
      env.attempt,
      {
        eventsTopic: this.cfg.eventsTopic,
        maxAttempts: p.maxAttempts,
        retryDelayMs: (attempt) => Math.max(fullJitter(attempt, p.retryBaseMs, p.retryMaxMs), 100),
        work: async (ev: EventRecord) => {
          if (p.chaosFailTypes.includes(ev.type)) throw new Error(`chaos: ${ev.type} is configured to fail`);
        },
      },
      actor('processor'),
    );
  }
}

/** Reference downstream consumer: the inbox pattern applies each message once. */
@Injectable()
export class SinkWorker {
  private readonly log = new Logger('sink');
  constructor(
    private readonly store: StoreService,
    @Inject(BROKER) private readonly broker: Broker,
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
  ) {}

  run(signal: AbortSignal) {
    return Promise.all(
      this.cfg.sink.topics.map((t) =>
        consumeForever(this.broker, t, this.cfg.sink.concurrency, (m) => this.handle(t, m), signal, this.log),
      ),
    );
  }

  async handle(topic: string, m: BrokerMessage) {
    let env: Envelope;
    try {
      env = decodeEnvelope(m.body);
    } catch (err) {
      this.log.warn(`dropping malformed message ${m.id}: ${(err as Error).message}`);
      return;
    }
    if (env.kind !== 'message') return;
    try {
      await this.store.deliver(`sink:${topic}`, env.ref_id, env.id, actor('sink'));
    } catch (err) {
      if (err instanceof HookgateError && err.code === 'invalid') return;
      throw err;
    }
  }
}

/**
 * Keeps a consumer attached, reconnecting with backoff when the broker
 * connection drops. Handler errors are delayed briefly before the nack so a
 * database outage doesn't become a hot redelivery loop.
 */
async function consumeForever(broker: Broker, topic: string, concurrency: number, h: Handler, signal: AbortSignal, log: Logger) {
  const wrapped: Handler = async (m) => {
    try {
      await h(m);
    } catch (err) {
      log.error(`handler failed; message ${m.id} returned to broker: ${(err as Error).message}`);
      await sleep(1000, undefined, { signal }).catch(() => undefined);
      throw err;
    }
  };
  for (let attempt = 1; !signal.aborted; attempt++) {
    const start = Date.now();
    try {
      await broker.consume(topic, concurrency, wrapped, signal);
    } catch (err) {
      if (signal.aborted) return;
      if (Date.now() - start > 60_000) attempt = 1;
      const d = fullJitter(attempt, 500, 30_000);
      log.warn(`consumer on ${topic} stopped (${(err as Error).message}); reconnecting in ${d}ms`);
      await sleep(d, undefined, { signal }).catch(() => undefined);
    }
  }
}

/** Starts the configured background roles and stops them on shutdown. */
@Injectable()
export class WorkerRunner implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly log = new Logger('hookgate');
  private readonly abort = new AbortController();
  private running: Promise<unknown>[] = [];

  constructor(
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
    @Inject(BROKER) private readonly broker: Broker,
    private readonly relay: RelayWorker,
    private readonly processor: ProcessorWorker,
    private readonly sink: SinkWorker,
  ) {}

  onApplicationBootstrap() {
    const { roles } = this.cfg;
    const signal = this.abort.signal;
    if (roles.includes('relay')) this.running.push(this.relay.run(signal));
    if (roles.includes('processor')) this.running.push(this.processor.run(signal));
    if (roles.includes('sink')) {
      if (this.cfg.sink.topics.length) this.running.push(this.sink.run(signal));
      else this.log.log('sink: SINK_TOPICS is empty; nothing to consume');
    }
    this.log.log(`hookgate-nest started: roles=${roles.join(',')} broker=${this.broker.name} events_topic=${this.cfg.eventsTopic}`);
  }

  async beforeApplicationShutdown() {
    this.log.log('shutting down: draining in-flight work');
    this.abort.abort();
    await Promise.allSettled(this.running);
    await this.broker.close();
  }
}
