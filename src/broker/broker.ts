/**
 * hookgate's transport: a small interface over RabbitMQ, Amazon SQS and
 * Google Pub/Sub (plus an in-memory broker for tests). It mirrors the Go
 * package: the broker only moves bytes, retries are scheduled through the
 * database outbox and dedup happens in consumers, so every broker gets the
 * same at-least-once semantics.
 */
export interface BrokerMessage {
  /** Stable across re-publishes of the same outbox row ("ob-<outbox id>"). */
  id: string;
  topic: string;
  body: Buffer;
}

/** Resolving acks the message; throwing hands it back for redelivery. */
export type Handler = (m: BrokerMessage) => Promise<void>;

export interface Broker {
  readonly name: 'rabbitmq' | 'sqs' | 'pubsub' | 'memory';
  /** Resolves only after the broker durably accepted every message. */
  publish(msgs: BrokerMessage[]): Promise<void>;
  /** Delivers messages until `signal` aborts (resolves) or the connection fails (rejects). */
  consume(topic: string, concurrency: number, handler: Handler, signal: AbortSignal): Promise<void>;
  close(): Promise<void>;
}

export const BROKER = Symbol('BROKER');

/** The JSON body of every message; identical to the Go implementation's. */
export interface Envelope {
  id: string;
  kind: 'event' | 'message';
  ref_id: string;
  attempt: number;
  source: string;
  type: string;
  topic: string;
  event_id: string;
  payload: unknown;
  producer: 'go' | 'nest';
}

export function decodeEnvelope(body: Buffer): Envelope {
  const e = JSON.parse(body.toString('utf8')) as Envelope;
  if (!e || typeof e !== 'object' || !e.id || !e.ref_id || (e.kind !== 'event' && e.kind !== 'message')) {
    throw new Error('decode envelope: missing id, ref_id or kind');
  }
  return e;
}

/** Message attribute carrying the hookgate id on SQS and Pub/Sub. */
export const ID_ATTR = 'hookgate-id';

/** Connects to the broker named by BROKER (default rabbitmq). */
export async function openBroker(env: NodeJS.ProcessEnv = process.env): Promise<Broker> {
  const kind = (env.BROKER ?? 'rabbitmq').toLowerCase();
  switch (kind) {
    case 'rabbitmq':
    case 'amqp': {
      const { RabbitMQBroker } = await import('./rabbitmq.broker.js');
      return RabbitMQBroker.connect(env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672/');
    }
    case 'sqs': {
      const { SQSBroker } = await import('./sqs.broker.js');
      return new SQSBroker({ region: env.AWS_REGION ?? 'us-east-1', endpoint: env.SQS_ENDPOINT || undefined });
    }
    case 'pubsub': {
      const { PubSubBroker } = await import('./pubsub.broker.js');
      return new PubSubBroker(env.PUBSUB_PROJECT ?? 'hookgate-local');
    }
    case 'memory': {
      const { MemoryBroker } = await import('./memory.broker.js');
      return new MemoryBroker();
    }
    default:
      throw new Error(`unknown BROKER "${kind}" (want rabbitmq, sqs, pubsub or memory)`);
  }
}

/** Runs fn over items with at most `limit` in flight. */
export class Semaphore {
  private active = 0;
  private readonly waiters: (() => void)[] = [];
  constructor(private readonly limit: number) {}

  async acquire() {
    if (this.active < this.limit) {
      this.active++;
      return;
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  release() {
    const next = this.waiters.shift();
    if (next) next();
    else this.active--;
  }

  /** Resolves once nothing is in flight. */
  async drain() {
    while (this.active > 0) await new Promise((r) => setTimeout(r, 10));
  }
}
