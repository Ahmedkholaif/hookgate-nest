import { Broker, BrokerMessage, Handler, Semaphore } from './broker.js';

class Queue {
  private readonly items: BrokerMessage[] = [];
  private readonly waiters: ((m: BrokerMessage) => void)[] = [];

  push(m: BrokerMessage) {
    const w = this.waiters.shift();
    if (w) w(m);
    else this.items.push(m);
  }

  pop(signal: AbortSignal): Promise<BrokerMessage | undefined> {
    const m = this.items.shift();
    if (m) return Promise.resolve(m);
    if (signal.aborted) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      const waiter = (msg: BrokerMessage) => {
        signal.removeEventListener('abort', onAbort);
        resolve(msg);
      };
      const onAbort = () => {
        const i = this.waiters.indexOf(waiter);
        if (i >= 0) this.waiters.splice(i, 1);
        resolve(undefined);
      };
      this.waiters.push(waiter);
      signal.addEventListener('abort', onAbort, { once: true });
    });
  }
}

/**
 * In-process broker for tests. With `duplicate` set it delivers every message
 * twice, which lets tests prove that consumers are idempotent.
 */
export class MemoryBroker implements Broker {
  readonly name = 'memory' as const;
  private readonly queues = new Map<string, Queue>();

  constructor(public duplicate = false) {}

  private queue(topic: string) {
    let q = this.queues.get(topic);
    if (!q) this.queues.set(topic, (q = new Queue()));
    return q;
  }

  async publish(msgs: BrokerMessage[]) {
    for (const m of msgs) {
      this.queue(m.topic).push(m);
      if (this.duplicate) this.queue(m.topic).push(m);
    }
  }

  async consume(topic: string, concurrency: number, handler: Handler, signal: AbortSignal) {
    const q = this.queue(topic);
    const sem = new Semaphore(Math.max(concurrency, 1));
    while (!signal.aborted) {
      await sem.acquire();
      const m = await q.pop(signal);
      if (!m) {
        sem.release();
        break;
      }
      handler(m)
        .catch(() => q.push(m)) // nack: redeliver
        .finally(() => sem.release());
    }
    await sem.drain();
  }

  async close() {}
}
