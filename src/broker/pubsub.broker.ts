import { PubSub, Topic } from '@google-cloud/pubsub';
import { Broker, BrokerMessage, Handler, ID_ATTR } from './broker.js';

const ALREADY_EXISTS = 6; // gRPC status code

/**
 * One Pub/Sub topic per hookgate topic plus one shared subscription
 * ("<topic>-hookgate") that all consumers of that topic compete on. Both are
 * created on first use. PUBSUB_EMULATOR_HOST selects the local emulator.
 */
export class PubSubBroker implements Broker {
  readonly name = 'pubsub' as const;
  private readonly client: PubSub;
  private readonly topics = new Map<string, Promise<Topic>>();

  constructor(projectId: string) {
    this.client = new PubSub({ projectId });
  }

  /**
   * Creates the topic and its subscription if missing. Creating the
   * subscription up front means messages published before any consumer
   * starts are retained rather than dropped.
   */
  private topic(name: string) {
    let t = this.topics.get(name);
    if (!t) {
      t = (async () => {
        const topic = this.client.topic(name, { batching: { maxMessages: 100, maxMilliseconds: 5 } });
        await topic.create().catch(ignoreExists);
        await topic.createSubscription(`${name}-hookgate`, { ackDeadlineSeconds: 60 }).catch(ignoreExists);
        return topic;
      })();
      t.catch(() => this.topics.delete(name));
      this.topics.set(name, t);
    }
    return t;
  }

  async publish(msgs: BrokerMessage[]) {
    // The client batches these into a few RPCs; each promise resolves on the server ack.
    const pending: Promise<string>[] = [];
    for (const m of msgs) {
      const topic = await this.topic(m.topic);
      pending.push(topic.publishMessage({ data: m.body, attributes: { [ID_ATTR]: m.id } }));
    }
    await Promise.all(pending);
  }

  async consume(topic: string, concurrency: number, handler: Handler, signal: AbortSignal) {
    await this.topic(topic);
    const sub = this.client.subscription(`${topic}-hookgate`, {
      flowControl: { maxMessages: Math.max(concurrency, 1), allowExcessMessages: false },
    });
    await new Promise<void>((resolve, reject) => {
      sub.on('message', (m) => {
        const id = m.attributes?.[ID_ATTR] || m.id;
        handler({ id, topic, body: m.data }).then(
          () => m.ack(),
          () => m.nack(),
        );
      });
      sub.on('error', (err) => {
        sub.close().catch(() => undefined);
        reject(err);
      });
      const stop = () => sub.close().then(resolve, resolve);
      if (signal.aborted) void stop();
      else signal.addEventListener('abort', () => void stop(), { once: true });
    });
  }

  async close() {
    await this.client.close();
  }
}

function ignoreExists(err: { code?: number }) {
  if (err?.code !== ALREADY_EXISTS) throw err;
}
