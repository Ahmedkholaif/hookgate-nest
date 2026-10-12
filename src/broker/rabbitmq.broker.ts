import amqp from 'amqplib';
import { Broker, BrokerMessage, Handler } from './broker.js';

const QUEUE_ARGS = { durable: true, arguments: { 'x-queue-type': 'quorum' } };

/**
 * Durable quorum queues (one per topic, via the default exchange), publisher
 * confirms, manual acks. The connection is re-dialled lazily if it drops.
 */
export class RabbitMQBroker implements Broker {
  readonly name = 'rabbitmq' as const;
  private conn?: amqp.ChannelModel;
  private pubCh?: amqp.ConfirmChannel;
  private declared = new Set<string>();
  private publishing: Promise<void> = Promise.resolve();

  private constructor(private readonly url: string) {}

  static async connect(url: string) {
    const b = new RabbitMQBroker(url);
    await b.connection();
    return b;
  }

  private async connection() {
    if (!this.conn) {
      const conn = await amqp.connect(this.url, { clientProperties: { connection_name: 'hookgate-nest' } });
      conn.on('error', () => undefined); // surfaced through 'close'
      conn.on('close', () => {
        if (this.conn === conn) {
          this.conn = undefined;
          this.pubCh = undefined;
          this.declared.clear();
        }
      });
      this.conn = conn;
    }
    return this.conn;
  }

  publish(msgs: BrokerMessage[]): Promise<void> {
    // One confirm channel, one batch at a time (like the Go mutex).
    const run = this.publishing.then(() => this.publishBatch(msgs));
    this.publishing = run.catch(() => undefined);
    return run;
  }

  private async publishBatch(msgs: BrokerMessage[]) {
    if (msgs.length === 0) return;
    try {
      const conn = await this.connection();
      if (!this.pubCh) {
        const ch = await conn.createConfirmChannel();
        ch.on('error', () => undefined);
        ch.on('close', () => {
          if (this.pubCh === ch) this.pubCh = undefined;
        });
        this.pubCh = ch;
      }
      const ch = this.pubCh;
      for (const m of msgs) {
        if (!this.declared.has(m.topic)) {
          await ch.assertQueue(m.topic, QUEUE_ARGS);
          this.declared.add(m.topic);
        }
        ch.sendToQueue(m.topic, m.body, {
          messageId: m.id,
          contentType: 'application/json',
          persistent: true,
          timestamp: Math.floor(Date.now() / 1000),
        });
      }
      // Publishes are pipelined; this rejects if the broker nacks any of them.
      await ch.waitForConfirms();
    } catch (err) {
      await this.pubCh?.close().catch(() => undefined);
      this.pubCh = undefined;
      throw err;
    }
  }

  async consume(topic: string, concurrency: number, handler: Handler, signal: AbortSignal) {
    const conn = await this.connection();
    const ch = await conn.createChannel();
    await ch.assertQueue(topic, QUEUE_ARGS);
    // prefetch caps unacked deliveries, which caps concurrent handlers.
    await ch.prefetch(Math.max(concurrency, 1));
    let inFlight = 0;

    await new Promise<void>((resolve, reject) => {
      const onClose = () => (signal.aborted ? resolve() : reject(new Error('rabbitmq: consumer channel closed')));
      ch.on('close', onClose);
      ch.on('error', () => undefined);
      ch.consume(
        topic,
        (d) => {
          if (!d) return; // consumer cancelled by the broker
          inFlight++;
          handler({ id: String(d.properties.messageId ?? ''), topic, body: d.content })
            .then(
              () => ch.ack(d),
              () => ch.nack(d, false, true),
            )
            .catch(() => undefined) // channel already closed; the broker redelivers
            .finally(() => inFlight--);
        },
        { noAck: false },
      ).then(({ consumerTag }) => {
        const stop = async () => {
          await ch.cancel(consumerTag).catch(() => undefined);
          while (inFlight > 0) await new Promise((r) => setTimeout(r, 10));
          await ch.close().catch(() => undefined);
        };
        if (signal.aborted) void stop();
        else signal.addEventListener('abort', () => void stop(), { once: true });
      }, reject);
    });
  }

  async close() {
    await this.conn?.close().catch(() => undefined);
  }
}
