import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Broker, BrokerMessage, Handler, ID_ATTR, Semaphore } from './broker.js';

/**
 * One standard queue per topic, created on first use. Publishes with
 * SendMessageBatch (10 per call) and consumes with long polling.
 */
export class SQSBroker implements Broker {
  readonly name = 'sqs' as const;
  private readonly client: SQSClient;
  private readonly urls = new Map<string, Promise<string>>();

  constructor(opts: { region: string; endpoint?: string }) {
    this.client = new SQSClient({
      region: opts.region,
      endpoint: opts.endpoint,
      // Local emulators accept any credentials; don't require real ones.
      credentials: opts.endpoint ? { accessKeyId: 'local', secretAccessKey: 'local' } : undefined,
    });
  }

  private queueUrl(topic: string) {
    let url = this.urls.get(topic);
    if (!url) {
      // CreateQueue is idempotent for identical attributes and returns the URL.
      url = this.client
        .send(new CreateQueueCommand({ QueueName: topic, Attributes: { VisibilityTimeout: '60' } }))
        .then((out) => out.QueueUrl!);
      url.catch(() => this.urls.delete(topic));
      this.urls.set(topic, url);
    }
    return url;
  }

  async publish(msgs: BrokerMessage[]) {
    const byTopic = new Map<string, BrokerMessage[]>();
    for (const m of msgs) byTopic.set(m.topic, [...(byTopic.get(m.topic) ?? []), m]);
    for (const [topic, ms] of byTopic) {
      const QueueUrl = await this.queueUrl(topic);
      for (let i = 0; i < ms.length; i += 10) {
        const chunk = ms.slice(i, i + 10);
        const out = await this.client.send(
          new SendMessageBatchCommand({
            QueueUrl,
            Entries: chunk.map((m, j) => ({
              Id: String(j),
              MessageBody: m.body.toString('utf8'),
              MessageAttributes: { [ID_ATTR]: { DataType: 'String', StringValue: m.id } },
            })),
          }),
        );
        if (out.Failed?.length) {
          const f = out.Failed[0];
          throw new Error(`sqs: ${out.Failed.length} of ${chunk.length} messages rejected (${f.Code}: ${f.Message})`);
        }
      }
    }
  }

  async consume(topic: string, concurrency: number, handler: Handler, signal: AbortSignal) {
    const QueueUrl = await this.queueUrl(topic);
    const sem = new Semaphore(Math.max(concurrency, 1));
    while (!signal.aborted) {
      let out;
      try {
        out = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl,
            MaxNumberOfMessages: Math.min(concurrency, 10),
            WaitTimeSeconds: 10,
            MessageAttributeNames: [ID_ATTR],
          }),
          { abortSignal: signal },
        );
      } catch (err) {
        if (signal.aborted) break;
        throw err;
      }
      for (const m of out.Messages ?? []) {
        await sem.acquire();
        const id = m.MessageAttributes?.[ID_ATTR]?.StringValue ?? m.MessageId ?? '';
        handler({ id, topic, body: Buffer.from(m.Body ?? '', 'utf8') })
          .then(
            () => this.client.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: m.ReceiptHandle })),
            // Make it visible again soon instead of after the full timeout.
            () => this.client.send(new ChangeMessageVisibilityCommand({ QueueUrl, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 2 })),
          )
          .catch(() => undefined) // settle failed: the visibility timeout redelivers it
          .finally(() => sem.release());
      }
    }
    await sem.drain();
  }

  async close() {
    this.client.destroy();
  }
}
