import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Broker, BrokerMessage, openBroker } from './broker.js';

/**
 * The same contract the Go broker tests check. The memory broker always runs;
 * set BROKER_TEST=rabbitmq|sqs|pubsub (plus AMQP_URL / SQS_ENDPOINT /
 * PUBSUB_EMULATOR_HOST) to run it against a real broker.
 */
const kinds = ['memory', ...(process.env.BROKER_TEST ? [process.env.BROKER_TEST] : [])];

for (const kind of kinds) {
  describe(`broker conformance: ${kind}`, () => {
    const open = () => openBroker({ ...process.env, BROKER: kind });
    const topic = () => `conformance-nest-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

    async function consumeUntil(b: Broker, t: string, handler: (m: BrokerMessage) => Promise<boolean>) {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), 45_000);
      let done = false;
      await b.consume(
        t,
        4,
        async (m) => {
          if (await handler(m)) {
            done = true;
            ac.abort();
          }
        },
        ac.signal,
      );
      clearTimeout(timer);
      return done;
    }

    it('delivers every published message, published before any consumer', async () => {
      const b = await open();
      try {
        const t = topic();
        const n = 25;
        await b.publish(Array.from({ length: n }, (_, i) => ({ id: `ob-${i}`, topic: t, body: Buffer.from(`{"i":${i}}`) })));
        const seen = new Map<string, string>();
        const ok = await consumeUntil(b, t, async (m) => (seen.set(m.id, m.body.toString()), seen.size === n));
        assert.ok(ok, `received ${seen.size} of ${n}`);
        assert.equal(seen.get('ob-7'), '{"i":7}');
      } finally {
        await b.close();
      }
    });

    it('redelivers when the handler throws', async () => {
      const b = await open();
      try {
        const t = topic();
        await b.publish([{ id: 'ob-retry', topic: t, body: Buffer.from('{}') }]);
        let calls = 0;
        const ok = await consumeUntil(b, t, async () => {
          if (++calls === 1) throw new Error('transient');
          return true;
        });
        assert.ok(ok, 'nacked message was not redelivered');
      } finally {
        await b.close();
      }
    });
  });
}
