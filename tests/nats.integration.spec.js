"use strict";

const { lastValueFrom } = require("rxjs");
const { NatsClient } = require("../src/lib/nats.client");
const { NatsTransportStrategy } = require("../src/lib/nats.strategy");

const describeIntegration = process.env.NATS_URL ? describe : describe.skip;

describeIntegration("NATS JetStream integration", () => {
  test("creates and updates streams, reuses a durable, and exchanges messages", async () => {
    const streamName = `TEST_${process.pid}_${Date.now()}`;
    const eventSubject = `${streamName}.created`;
    const requestSubject = `${streamName}.request`;
    const connection = { servers: process.env.NATS_URL };
    const options = {
      connection,
      streams: [{ name: streamName, subjects: [eventSubject] }],
      queue: "workers",
      consumer: (config) => {
        config.durable_name = "integration";
        config.max_deliver = 5;
      },
    };
    const strategy = new NatsTransportStrategy(options);
    const client = new NatsClient({ connection });
    let manager;

    try {
      let resolveEvent;
      const eventReceived = new Promise((resolve) => { resolveEvent = resolve; });
      const eventHandler = async (data) => { resolveEvent(data); };
      eventHandler.isEventHandler = true;
      strategy.messageHandlers.set(eventSubject, eventHandler);
      strategy.messageHandlers.set(requestSubject, async (data) => data.value * 2);

      await strategy.listen(() => {});
      manager = strategy.jetstreamManager;
      await client.connect();
      await lastValueFrom(client.emit(eventSubject, { value: 21 }));
      expect(await eventReceived).toEqual({ value: 21 });
      expect(await lastValueFrom(client.send(requestSubject, { value: 21 }))).toBe(42);

      const durableName = strategy.createDurableName("integration", eventSubject);
      const consumer = await manager.consumers.info(streamName, durableName);
      expect(consumer.config.max_deliver).toBe(5);
      expect(consumer.config.deliver_group).toBe("workers");

      await strategy.close();
      await strategy.listen(() => {});
      await strategy.upsertStream(strategy.jetstreamManager, {
        name: streamName,
        subjects: [eventSubject],
        max_msgs: 100,
      });
      expect((await strategy.jetstreamManager.streams.info(streamName)).config.max_msgs).toBe(100);
    } finally {
      if (strategy.jetstreamManager) {
        await strategy.jetstreamManager.streams.delete(streamName);
      } else if (manager && client.getConnection()) {
        await client.getConnection().jetstreamManager().then((connectedManager) => connectedManager.streams.delete(streamName));
      }
      await strategy.close();
      await client.close();
    }
  }, 15000);
});