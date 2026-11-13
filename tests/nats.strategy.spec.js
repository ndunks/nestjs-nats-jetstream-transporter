"use strict";

/**
 * Compatibility tests for NatsTransportStrategy with nats@2.29.3
 *
 * These tests verify that the library correctly uses the nats 2.29.3 API:
 *  - consumerOpts() builder and push subscriptions
 *  - streams.update(name, cfg)
 *  - ConsumerConfig plain-object fields (durable_name, deliver_group, etc.)
 *  - All status.type strings from the Events / DebugEvents enums
 */

// ---------------------------------------------------------------------------
// Module-level mock for nats — must be hoisted before any require()
// consumerOpts is exported as a non-configurable/non-writable property in
// nats@2.29.3 so jest.spyOn() cannot patch it; we mock the whole module instead.
// ---------------------------------------------------------------------------
const realNats = jest.requireActual("nats");

jest.mock("nats", () => {
  const actual = jest.requireActual("nats");
  return {
    ...actual,
    consumerOpts: jest.fn((options) => actual.consumerOpts(options)),
  };
});

const { NatsTransportStrategy } = require("../src/lib/nats.strategy");
const { NACK, TERM } = require("../src/lib/nats.constants");
const nats = require("nats");

// ---------------------------------------------------------------------------
// Helpers – build lightweight fakes for connection and manager APIs
// ---------------------------------------------------------------------------

function makeJsMsg(overrides = {}) {
  return {
    data: nats.JSONCodec().encode({ hello: "world" }),
    subject: "test.subject",
    headers: undefined,
    working: jest.fn(),
    ack: jest.fn(),
    nak: jest.fn(),
    term: jest.fn(),
    ...overrides,
  };
}

function makeConsumerInfo(stream = "my-stream", name = "subscriber-orders-created") {
  return { stream_name: stream, name, config: {} };
}

function makeJetStreamSubscription() {
  return { unsubscribe: jest.fn() };
}

function makeJetStreamClient(opts = {}) {
  return {
    subscribe: jest.fn().mockResolvedValue(makeJetStreamSubscription()),
    publish: jest.fn().mockResolvedValue(undefined),
    ...opts,
  };
}

function makeJetStreamManager(opts = {}) {
  const consumerInfo = makeConsumerInfo();
  return {
    consumers: {
      info: jest.fn().mockRejectedValue(new Error("consumer not found")),
      add: jest.fn().mockResolvedValue(consumerInfo),
    },
    streams: {
      info: jest.fn().mockResolvedValue({ config: { name: "my-stream", subjects: [] } }),
      update: jest.fn().mockResolvedValue({ config: {} }),
      add: jest.fn().mockResolvedValue({ config: {} }),
    },
    ...opts,
  };
}

function makeNatsConnection(opts = {}) {
  const statusIterable = (async function* () {})();
  return {
    getServer: jest.fn().mockReturnValue("nats://localhost:4222"),
    drain: jest.fn().mockResolvedValue(undefined),
    jetstream: jest.fn().mockReturnValue(makeJetStreamClient()),
    jetstreamManager: jest.fn().mockResolvedValue(makeJetStreamManager()),
    status: jest.fn().mockReturnValue(statusIterable),
    subscribe: jest.fn().mockReturnValue({ unsubscribe: jest.fn() }),
    request: jest.fn().mockResolvedValue({ data: nats.JSONCodec().encode({ result: 42 }) }),
    ...opts,
  };
}

// ---------------------------------------------------------------------------
// 1. NatsTransportStrategy — constructor
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — constructor", () => {
  test("instantiates with no options", () => {
    const strategy = new NatsTransportStrategy();
    expect(strategy).toBeInstanceOf(NatsTransportStrategy);
  });

  test("accepts codec option", () => {
    const codec = nats.JSONCodec();
    const strategy = new NatsTransportStrategy({ codec });
    expect(strategy.codec).toBe(codec);
  });

  test("defaults to JSONCodec when no codec provided", () => {
    const strategy = new NatsTransportStrategy();
    // JSONCodec returns an object with encode/decode — just verify they exist
    expect(typeof strategy.codec.encode).toBe("function");
    expect(typeof strategy.codec.decode).toBe("function");
  });
});

// ---------------------------------------------------------------------------
// 2. NatsTransportStrategy — createDurableName
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — createDurableName", () => {
  let strategy;
  beforeEach(() => { strategy = new NatsTransportStrategy(); });

  test("joins parts with hyphens", () => {
    expect(strategy.createDurableName("subscriber", "orders.created")).toBe("subscriber-orders-created");
  });

  test("replaces spaces with hyphens", () => {
    expect(strategy.createDurableName("my consumer", "event")).toBe("my-consumer-event");
  });

  test("replaces NATS wildcard * with hyphen", () => {
    expect(strategy.createDurableName("sub", "orders.*")).toBe("sub-orders--");
  });

  test("replaces NATS wildcard > with hyphen", () => {
    expect(strategy.createDurableName("sub", "orders.>")).toBe("sub-orders--");
  });
});

// ---------------------------------------------------------------------------
// 3. NatsTransportStrategy — createJetStreamClient / createJetStreamManager
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — JetStream factory methods", () => {
  let strategy;
  beforeEach(() => { strategy = new NatsTransportStrategy(); });

  test("createJetStreamClient calls connection.jetstream()", () => {
    const conn = makeNatsConnection();
    strategy.createJetStreamClient(conn);
    expect(conn.jetstream).toHaveBeenCalledTimes(1);
  });

  test("createJetStreamManager calls connection.jetstreamManager()", async () => {
    const conn = makeNatsConnection();
    await strategy.createJetStreamManager(conn);
    expect(conn.jetstreamManager).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// 4. NatsTransportStrategy — subscribeToEventPatterns
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — subscribeToEventPatterns", () => {
  let strategy;
  let jsClient;
  let jsm;
  beforeEach(() => {
    nats.consumerOpts.mockClear();

    jsClient = makeJetStreamClient();
    jsm = makeJetStreamManager();
    strategy = new NatsTransportStrategy();
  });

  test("uses consumerOpts() builder — not consumers.get().consume()", async () => {
    // Register a fake event handler
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    expect(nats.consumerOpts).toHaveBeenCalled();
    expect(jsClient.consumers).toBeUndefined();
  });

  test("calls js.subscribe(pattern, opts) with the consumerOpts builder", async () => {
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    expect(jsClient.subscribe).toHaveBeenCalledWith("orders.created", expect.objectContaining({ getOpts: expect.any(Function) }));
  });

  test("sets a default durable name from the pattern", async () => {
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    const options = jsClient.subscribe.mock.calls[0][1].getOpts();
    expect(options.config.durable_name).toBe("subscriber-orders-created");
    expect(options.config.deliver_subject).toMatch(/^_INBOX\./);
    expect(options.mack).toBe(true);
  });

  test("uses durable_name from consumer config callback", async () => {
    strategy = new NatsTransportStrategy({
      consumer: (cfg) => { cfg.durable_name = "my-durable"; },
    });
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    jsClient = makeJetStreamClient();
    jsm = makeJetStreamManager();

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    expect(jsClient.subscribe.mock.calls[0][1].getOpts().config.durable_name).toBe("my-durable-orders-created");
  });

  test("applies queue group via consumerOpts.queue()", async () => {
    strategy = new NatsTransportStrategy({ queue: "my-group" });
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    jsClient = makeJetStreamClient();
    jsm = makeJetStreamManager();

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    const options = jsClient.subscribe.mock.calls[0][1].getOpts();
    expect(options.queue).toBe("my-group");
    expect(options.config.deliver_group).toBe("my-group");
  });

  test("passes consumer config overrides to the real builder", async () => {
    strategy = new NatsTransportStrategy({
      consumer: (config) => {
        config.max_deliver = 5;
        config.ack_wait = nats.nanos(30000);
        config.deliver_policy = nats.DeliverPolicy.New;
        config.deliver_group = "workers";
      },
    });
    strategy.messageHandlers.set("orders.created", { isEventHandler: true });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    const options = jsClient.subscribe.mock.calls[0][1].getOpts();
    expect(options.config).toEqual(expect.objectContaining({
      max_deliver: 5,
      ack_wait: nats.nanos(30000),
      deliver_policy: nats.DeliverPolicy.New,
      deliver_group: "workers",
    }));
    expect(options.queue).toBe("workers");
  });

  test("throws descriptive error when stream does not match subject", async () => {
    jsClient.subscribe.mockRejectedValue(new Error("no stream matches subject"));
    strategy.messageHandlers.set("orphan.event", { isEventHandler: true, callback: jest.fn() });

    await expect(strategy.subscribeToEventPatterns(jsClient, jsm))
      .rejects.toThrow("Cannot find stream with the orphan.event event pattern");
  });

  test("uses the push subscription API", async () => {
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    // If the old code ran, it would have crashed because jsClient.consumers is undefined
    // This test passes only if the new code path (js.subscribe) is taken
    expect(jsClient.consumers).toBeUndefined();
    expect(jsClient.subscribe).toHaveBeenCalled();
  });

  test("skips non-event (message pattern) handlers", async () => {
    strategy.messageHandlers.set("sum", { isEventHandler: false, callback: jest.fn() });

    await strategy.subscribeToEventPatterns(jsClient, jsm);

    expect(jsClient.subscribe).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 5. NatsTransportStrategy — subscribeToMessagePatterns
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — subscribeToMessagePatterns", () => {
  test("subscribes using connection.subscribe (core NATS — unchanged API)", () => {
    const strategy = new NatsTransportStrategy();
    strategy.messageHandlers.set("sum", { isEventHandler: false, callback: jest.fn() });

    const conn = makeNatsConnection();
    strategy.subscribeToMessagePatterns(conn);

    expect(conn.subscribe).toHaveBeenCalledWith("sum", expect.objectContaining({
      callback: expect.any(Function),
    }));
  });

  test("passes queue group to subscription options", () => {
    const strategy = new NatsTransportStrategy({ queue: "my-group" });
    strategy.messageHandlers.set("sum", { isEventHandler: false, callback: jest.fn() });

    const conn = makeNatsConnection();
    strategy.subscribeToMessagePatterns(conn);

    expect(conn.subscribe).toHaveBeenCalledWith("sum", expect.objectContaining({
      queue: "my-group",
    }));
  });

  test("skips event handlers", () => {
    const strategy = new NatsTransportStrategy();
    strategy.messageHandlers.set("orders.created", { isEventHandler: true, callback: jest.fn() });

    const conn = makeNatsConnection();
    strategy.subscribeToMessagePatterns(conn);

    expect(conn.subscribe).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 6. NatsTransportStrategy — upsertStream
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — upsertStream", () => {
  let strategy;

  beforeEach(() => { strategy = new NatsTransportStrategy(); });

  test("calls streams.update(name, mergedConfig) when stream exists", async () => {
    const existingConfig = { name: "orders-events", subjects: ["orders.*"], max_msgs: 100 };
    const jsm = makeJetStreamManager();
    jsm.streams.info.mockResolvedValue({ config: existingConfig });

    await strategy.upsertStream(jsm, { name: "orders-events", subjects: ["orders.created"] });

    expect(jsm.streams.update).toHaveBeenCalledTimes(1);
    const [firstArg, secondArg] = jsm.streams.update.mock.calls[0];
    expect(firstArg).toBe("orders-events");
    expect(secondArg.name).toBe("orders-events");
  });

  test("merges existing stream config with new config on update", async () => {
    const jsm = makeJetStreamManager();
    jsm.streams.info.mockResolvedValue({ config: { name: "orders-events", max_msgs: 500 } });

    await strategy.upsertStream(jsm, { name: "orders-events", subjects: ["orders.created"] });

    const [, mergedConfig] = jsm.streams.update.mock.calls[0];
    expect(mergedConfig.max_msgs).toBe(500);               // kept from existing
    expect(mergedConfig.subjects).toEqual(["orders.created"]); // applied from new
  });

  test("calls streams.add() when stream does not exist", async () => {
    const jsm = makeJetStreamManager();
    jsm.streams.info.mockRejectedValue(new Error("stream not found"));

    await strategy.upsertStream(jsm, { name: "new-stream", subjects: ["new.event"] });

    expect(jsm.streams.add).toHaveBeenCalledWith({ name: "new-stream", subjects: ["new.event"] });
    expect(jsm.streams.update).not.toHaveBeenCalled();
  });

  test("re-throws unexpected errors from streams.info()", async () => {
    const jsm = makeJetStreamManager();
    jsm.streams.info.mockRejectedValue(new Error("connection lost"));

    await expect(strategy.upsertStream(jsm, { name: "orders-events" }))
      .rejects.toThrow("connection lost");
  });
});

// ---------------------------------------------------------------------------
// 7. NatsTransportStrategy — handleJetStreamMessage
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — handleJetStreamMessage", () => {
  let strategy;

  beforeEach(() => { strategy = new NatsTransportStrategy(); });

  test("calls message.working() before invoking handler", async () => {
    const msg = makeJsMsg();
    const handler = jest.fn().mockResolvedValue(undefined);
    // transformToObservable is a Server method; stub it
    strategy.transformToObservable = jest.fn().mockReturnValue({ toPromise: () => Promise.resolve(undefined) });
    // lastValueFrom is from rxjs — stub at handler level to avoid Observable
    const { lastValueFrom } = require("rxjs");
    jest.spyOn(require("rxjs"), "lastValueFrom").mockResolvedValue(undefined);

    await strategy.handleJetStreamMessage(msg, handler);

    expect(msg.working).toHaveBeenCalled();
    jest.restoreAllMocks();
  });

  test("acks message when handler resolves normally", async () => {
    const msg = makeJsMsg();
    strategy.transformToObservable = jest.fn().mockReturnValue({});
    jest.spyOn(require("rxjs"), "lastValueFrom").mockResolvedValue("done");

    await strategy.handleJetStreamMessage(msg, jest.fn().mockResolvedValue("done"));

    expect(msg.ack).toHaveBeenCalled();
    expect(msg.nak).not.toHaveBeenCalled();
    expect(msg.term).not.toHaveBeenCalled();
    jest.restoreAllMocks();
  });

  test("calls message.nak() when handler returns NACK symbol", async () => {
    const msg = makeJsMsg();
    strategy.transformToObservable = jest.fn().mockReturnValue({});
    jest.spyOn(require("rxjs"), "lastValueFrom").mockResolvedValue(NACK);

    await strategy.handleJetStreamMessage(msg, jest.fn().mockResolvedValue(NACK));

    expect(msg.nak).toHaveBeenCalled();
    expect(msg.ack).not.toHaveBeenCalled();
    jest.restoreAllMocks();
  });

  test("calls message.term() when handler returns TERM symbol", async () => {
    const msg = makeJsMsg();
    strategy.transformToObservable = jest.fn().mockReturnValue({});
    jest.spyOn(require("rxjs"), "lastValueFrom").mockResolvedValue(TERM);

    await strategy.handleJetStreamMessage(msg, jest.fn().mockResolvedValue(TERM));

    expect(msg.term).toHaveBeenCalled();
    expect(msg.ack).not.toHaveBeenCalled();
    jest.restoreAllMocks();
  });

  test("calls default onError (term) when handler throws", async () => {
    const msg = makeJsMsg();
    strategy.transformToObservable = jest.fn().mockReturnValue({});
    jest.spyOn(require("rxjs"), "lastValueFrom").mockRejectedValue(new Error("boom"));

    await strategy.handleJetStreamMessage(msg, jest.fn().mockRejectedValue(new Error("boom")));

    expect(msg.term).toHaveBeenCalled();
    jest.restoreAllMocks();
  });

  test("calls custom onError when provided and handler throws", async () => {
    const onError = jest.fn();
    strategy = new NatsTransportStrategy({ onError });
    const msg = makeJsMsg();
    strategy.transformToObservable = jest.fn().mockReturnValue({});
    jest.spyOn(require("rxjs"), "lastValueFrom").mockRejectedValue(new Error("boom"));

    await strategy.handleJetStreamMessage(msg, jest.fn().mockRejectedValue(new Error("boom")));

    expect(onError).toHaveBeenCalledWith(msg);
    expect(msg.term).not.toHaveBeenCalled();
    jest.restoreAllMocks();
  });
});

// ---------------------------------------------------------------------------
// 8. NatsTransportStrategy — handleStatusUpdates
// ---------------------------------------------------------------------------

describe("NatsTransportStrategy — handleStatusUpdates", () => {
  let strategy;

  beforeEach(() => { strategy = new NatsTransportStrategy(); });

  async function emitStatus(type, data = "detail") {
    async function* gen() { yield { type, data }; }
    const conn = makeNatsConnection();
    conn.status.mockReturnValue(gen());
    strategy.logger = { log: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), verbose: jest.fn() };
    await strategy.handleStatusUpdates(conn);
  }

  // Events enum values
  test("logs error on 'disconnect'",      async () => { await emitStatus("disconnect");      expect(strategy.logger.error).toHaveBeenCalled(); });
  test("logs error on 'error'",           async () => { await emitStatus("error");           expect(strategy.logger.error).toHaveBeenCalled(); });
  test("logs info on 'reconnect'",        async () => { await emitStatus("reconnect");        expect(strategy.logger.log).toHaveBeenCalled(); });
  test("logs warn on 'ldm'",             async () => { await emitStatus("ldm");             expect(strategy.logger.warn).toHaveBeenCalled(); });
  test("logs verbose on 'update'",        async () => { await emitStatus("update", { server: {} }); expect(strategy.logger.verbose).toHaveBeenCalled(); });

  // DebugEvents enum values
  test("logs debug on 'pingTimer'",       async () => { await emitStatus("pingTimer");       expect(strategy.logger.debug).toHaveBeenCalled(); });
  test("logs debug on 'reconnecting'",    async () => { await emitStatus("reconnecting");    expect(strategy.logger.debug).toHaveBeenCalled(); });
  test("logs debug on 'staleConnection'", async () => { await emitStatus("staleConnection"); expect(strategy.logger.debug).toHaveBeenCalled(); });
});

// ---------------------------------------------------------------------------
// 9. nats@2.29.3 API surface — verify required exports exist
// ---------------------------------------------------------------------------

describe("nats@2.29.3 — required API surface", () => {
  test("connect is exported", () => {
    expect(typeof nats.connect).toBe("function");
  });

  test("JSONCodec is exported", () => {
    expect(typeof nats.JSONCodec).toBe("function");
    const codec = nats.JSONCodec();
    expect(typeof codec.encode).toBe("function");
    expect(typeof codec.decode).toBe("function");
  });

  test("consumerOpts() factory is exported and returns a builder", () => {
    expect(typeof nats.consumerOpts).toBe("function");
    const builder = nats.consumerOpts();
    expect(typeof builder.durable).toBe("function");
    expect(typeof builder.ackExplicit).toBe("function");
    expect(typeof builder.replayOriginal).toBe("function");
    expect(typeof builder.manualAck).toBe("function");
    expect(typeof builder.deliverAll).toBe("function");
    expect(typeof builder.callback).toBe("function");
    expect(typeof builder.queue).toBe("function");
  });

  test("AckPolicy enum is exported with Explicit value", () => {
    expect(nats.AckPolicy).toBeDefined();
    expect(nats.AckPolicy.Explicit).toBeDefined();
  });

  test("ReplayPolicy enum is exported with Original value", () => {
    expect(nats.ReplayPolicy).toBeDefined();
    expect(nats.ReplayPolicy.Original).toBeDefined();
  });

  test("DeliverPolicy enum is exported", () => {
    expect(nats.DeliverPolicy).toBeDefined();
    expect(nats.DeliverPolicy.All).toBeDefined();
  });

  test("nanos() helper is exported", () => {
    expect(typeof nats.nanos).toBe("function");
    expect(nats.nanos(1000)).toBeGreaterThan(0);
  });

  test("Events enum matches expected status type strings", () => {
    expect(nats.Events.Disconnect).toBe("disconnect");
    expect(nats.Events.Reconnect).toBe("reconnect");
    expect(nats.Events.Update).toBe("update");
    expect(nats.Events.LDM).toBe("ldm");
    expect(nats.Events.Error).toBe("error");
  });

  test("DebugEvents enum matches expected status type strings", () => {
    expect(nats.DebugEvents.Reconnecting).toBe("reconnecting");
    expect(nats.DebugEvents.PingTimer).toBe("pingTimer");
    expect(nats.DebugEvents.StaleConnection).toBe("staleConnection");
  });

  test("consumerOpts accepts a consumer config", () => {
    expect(realNats.consumerOpts({ max_deliver: 5 }).getOpts().config.max_deliver).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// 10. NatsContext
// ---------------------------------------------------------------------------

describe("NatsContext", () => {
  const { NatsContext } = require("../src/lib/nats.context");

  test("getMessage() returns the original JsMsg", () => {
    const msg = makeJsMsg();
    const ctx = new NatsContext([msg]);
    expect(ctx.getMessage()).toBe(msg);
  });

  test("getHeaders() returns message headers", () => {
    const msg = makeJsMsg({ headers: { "nats-msg-id": "abc" } });
    const ctx = new NatsContext([msg]);
    expect(ctx.getHeaders()).toEqual({ "nats-msg-id": "abc" });
  });

  test("getSubject() returns message subject", () => {
    const msg = makeJsMsg({ subject: "orders.created" });
    const ctx = new NatsContext([msg]);
    expect(ctx.getSubject()).toBe("orders.created");
  });
});

// ---------------------------------------------------------------------------
// 11. NACK / TERM constants
// ---------------------------------------------------------------------------

describe("NACK / TERM constants", () => {
  test("NACK is a Symbol", () => {
    expect(typeof NACK).toBe("symbol");
  });

  test("TERM is a Symbol", () => {
    expect(typeof TERM).toBe("symbol");
  });

  test("NACK and TERM are distinct", () => {
    expect(NACK).not.toBe(TERM);
  });
});
