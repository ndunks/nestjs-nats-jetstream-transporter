# Migration Guide — `nats` peer dependency upgrade to `^2.4.0`

This document describes the breaking changes introduced when the `nats` peer dependency was updated from `2.2.x` to `^2.4.0`, and what you need to review or update in your own application code.

---

## What changed in this library

| Area | Change |
|---|---|
| `nats` peer dependency | `2.28.1` (exact) → `^2.4.0` |
| `NatsTransportStrategyOptions.consumer` callback | **Breaking** — argument type changed (see below) |
| JetStream stream update internal call | Fixed to match `nats@2.4.x` `StreamAPI.update(cfg)` signature |
| JetStream event subscription internals | Replaced `consumers.get().consume()` with `consumerOpts()` + `js.subscribe()` |

---

## Breaking Changes for Library Users

### 1. `consumer` callback argument — `ConsumerOptsBuilder` → `Partial<ConsumerConfig>`

> **Keywords to search in your IDE:** `consumer:`, `NatsTransportStrategyOptions`, `options.durable`, `options.deliverTo`, `options.queue`, `options.ackExplicit`, `options.replayOriginal`

**This is the most impactful change.** The `consumer` option in `NatsTransportStrategyOptions` previously received a `ConsumerOptsBuilder` — an object with fluent builder methods. It now receives a plain `Partial<ConsumerConfig>` object whose properties you mutate directly.

#### Before (nats `2.2.x` — builder methods)

```ts
new NatsTransportStrategy({
  consumer: (options) => {
    options.durable("my-durable-name");
    options.deliverTo("my-queue-group");
    options.queue("my-queue-group");
    options.ackExplicit();
    options.replayOriginal();
    options.deliverAll();
    options.maxDeliver(5);
    options.ackWait(30_000);
  }
});
```

#### After (nats `2.4.x` — plain config object)

```ts
import { AckPolicy, DeliverPolicy, ReplayPolicy } from "nats";

new NatsTransportStrategy({
  consumer: (options) => {
    options.durable_name = "my-durable-name";
    options.deliver_group = "my-queue-group";
    options.max_deliver = 5;
    options.ack_wait = 30_000 * 1_000_000; // in nanoseconds
    options.deliver_policy = DeliverPolicy.All;
    // ack_policy and replay_policy are pre-set to Explicit / Original
    // and do not need to be set unless you want to override them
  }
});
```

#### Builder method → `ConsumerConfig` field mapping

| Old builder method | New `ConsumerConfig` field | Notes |
|---|---|---|
| `options.durable("name")` | `options.durable_name = "name"` | |
| `options.deliverTo("subject")` | _(no equivalent needed)_ | Delivery subject is managed internally; remove this call |
| `options.queue("name")` | `options.deliver_group = "name"` | Also set `queue` at the top-level strategy options |
| `options.ackExplicit()` | `options.ack_policy = AckPolicy.Explicit` | Already the default; only set to override |
| `options.replayOriginal()` | `options.replay_policy = ReplayPolicy.Original` | Already the default; only set to override |
| `options.deliverAll()` | `options.deliver_policy = DeliverPolicy.All` | Already the default; only set to override |
| `options.deliverLast()` | `options.deliver_policy = DeliverPolicy.Last` | |
| `options.deliverNew()` | `options.deliver_policy = DeliverPolicy.New` | |
| `options.maxDeliver(n)` | `options.max_deliver = n` | |
| `options.ackWait(ms)` | `options.ack_wait = ms * 1_000_000` | Converts milliseconds to nanoseconds |
| `options.maxAckPending(n)` | `options.max_ack_pending = n` | |
| `options.filterSubject("s")` | `options.filter_subject = "s"` | |
| `options.idleHeartbeat(ms)` | `options.idle_heartbeat = ms * 1_000_000` | Converts milliseconds to nanoseconds |

---

### 2. Queue groups with JetStream events

> **Keywords to search in your IDE:** `deliverTo`, `options.deliverTo`, `queue`, `deliver_group`

Previously, the README instructed you to call both `options.deliverTo()` and `options.queue()` with the same name when using queue groups with JetStream events. The `deliverTo` call is **no longer needed** — the library handles the delivery subject internally. Only set the `deliver_group` field (and the top-level `queue` option).

#### Before

```ts
new NatsTransportStrategy({
  consumer: (options) => {
    options.deliverTo("my-queue-group"); // ← remove this
    options.queue("my-queue-group");
  },
  queue: "my-queue-group"
});
```

#### After

```ts
new NatsTransportStrategy({
  consumer: (options) => {
    options.deliver_group = "my-queue-group";
  },
  queue: "my-queue-group"
});
```

---

### 3. `ConsumerConfig` fields not available in `nats@2.4.x`

> **Keywords to search in your IDE:** `inactive_threshold`, `name` (on consumer config), `nanos(`

The following `ConsumerConfig` fields **do not exist** in `nats@2.4.x`. If you set them inside the `consumer` callback, they will be silently ignored at runtime and may cause TypeScript type errors.

| Field | Introduced in | Action |
|---|---|---|
| `name` | `nats@~2.9+` | Remove — use `durable_name` instead |
| `inactive_threshold` | `nats@~2.17+` | Remove — no equivalent in `2.4.x` |

---

### 4. No changes required for these patterns

The following parts of this library's public API are **unchanged** and require no migration work:

- `@EventPattern()` and `@MessagePattern()` decorators usage
- `NatsClient` — `send()` and `emit()` calls
- `NatsContext` — `getMessage()` usage
- `NACK` and `TERM` symbols returned from handlers
- `onError` option in `NatsTransportStrategyOptions`
- `streams` option — stream configuration (`NatsStreamConfig`)
- `connection` option — `ConnectionOptions` from `nats`
- `codec` option — `JSONCodec()`, `StringCodec()`, or custom codecs
- `queue` top-level option

---

## Full Before/After Example

### Before (nats `2.2.x`)

```ts
import { NatsTransportStrategy } from "@alexy4744/nestjs-nats-jetstream-transporter";

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    streams: [
      { name: "orders-events", subjects: ["orders.created"] }
    ],
    consumer: (options) => {
      options.durable("my-service-consumer");
      options.maxDeliver(5);
      options.ackWait(30_000);
    },
    queue: "my-queue-group"
  })
});
```

### After (nats `^2.4.0`)

```ts
import { NatsTransportStrategy } from "@alexy4744/nestjs-nats-jetstream-transporter";
import { DeliverPolicy } from "nats";

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    streams: [
      { name: "orders-events", subjects: ["orders.created"] }
    ],
    consumer: (options) => {
      options.durable_name = "my-service-consumer";
      options.max_deliver = 5;
      options.ack_wait = 30_000 * 1_000_000; // milliseconds → nanoseconds
    },
    queue: "my-queue-group"
  })
});
```
