# Migrating to NATS.js 2.29.3

This transporter supports the `nats` 2.x client starting at 2.29.3. Upgrade the transporter and its peer dependency together:

```bash
npm install @alexy4744/nestjs-nats-jetstream-transporter nats@2.29.3
```

Node.js 14 or newer is required by NATS.js 2.29.3. A NATS server with JetStream enabled is required for event patterns. The package still uses the NATS.js push subscription API; it does not require the newer pull-consumer API. NATS.js has deprecated the `nats` package in favor of modular packages, which are not a drop-in replacement for this transporter.

## Consumer options

The `consumer` callback receives a mutable `Partial<ConsumerConfig>`. If your application uses an older transporter whose callback accepted builder methods, replace those calls with config fields:

```ts
import { DeliverPolicy, nanos } from "nats";
import { NatsTransportStrategy } from "@alexy4744/nestjs-nats-jetstream-transporter";

new NatsTransportStrategy({
  queue: "workers",
  consumer: (config) => {
    config.durable_name = "orders-service";
    config.max_deliver = 5;
    config.ack_wait = nanos(30_000);
    config.deliver_policy = DeliverPolicy.All;
  },
});
```

| Builder method | Config field |
| --- | --- |
| `durable(name)` | `durable_name = name` |
| `maxDeliver(count)` | `max_deliver = count` |
| `ackWait(milliseconds)` | `ack_wait = nanos(milliseconds)` |
| `deliverAll()` / `deliverLast()` / `deliverNew()` | `deliver_policy = DeliverPolicy.All` / `Last` / `New` |
| `maxAckPending(count)` | `max_ack_pending = count` |
| `idleHeartbeat(milliseconds)` | `idle_heartbeat = nanos(milliseconds)` |
| `queue(name)` | top-level `queue: name` or `deliver_group = name` |

The transporter generates a delivery inbox unless `deliver_subject` is supplied. The top-level `queue` applies to both request-reply and event subscriptions and takes precedence over `deliver_group`. The durable name is derived from the configured prefix plus the event pattern, or from `subscriber` plus the pattern when no prefix is set; spaces, dots, and wildcards are replaced by hyphens.

The callback already accepted a config object in the immediately preceding implementation, but only `durable_name` was applied. Other supplied fields are now passed to NATS when creating a consumer. Defaults are explicit acknowledgement, original replay, and delivery of all messages. Avoid changing the acknowledgement policy unless your handlers and server configuration support it.

## Existing deployments

Existing durable consumers are reused with their server-side configuration. Changing the callback does not update an existing consumer. Check the existing consumer's delivery mode, delivery group, acknowledgement settings, and durable name before deployment. An existing pull consumer cannot be reused as a push consumer. If its configuration is incompatible or must change, plan a controlled consumer migration using NATS management tools. Deleting or replacing a consumer can affect pending messages and delivery position; do not delete one without assessing that impact. Instances sharing a durable must use the same queue group.

The transporter now uses the 2.29 stream update signature and supplies a delivery subject for new push consumers. Stream configuration, `NatsClient`, `NatsContext`, `NACK`, `TERM`, and handler decorators retain their public interfaces. Applications using NATS APIs directly should review NATS.js changes separately.