# NestJS NATS JetStream Transporter

A NATS transporter for NestJS that takes advantage of [JetStream](https://docs.nats.io/jetstream/jetstream) for event patterns.

- [NestJS NATS JetStream Transporter](#nestjs-nats-jetstream-transporter)
  - [Installation](#installation)
  - [Publishing Messages](#publishing-messages)
    - [Request-Reply](#request-reply)
    - [Event-Based](#event-based)
  - [Receiving Messages](#receiving-messages)
    - [Customizing JetStream consumer options](#customizing-jetstream-consumer-options)
    - [NACK and TERM JetStream messages](#nack-and-term-jetstream-messages)
    - [Queue Groups](#queue-groups)
  - [Tests](#tests)
  
## Installation

```bash
$ npm install @alexy4744/nestjs-nats-jetstream-transporter nats@2.29.3
```

Node.js 14 or newer and a NATS server with JetStream enabled are required. The `nats` peer dependency is `^2.29.3`. See the [migration guide](./MIGRATION.md) before upgrading an existing deployment. NATS.js has deprecated the `nats` package in favor of its newer modular packages; this transporter currently uses the 2.x `nats` package.

## Publishing Messages

The `NatsClient` works mostly the same with the built-in NATS transporter for NestJS. The only difference is that you must instantiate `NatsClient` yourself.

### Request-Reply
```ts
// main.ts
import { NatsTransportStrategy } from "@alexy4744/nestjs-nats-jetstream-transporter";

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy()
});

await app.listenAsync();
```

```ts
// math.controller.ts
import { NatsClient } from "@alexy4744/nestjs-nats-jetstream-transporter";

@Controller()
export class MathController {
  private readonly client = new NatsClient();

  accumulate(): Observable<number> {
    return this.client.send<number>("sum", [1, 2, 3]);
  }
}
```

### Event-Based
```ts
// main.ts
import { NatsTransportStrategy } from "@alexy4744/nestjs-nats-jetstream-transporter";

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    // Create a stream with a subject called "orders.created"
    // This is important later on when we publish an event with NatsClient
    streams: [
      {
        name: "orders-events",
        subjects: ["orders.created"]
      }
    ]
  })
});

await app.listenAsync();
```

```ts
// orders.controller.ts
import { NatsClient } from "@alexy4744/nestjs-nats-jetstream-transporter";

@Controller()
export class OrdersController {
  private readonly client = new NatsClient();

  constructor(private readonly ordersService: OrdersService) {}

  async create(): Promise<void> {
    const order = await this.ordersService.create();

    this.client.emit("orders.created", order);
  }
}
```

## Receiving Messages

There are no special changes needed to receive messages. Just use the `@EventPattern()` and `@MessagePattern()` decorators provided by NestJS.

`@Ctx()` works exactly the same, however you should use the `NatsContext` provided by this package as the parameter type. It exposes an additional `getMessage()` method that returns the original message object if needed.

### Customizing JetStream consumer options

You can customize the JetStream push consumer using a `Partial<ConsumerConfig>`. Each event pattern uses a durable consumer; the default name is derived from the pattern. A configured `durable_name` is used as a prefix and the pattern is appended.

Read more about [JetStream consumers](https://docs.nats.io/jetstream/concepts/consumers). Time-based `ConsumerConfig` fields such as `ack_wait` are in nanoseconds.

```ts
import { nanos } from "nats";

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    consumer: (options) => {
      options.durable_name = "my-service";
      options.max_deliver = 5;
      options.ack_wait = nanos(30_000);
    }
  })
});
```

The transporter uses explicit acknowledgements, original replay, and delivery of all messages by default. Configuration is applied when a consumer is created; an existing durable retains its server-side configuration. See the [migration guide](./MIGRATION.md) for deployment considerations.

### NACK and TERM JetStream messages

By default, all JetStream messages are automatically acknowledged. However, you can also NACK and TERM the message by returning the respective symbols from your application.

Returning `NACK` will ask for the message to be redelivered, while `TERM` will terminate future deliveries of the message.

```ts
// shipping.controller.ts
import { NACK, TERM } from "@alexy4744/nestjs-nats-jetstream-transporter";

@Controller()
export class ShippingController {
  constructor(private readonly shippingService: ShippingService) {}

  @EventPattern("orders.created")
  handleCreatedOrder(order) {
    // If a shipment cannot be scheduled at this time, then ask for the message to be redelivered
    if (this.shippingService.isBusy()) {
      return NACK;
    }

    // If a shipment already exists for this order, then terminate the redelivery of this message
    if (this.shippingService.exists(order)) {
      return TERM;
    }

    this.shippingService.scheduleShipment(order);
    
    // Otherwise, the message will be auto-acked
  }
}
```

If the handler for your event pattern throws an error, the message will automatically be terminated. You can change this behavior by providing an `onError` function to the transport strategy options.

```ts
const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    // Messages that caused an exception will be acked instead
    onError: (message) => message.ack()
  })
});
```

### Queue Groups

You can specify a queue group name to enable [distributed queues](https://docs.nats.io/nats-concepts/queue). This will load balance message delivery across all other application instances with the same queue group name. It makes horozontal scaling simple as you can scale up by running another instance of your application with no additional configuration.

```ts
const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new NatsTransportStrategy({
    queue: "my-queue-group"
  })
});
```

The top-level `queue` applies to both request-reply and JetStream event subscriptions. For event subscriptions, `consumer` may instead set `deliver_group`; the top-level `queue` takes precedence when both are set. Instances sharing a durable consumer must use the same queue group.

## Development

```bash
# Run unit tests
$ npm test
```

To also run the integration test, start a NATS server with JetStream enabled and run `NATS_URL=nats://127.0.0.1:4222 npm test`. The test creates and deletes a uniquely named stream.

