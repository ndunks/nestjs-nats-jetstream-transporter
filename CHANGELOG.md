# Changelog

This file was generated using [@jscutlery/semver](https://github.com/jscutlery/semver).

## Unreleased

- Require Node.js 14 or newer and `nats` `^2.29.3`.
- Fix JetStream stream updates and push consumer creation for NATS.js 2.29.3.
- Apply consumer configuration fields when creating a consumer; existing durables retain their server-side configuration.
- See [MIGRATION.md](./MIGRATION.md) for upgrade and consumer migration guidance.

## [1.0.9] — nats peer dependency upgrade to `^2.4.0`

### ⚠️ Breaking Changes

- **`nats` peer dependency** updated from `2.28.1` (exact pin) to `^2.4.0`.
- **`NatsTransportStrategyOptions.consumer` callback argument changed** — the callback now receives a plain `Partial<ConsumerConfig>` object instead of a `ConsumerOptsBuilder`. Builder methods such as `options.durable()`, `options.deliverTo()`, `options.ackExplicit()` etc. no longer exist; replace them with direct property assignments on the config object (e.g. `options.durable_name = "name"`).

See [MIGRATION.md](./MIGRATION.md) for a full field-by-field mapping and before/after code examples.

### Internal Changes

- Replaced `JetStreamClient.consumers.get().consume()` (introduced in nats `~2.17+`) with `consumerOpts()` builder + `JetStreamClient.subscribe()`, which is the correct API for nats `2.4.x`.
- Fixed `JetStreamManager.streams.update()` call signature to match nats `2.4.x` (`update(cfg)` — no stream name as first argument).
- Removed `ConsumerConfig.inactive_threshold` and `ConsumerConfig.name` fields from internal consumer config construction as they do not exist in nats `2.4.x`.

## [1.0.2](https://github.com/alexy4744/packages/compare/nestjs-nats-jetstream-transporter-1.0.1...nestjs-nats-jetstream-transporter-1.0.2) (2021-09-27)



## [1.0.1](https://github.com/alexy4744/packages/compare/nestjs-nats-jetstream-transporter-1.0.0...nestjs-nats-jetstream-transporter-1.0.1) (2021-09-27)



# 1.0.0 (2021-07-08)


### Features

* **nestjs-nats-jetstream-transporter:** add nestjs-nats-jetstream transporter package ([d529e78](https://github.com/alexy4744/packages/commit/d529e78ae8e496c3e758eb747152f95bcae838c6))
