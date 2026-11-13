"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NatsTransportStrategy = void 0;
const tslib_1 = require("tslib");
const microservices_1 = require("@nestjs/microservices");
const common_1 = require("@nestjs/common");
const nats_1 = require("nats");
const rxjs_1 = require("rxjs");
const nats_context_1 = require("./nats.context");
const nats_constants_1 = require("./nats.constants");
class NatsTransportStrategy extends microservices_1.Server {
    constructor(options = {}) {
        super();
        this.options = options;
        this.codec = options.codec || nats_1.JSONCodec();
        this.logger = new common_1.Logger("NatsServer");
    }
    listen(callback) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            this.connection = yield this.createNatsConnection(this.options.connection);
            this.jetstreamClient = this.createJetStreamClient(this.connection);
            this.jetstreamManager = yield this.createJetStreamManager(this.connection);
            this.handleStatusUpdates(this.connection);
            yield this.createStreams(this.jetstreamManager, this.options.streams);
            yield this.subscribeToEventPatterns(this.jetstreamClient, this.jetstreamManager);
            this.subscribeToMessagePatterns(this.connection);
            this.logger.log(`Connected to ${this.connection.getServer()}`);
            callback();
        });
    }
    close() {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            if (this.connection) {
                yield this.connection.drain();
                this.connection = undefined;
                this.jetstreamClient = undefined;
                this.jetstreamManager = undefined;
            }
        });
    }
    /**
     * Create a durable name that follows NATS naming rules
     * @see https://docs.nats.io/jetstream/administration/naming
     */
    createDurableName(...parts) {
        return parts.join("-").replace(/\s|\.|>|\*/g, "-");
    }
    createJetStreamClient(connection) {
        return connection.jetstream();
    }
    createJetStreamManager(connection) {
        return connection.jetstreamManager();
    }
    createNatsConnection(options = {}) {
        return nats_1.connect(options);
    }
    createStreams(manager, configs = []) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            yield Promise.all(configs.map((config) => this.upsertStream(manager, config)));
        });
    }
    handleJetStreamMessage(message, handler) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            const handleError = this.options.onError || ((message) => message.term());
            try {
                const decoded = this.codec.decode(message.data);
                message.working();
                const signal = yield handler(decoded, new nats_context_1.NatsContext([message]))
                    .then((maybeObservable) => this.transformToObservable(maybeObservable))
                    .then((observable) => rxjs_1.lastValueFrom(observable));
                if (signal === nats_constants_1.NACK) {
                    return message.nak();
                }
                if (signal === nats_constants_1.TERM) {
                    return message.term();
                }
                message.ack();
            }
            catch (_a) {
                handleError(message);
            }
        });
    }
    handleNatsMessage(message, handler) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            const decoded = this.codec.decode(message.data);
            const maybeObservable = yield handler(decoded, new nats_context_1.NatsContext([message]));
            const response$ = this.transformToObservable(maybeObservable);
            this.send(response$, (response) => {
                const encoded = this.codec.encode(response);
                message.respond(encoded);
            });
        });
    }
    handleStatusUpdates(connection) {
        var e_1, _a;
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            try {
                for (var _b = tslib_1.__asyncValues(connection.status()), _c; _c = yield _b.next(), !_c.done;) {
                    const status = _c.value;
                    const data = typeof status.data === "object" ? JSON.stringify(status.data) : status.data;
                    const message = `(${status.type}): ${data}`;
                    switch (status.type) {
                        case "pingTimer":
                        case "reconnecting":
                        case "staleConnection":
                            this.logger.debug(message);
                            break;
                        case "disconnect":
                        case "error":
                            this.logger.error(message);
                            break;
                        case "reconnect":
                            this.logger.log(message);
                            break;
                        case "ldm":
                            this.logger.warn(message);
                            break;
                        case "update":
                            this.logger.verbose(message);
                            break;
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
                }
                finally { if (e_1) throw e_1.error; }
            }
        });
    }
    subscribeToEventPatterns(client, jsm) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            const eventHandlers = [...this.messageHandlers.entries()].filter(([, handler]) => handler.isEventHandler);
            for (const [pattern, handler] of eventHandlers) {
                const consumerConfig = {
                    ack_policy: nats_1.AckPolicy.Explicit,
                    replay_policy: nats_1.ReplayPolicy.Original,
                    deliver_policy: nats_1.DeliverPolicy.All,
                };
                if (this.options.consumer) {
                    this.options.consumer(consumerConfig);
                }
                const durableName = this.createDurableName(consumerConfig.durable_name || "subscriber", pattern);
                const consumerOptions = nats_1.consumerOpts(consumerConfig);
                consumerOptions.manualAck();
                consumerOptions.durable(durableName);
                if (!consumerConfig.deliver_subject) {
                    consumerOptions.deliverTo(nats_1.createInbox());
                }
                const queue = this.options.queue || consumerConfig.deliver_group;
                if (queue) {
                    consumerOptions.queue(queue);
                }
                consumerOptions.callback((error, message) => {
                    if (error) {
                        return this.logger.error(error.message, error.stack);
                    }
                    if (message) {
                        return this.handleJetStreamMessage(message, handler);
                    }
                });
                try {
                    yield client.subscribe(pattern, consumerOptions);
                    this.logger.log(`Subscribed to ${pattern} events`);
                }
                catch (error) {
                    if (error.message === "no stream matches subject") {
                        throw new Error(`Cannot find stream with the ${pattern} event pattern`);
                    }
                    throw error;
                }
            }
        });
    }
    subscribeToMessagePatterns(connection) {
        const messageHandlers = [...this.messageHandlers.entries()].filter(([, handler]) => !handler.isEventHandler);
        for (const [pattern, handler] of messageHandlers) {
            connection.subscribe(pattern, {
                callback: (error, message) => {
                    if (error) {
                        return this.logger.error(error.message, error.stack);
                    }
                    return this.handleNatsMessage(message, handler);
                },
                queue: this.options.queue
            });
            this.logger.log(`Subscribed to ${pattern} messages`);
        }
    }
    /**
     * Creates a new stream if it doesn't exist, otherwise updates the existing stream
     */
    upsertStream(manager, config) {
        return tslib_1.__awaiter(this, void 0, void 0, function* () {
            try {
                const stream = yield manager.streams.info(config.name);
                const updated = yield manager.streams.update(config.name, Object.assign(Object.assign({}, stream.config), config));
                return updated;
            }
            catch (error) {
                if (error.message === "stream not found") {
                    const added = yield manager.streams.add(config);
                    return added;
                }
                throw error;
            }
        });
    }
}
exports.NatsTransportStrategy = NatsTransportStrategy;
//# sourceMappingURL=nats.strategy.js.map