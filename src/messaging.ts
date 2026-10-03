import amqp, {
  type ChannelModel,
  type ConfirmChannel,
  type Options,
} from "amqplib";
import {
  InventoryReservationResultSchema,
  KAFKA_TOPICS,
  RABBITMQ,
  type InventoryReservationResult,
  type MessageEnvelope,
  type TraceEvent,
} from "./contracts.js";
import {
  Kafka,
  Partitioners,
  logLevel as kafkaLogLevel,
  type Producer,
  type SASLOptions,
} from "kafkajs";
import type { FastifyBaseLogger } from "fastify";
import type { GatewayConfig } from "./config.js";
import type { GatewayMetrics } from "./metrics.js";
import type { TraceSink } from "./trace.js";
import type { OutboxRecord } from "./storage.js";

export interface GatewayMessaging extends TraceSink {
  start(): Promise<void>;
  stop(): Promise<void>;
  isReady(): boolean;
  publishKafkaMessage(message: MessageEnvelope): Promise<void>;
  publishRabbitCommand(message: MessageEnvelope): Promise<void>;
  publishOutbox(record: OutboxRecord): Promise<void>;
  consumeInventoryResults(
    handler: (result: InventoryReservationResult) => Promise<void>,
  ): Promise<void>;
}

function rabbitConnectionOptions(connectionUrl: string): Options.Connect {
  const url = new URL(connectionUrl);
  if (url.protocol !== "amqp:" && url.protocol !== "amqps:") {
    throw new Error("RABBITMQ_URL must use amqp:// or amqps://");
  }
  return {
    protocol: url.protocol.slice(0, -1),
    hostname: url.hostname,
    ...(url.port ? { port: Number(url.port) } : {}),
    ...(url.username || url.password
      ? {
          username: decodeURIComponent(url.username),
          password: decodeURIComponent(url.password),
        }
      : {}),
    vhost: url.pathname.length > 1
      ? decodeURIComponent(url.pathname.slice(1))
      : "/",
    frameMax: 131_072,
    heartbeat: 15,
  };
}

export class BrokerMessaging implements GatewayMessaging {
  private readonly producer: Producer;
  private rabbitConnection?: ChannelModel;
  private rabbitChannel?: ConfirmChannel;
  private kafkaReady = false;
  private rabbitReady = false;
  private closing = false;
  private rabbitReconnectTimer?: NodeJS.Timeout;
  private rabbitReconnectDelayMs = 1_000;
  private kafkaConnectPromise?: Promise<void>;
  private rabbitConnectPromise?: Promise<void>;
  private readonly returnedMessageIds = new Set<string>();
  private resultConsumerTag?: string;
  private resultConsumerChannel?: ConfirmChannel;
  private resultHandler?: (result: InventoryReservationResult) => Promise<void>;

  constructor(
    private readonly config: GatewayConfig,
    private readonly logger: FastifyBaseLogger,
    private readonly metrics: GatewayMetrics,
  ) {
    const kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      ssl: config.kafkaSsl,
      ...(config.kafkaSasl ? { sasl: config.kafkaSasl as SASLOptions } : {}),
      connectionTimeout: 5_000,
      requestTimeout: 10_000,
      retry: { retries: 8, initialRetryTime: 250, maxRetryTime: 5_000 },
      logLevel: kafkaLogLevel.NOTHING,
    });
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 5,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async start(): Promise<void> {
    const attempts = await Promise.allSettled([
      this.startKafka(),
      this.startRabbit(),
    ]);
    const failures = attempts
      .filter((attempt): attempt is PromiseRejectedResult => attempt.status === "rejected")
      .map((attempt) => attempt.reason);
    if (failures.length > 0) {
      throw new AggregateError(failures, "One or more outbox transports are unavailable");
    }
  }

  async startKafka(): Promise<void> {
    if (this.closing) throw new Error("Messaging is stopping");
    if (this.kafkaReady) return;
    if (this.kafkaConnectPromise) return this.kafkaConnectPromise;
    const operation = this.producer.connect()
      .then(() => {
        this.kafkaReady = true;
      })
      .catch((error) => {
        this.kafkaReady = false;
        throw error;
      })
      .finally(() => {
        if (this.kafkaConnectPromise === operation) this.kafkaConnectPromise = undefined;
      });
    this.kafkaConnectPromise = operation;
    return operation;
  }

  async startRabbit(): Promise<void> {
    if (this.closing) throw new Error("Messaging is stopping");
    if (this.rabbitReady) return;
    try {
      await this.connectRabbit();
    } catch (error) {
      this.rabbitReady = false;
      this.scheduleRabbitReconnect();
      throw error;
    }
  }

  private connectRabbit(): Promise<void> {
    if (this.rabbitReady) return Promise.resolve();
    if (this.rabbitConnectPromise) return this.rabbitConnectPromise;
    const operation = this.openRabbit().finally(() => {
      if (this.rabbitConnectPromise === operation) this.rabbitConnectPromise = undefined;
    });
    this.rabbitConnectPromise = operation;
    return operation;
  }

  private async openRabbit(): Promise<void> {
    const connection = await amqp.connect(rabbitConnectionOptions(this.config.rabbitUrl), {
      clientProperties: { connection_name: "flashdrop-order-service" },
      keepAlive: true,
      keepAliveDelay: 5_000,
    });
    let channel: ConfirmChannel | undefined;
    try {
      channel = await connection.createConfirmChannel();
      await channel.assertExchange(RABBITMQ.exchanges.commands, "direct", { durable: true });
      await channel.assertExchange(RABBITMQ.exchanges.results, "fanout", { durable: true });
      channel.on("return", (message) => {
        if (message.properties.messageId) {
          this.returnedMessageIds.add(message.properties.messageId);
        }
        this.logger.warn(
          { messageId: message.properties.messageId, replyText: message.fields.replyText },
          "RabbitMQ returned an unroutable command",
        );
      });
      this.rabbitConnection = connection;
      this.rabbitChannel = channel;
      this.rabbitReady = true;
      this.rabbitReconnectDelayMs = 1_000;
      if (this.rabbitReconnectTimer) {
        clearTimeout(this.rabbitReconnectTimer);
        this.rabbitReconnectTimer = undefined;
      }
      connection.on("error", (error) => {
        if (this.rabbitConnection === connection) this.rabbitReady = false;
        this.logger.error({ err: error }, "RabbitMQ connection error");
      });
      connection.on("close", () => {
        if (this.rabbitConnection !== connection) return;
        this.rabbitReady = false;
        this.rabbitConnection = undefined;
        this.rabbitChannel = undefined;
        this.resultConsumerTag = undefined;
        this.resultConsumerChannel = undefined;
        if (!this.closing) {
          this.logger.warn("RabbitMQ connection closed; reconnect scheduled");
          this.scheduleRabbitReconnect();
        }
      });
      if (this.resultHandler) await this.setupResultConsumer(this.resultHandler);
    } catch (error) {
      if (this.rabbitConnection === connection) {
        this.rabbitReady = false;
        this.rabbitConnection = undefined;
        this.rabbitChannel = undefined;
        this.resultConsumerTag = undefined;
        this.resultConsumerChannel = undefined;
      }
      await Promise.allSettled([channel?.close(), connection.close()]);
      throw error;
    }
  }

  private scheduleRabbitReconnect(): void {
    if (this.closing || this.rabbitReconnectTimer) return;
    const delay = this.rabbitReconnectDelayMs;
    this.rabbitReconnectDelayMs = Math.min(this.rabbitReconnectDelayMs * 2, 30_000);
    this.rabbitReconnectTimer = setTimeout(() => {
      this.rabbitReconnectTimer = undefined;
      void this.connectRabbit().catch((error) => {
        this.logger.error({ err: error, retryInMs: this.rabbitReconnectDelayMs }, "RabbitMQ reconnect failed");
        this.scheduleRabbitReconnect();
      });
    }, delay);
    this.rabbitReconnectTimer.unref();
  }

  isReady(): boolean {
    return this.kafkaReady && this.rabbitReady;
  }

  isKafkaReady(): boolean {
    return this.kafkaReady;
  }

  isRabbitReady(): boolean {
    return this.rabbitReady;
  }

  async publishTrace(event: TraceEvent): Promise<void> {
    await this.sendKafka(KAFKA_TOPICS.traces, event.traceId, event);
  }

  async publishKafkaMessage(message: MessageEnvelope): Promise<void> {
    await this.sendKafka(KAFKA_TOPICS.messages, message.traceId, message);
  }

  private async sendKafka(topic: string, key: string, value: unknown): Promise<void> {
    try {
      await this.startKafka();
      await this.producer.send({
        topic,
        acks: -1,
        messages: [
          {
            key,
            value: JSON.stringify(value),
            headers: { "content-type": "application/json", "schema-version": "1" },
          },
        ],
      });
      this.kafkaReady = true;
    } catch (error) {
      this.kafkaReady = false;
      this.metrics.publishFailures.inc({ transport: "kafka" });
      throw error;
    }
  }

  async publishRabbitCommand(message: MessageEnvelope): Promise<void> {
    await this.startRabbit();
    const channel = this.rabbitChannel;
    if (!channel || !this.rabbitReady) throw new Error("RabbitMQ channel is not ready");
    try {
      const accepted = channel.publish(
        RABBITMQ.exchanges.commands,
        RABBITMQ.routingKeys.process,
        Buffer.from(JSON.stringify(message)),
        {
          persistent: true,
          mandatory: true,
          contentType: "application/json",
          contentEncoding: "utf-8",
          messageId: message.messageId,
          correlationId: message.traceId,
          timestamp: Date.now(),
          type: "lab.message.command.v1",
          headers: { "x-attempt": 0, "x-schema-version": 1 },
        },
      );
      if (!accepted) await new Promise<void>((resolve) => channel.once("drain", resolve));
      await channel.waitForConfirms();
      if (this.returnedMessageIds.delete(message.messageId)) {
        throw new Error("RabbitMQ command was unroutable; rabbit-worker queue is not bound");
      }
    } catch (error) {
      this.metrics.publishFailures.inc({ transport: "rabbitmq" });
      throw error;
    }
  }

  async publishOutbox(record: OutboxRecord): Promise<void> {
    if (record.transport === "kafka") {
      await this.sendKafka(record.destination, record.partitionKey, record.payload);
      return;
    }
    await this.startRabbit();
    const channel = this.rabbitChannel;
    if (!channel || !this.rabbitReady) throw new Error("RabbitMQ channel is not ready");
    const accepted = channel.publish(
      record.destination,
      record.routingKey ?? "",
      Buffer.from(JSON.stringify(record.payload)),
      {
        persistent: true,
        mandatory: true,
        contentType: "application/json",
        contentEncoding: "utf-8",
        messageId: record.outboxId,
        correlationId: record.traceId,
        timestamp: Date.now(),
        type: record.eventType,
        headers: { "x-schema-version": 1, "x-outbox-id": record.outboxId },
      },
    );
    if (!accepted) await new Promise<void>((resolve) => channel.once("drain", resolve));
    await channel.waitForConfirms();
    if (this.returnedMessageIds.delete(record.outboxId)) {
      throw new Error(`RabbitMQ outbox item ${record.outboxId} was unroutable`);
    }
  }

  async consumeInventoryResults(
    handler: (result: InventoryReservationResult) => Promise<void>,
  ): Promise<void> {
    this.resultHandler = handler;
    await this.startRabbit();
    await this.setupResultConsumer(handler);
  }

  private async setupResultConsumer(
    handler: (result: InventoryReservationResult) => Promise<void>,
  ): Promise<void> {
    const channel = this.rabbitChannel;
    if (!channel || !this.rabbitReady) throw new Error("RabbitMQ channel is not ready");
    if (this.resultConsumerChannel === channel && this.resultConsumerTag) return;
    await channel.assertExchange(RABBITMQ.exchanges.results, "fanout", { durable: true });
    await channel.assertQueue(RABBITMQ.queues.orderResults, {
      durable: true,
      arguments: { "x-queue-type": "quorum" },
    });
    await channel.bindQueue(RABBITMQ.queues.orderResults, RABBITMQ.exchanges.results, "");
    await channel.prefetch(50);
    const consumer = await channel.consume(
      RABBITMQ.queues.orderResults,
      (message) => {
        if (!message) return;
        let result: InventoryReservationResult;
        try {
          result = InventoryReservationResultSchema.parse(
            JSON.parse(message.content.toString("utf8")),
          );
        } catch (error) {
          this.logger.warn({ err: error }, "Discarding invalid inventory result");
          channel.ack(message);
          return;
        }
        void handler(result).then(
          () => channel.ack(message),
          (error) => {
            this.logger.error(
              { err: error, orderId: result.orderId, resultId: result.resultId },
              "Inventory result application failed; requeueing",
            );
            channel.nack(message, false, true);
          },
        );
      },
      { noAck: false, consumerTag: "flashdrop-order-results" },
    );
    this.resultConsumerTag = consumer.consumerTag;
    this.resultConsumerChannel = channel;
  }

  async stop(): Promise<void> {
    this.closing = true;
    this.rabbitReady = false;
    this.kafkaReady = false;
    if (this.rabbitReconnectTimer) clearTimeout(this.rabbitReconnectTimer);
    if (this.rabbitChannel && this.resultConsumerTag) {
      await this.rabbitChannel.cancel(this.resultConsumerTag).catch(() => undefined);
    }
    await Promise.allSettled([
      this.rabbitChannel?.close(),
      this.rabbitConnection?.close(),
      this.producer.disconnect(),
    ]);
    this.rabbitChannel = undefined;
    this.rabbitConnection = undefined;
    this.resultConsumerTag = undefined;
    this.resultConsumerChannel = undefined;
  }
}
