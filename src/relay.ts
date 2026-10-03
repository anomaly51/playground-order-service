import {
  KAFKA_TOPICS,
  TraceEventSchema,
  type InventoryReservationResult,
} from "./contracts.js";
import Fastify, { type FastifyInstance } from "fastify";
import pino from "pino";
import { ulid } from "ulid";
import { loadConfig } from "./config.js";
import { BrokerMessaging } from "./messaging.js";
import { GatewayMetrics } from "./metrics.js";
import { runTransportLanes } from "./relay-batch.js";
import { PostgresGatewayStorage } from "./storage.js";

const config = loadConfig();
const logger = pino({ name: "flashdrop-outbox-relay", level: config.logLevel });
const metrics = new GatewayMetrics();
const storage = new PostgresGatewayStorage(config, { logger });
const messaging = new BrokerMessaging(config, logger, metrics);
let stopping = false;
let operationsServer: FastifyInstance | undefined;
let relayInFlight = 0;
let relayTotal = 0;
let finalizerInFlight = 0;
let finalizerTotal = 0;

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function handleInventoryResult(
  result: InventoryReservationResult,
): Promise<void> {
  finalizerInFlight += 1;
  try {
    await storage.saveTrace(TraceEventSchema.parse({
    // Reusing the result ULID makes this receipt trace idempotent on redelivery.
    id: result.resultId,
    traceId: result.traceId,
    orderId: result.orderId,
    ...(result.runId ? { runId: result.runId } : {}),
    correlationId: result.commandId,
    causationId: result.resultId,
    timestamp: new Date().toISOString(),
    source: "rabbitmq",
    target: "order-finalizer",
    stage: "inventory.result.received",
    status: "succeeded",
    transport: "rabbitmq",
    summary: `Order finalizer received inventory result: ${result.status}`,
    payload: {
      resultId: result.resultId,
      sku: result.sku,
      quantity: result.quantity,
      inventoryStatus: result.status,
    },
    }));

    const applied = await storage.applyInventoryResult(result);
    await storage.saveTrace(TraceEventSchema.parse({
    id: ulid(),
    traceId: result.traceId,
    orderId: result.orderId,
    ...(result.runId ? { runId: result.runId } : {}),
    correlationId: result.commandId,
    causationId: result.resultId,
    timestamp: new Date().toISOString(),
    source: "order-finalizer",
    target: "postgresql",
    stage: "order.status.persist",
    status: "succeeded",
    transport: "postgresql",
    summary: applied.duplicate
      ? "Inventory result was already applied"
      : `Order status changed to ${applied.order?.status ?? result.status}`,
    payload: { resultId: result.resultId, duplicate: applied.duplicate },
    }));
    finalizerTotal += 1;
  } finally {
    finalizerInFlight = Math.max(0, finalizerInFlight - 1);
  }
}

async function publishBatch(): Promise<number> {
  const records = await storage.claimOutbox(config.outboxBatchSize);
  await runTransportLanes(records, async (record) => {
    if (record.destination !== KAFKA_TOPICS.traces) {
      const runId = typeof record.payload === "object" && record.payload !== null
        && "runId" in record.payload && typeof record.payload.runId === "string"
        ? record.payload.runId : undefined;
      await storage.saveTrace(TraceEventSchema.parse({
        id: ulid(),
        traceId: record.traceId,
        orderId: record.aggregateId,
        ...(runId ? { runId } : {}),
        correlationId: record.outboxId,
        timestamp: new Date().toISOString(),
        source: "postgresql",
        target: "outbox-relay",
        stage: "outbox.claim",
        status: "succeeded",
        transport: "postgresql",
        summary: `Outbox relay claimed ${record.eventType}`,
        payload: {
          outboxId: record.outboxId,
          eventType: record.eventType,
          transport: record.transport,
          attempt: record.attempts,
        },
      })).catch((error) => {
        logger.warn({ err: error, outboxId: record.outboxId }, "Outbox claim trace unavailable");
      });
    }
    relayInFlight += 1;
    try {
      await messaging.publishOutbox(record);
      await storage.markOutboxPublished(record.outboxId);
      relayTotal += 1;
      if (record.destination !== KAFKA_TOPICS.traces) {
        const runId = typeof record.payload === "object" && record.payload !== null
          && "runId" in record.payload && typeof record.payload.runId === "string"
          ? record.payload.runId : undefined;
        await storage.saveTrace(TraceEventSchema.parse({
          id: ulid(),
          traceId: record.traceId,
          orderId: record.aggregateId,
          ...(runId ? { runId } : {}),
          correlationId: record.outboxId,
          timestamp: new Date().toISOString(),
          source: "order-service-relay",
          target: record.transport === "kafka" ? "kafka" : "rabbitmq",
          stage: "outbox.publish",
          status: "succeeded",
          transport: record.transport,
          summary: `Published ${record.eventType} from the PostgreSQL outbox`,
          payload: {
            outboxId: record.outboxId,
            eventType: record.eventType,
            destination: record.destination,
            attempt: record.attempts,
          },
        }));
      }
    } catch (error) {
      const retryInMs = Math.min(
        250 * 2 ** Math.min(Math.max(0, record.attempts - 1), 10),
        config.outboxRetryMaxMs,
      );
      await storage.markOutboxFailed(
        record.outboxId,
        error instanceof Error ? error.message : String(error),
        retryInMs,
      );
      logger.warn(
        { err: error, outboxId: record.outboxId, retryInMs },
        "Outbox publication failed",
      );
    } finally {
      relayInFlight = Math.max(0, relayInFlight - 1);
    }
  });
  return records.length;
}

async function startOperationsServer(): Promise<void> {
  const server = Fastify({ logger: false });
  server.get("/healthz", async () => ({ status: stopping ? "stopping" : "ok" }));
  server.get("/readyz", async (_request, reply) => {
    const postgresReady = await storage.isReady(config.readinessTimeoutMs);
    return reply.code(postgresReady && !stopping ? 200 : 503).send({
      status: postgresReady && !stopping ? "ready" : "not-ready",
      dependencies: { postgres: postgresReady },
    });
  });
  server.get("/operations/snapshot", async () => {
    const postgresReady = await storage.isReady(config.readinessTimeoutMs);
    return {
      nodes: {
        "outbox-relay": {
          inFlight: relayInFlight,
          total: relayTotal,
          healthy: postgresReady && !stopping,
        },
        "order-finalizer": {
          inFlight: finalizerInFlight,
          total: finalizerTotal,
          healthy: postgresReady && messaging.isRabbitReady() && !stopping,
        },
      },
    };
  });
  await server.listen({
    host: config.relayOperationsHost,
    port: config.relayOperationsPort,
  });
  operationsServer = server;
}

async function run(): Promise<void> {
  await storage.start();
  await startOperationsServer();
  logger.info("FlashDrop transactional outbox relay started");
  void messaging.startKafka().then(
    () => logger.info("Kafka outbox transport connected"),
    (error) => logger.warn({ err: error }, "Kafka unavailable; Kafka rows will retry independently"),
  );
  void messaging.consumeInventoryResults(handleInventoryResult).then(
    () => logger.info("RabbitMQ inventory-result consumer connected"),
    (error) => logger.warn({ err: error }, "RabbitMQ unavailable; reconnect scheduled independently"),
  );
  while (!stopping) {
    try {
      const published = await publishBatch();
      if (published === 0) await sleep(config.outboxPollMs);
    } catch (error) {
      logger.error({ err: error }, "Outbox poll failed");
      await sleep(1_000);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  if (stopping) return;
  stopping = true;
  logger.info({ signal }, "FlashDrop outbox relay stopping");
  await operationsServer?.close().catch(() => undefined);
  await messaging.stop();
  await storage.stop();
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

await run();
