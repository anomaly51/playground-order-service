import cors from "@fastify/cors";
import {
  CreateOrderRequestSchema,
  CreateOrderResponseSchema,
  HTTP_LIMITS,
  InventoryReservationCommandSchema,
  LAB_SCENARIOS,
  OPENAPI_SCHEMAS,
  OrderEventEnvelopeSchema,
  type CreateOrderResponse,
  type LabScenario,
  type Order,
} from "./contracts.js";
import Fastify, { type FastifyInstance } from "fastify";
import { ulid } from "ulid";
import { ZodError } from "zod";
import type { GatewayConfig } from "./config.js";
import {
  fingerprint,
  type GatewayIdempotencyCache,
  type IdempotencyBackend,
  IdempotencyConflictError,
  IdempotencyInProgressError,
  IdempotencyPreviousAttemptFailedError,
  type RateLimitDecision,
} from "./idempotency.js";
import type { GatewayMessaging } from "./messaging.js";
import type { GatewayMetrics } from "./metrics.js";
import type { ProcessorClient } from "./processor.js";
import { parseInventoryResetRequest, type InventoryClient } from "./inventory.js";
import {
  hashExternalKey,
  PersistentIdempotencyConflictError,
  type GatewayStorage,
  type RuntimeSnapshot,
} from "./storage.js";
import { runTraced, TraceEmitter } from "./trace.js";

export interface GatewayDependencies {
  config: GatewayConfig;
  messaging: GatewayMessaging;
  processor: ProcessorClient;
  storage: GatewayStorage;
  idempotency: GatewayIdempotencyCache;
  metrics: GatewayMetrics;
  inventory: InventoryClient;
}

class RateLimitExceededError extends Error {
  constructor(readonly decision: RateLimitDecision) {
    super("FlashDrop request rate exceeded");
    this.name = "RateLimitExceededError";
  }
}

function isUlid(value: string): boolean {
  return /^[0-9A-HJKMNP-TV-Z]{26}$/i.test(value);
}

function idempotencyKey(headers: Record<string, unknown>): string {
  const value = headers["idempotency-key"];
  if (typeof value !== "string" || value.length < 8 || value.length > 128) {
    const error = new Error("Idempotency-Key must contain between 8 and 128 characters");
    Object.assign(error, { statusCode: 400 });
    throw error;
  }
  return value;
}

function labScenario(headers: Record<string, unknown>, enabled: boolean): LabScenario {
  const raw = headers["x-lab-scenario"];
  if (raw === undefined || raw === "") return "normal";
  if (typeof raw !== "string" || !(LAB_SCENARIOS as readonly string[]).includes(raw)) {
    const error = new Error(`X-Lab-Scenario must be one of: ${LAB_SCENARIOS.join(", ")}`);
    Object.assign(error, { statusCode: 400 });
    throw error;
  }
  return enabled ? raw as LabScenario : "normal";
}

function requestRunId(headers: Record<string, unknown>): string | undefined {
  const value = headers["x-run-id"];
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length < 1 || value.length > 128) {
    const error = new Error("X-Run-Id must contain between 1 and 128 characters");
    Object.assign(error, { statusCode: 400 });
    throw error;
  }
  return value;
}

function responseFor(order: Order): CreateOrderResponse {
  return CreateOrderResponseSchema.parse({
    orderId: order.orderId,
    traceId: order.traceId,
    status: "pending",
    quote: order.quote,
    acceptedAt: order.createdAt,
    links: { self: `/api/v1/orders/${order.orderId}` },
  });
}

export async function createApp(dependencies: GatewayDependencies): Promise<FastifyInstance> {
  const { config, messaging, processor, storage, idempotency, metrics, inventory } = dependencies;
  const app = Fastify({
    logger: {
      level: config.logLevel,
      redact: ["req.headers.authorization", "req.headers.cookie", "req.body.customerId"],
    },
    bodyLimit: HTTP_LIMITS.orderRequestBodyBytes,
    requestTimeout: config.requestTimeoutMs,
    connectionTimeout: 10_000,
    keepAliveTimeout: 72_000,
    trustProxy: true,
    genReqId: (request) => {
      const correlation = request.headers["x-correlation-id"];
      return typeof correlation === "string" && isUlid(correlation) ? correlation : ulid();
    },
  });
  const traceEmitter = new TraceEmitter(
    { publishTrace: (event) => storage.saveTrace(event) },
    app.log,
  );
  const activeOrderRequests = new Set<string>();

  const checkOperationsDependency = async (
    dependency: string,
    check: () => Promise<boolean>,
  ): Promise<boolean> => {
    try {
      return await check();
    } catch (error) {
      app.log.warn({ err: error, dependency }, "Operations dependency check failed");
      return false;
    }
  };
  const readOperationsRuntime = async (): Promise<RuntimeSnapshot | null> => {
    try {
      return await storage.getRuntime();
    } catch (error) {
      app.log.warn({ err: error }, "PostgreSQL runtime snapshot failed");
      return null;
    }
  };
  const collectOperationsRuntime = async (): Promise<{
    snapshot: RuntimeSnapshot | null;
    processorReady: boolean;
    postgresReady: boolean;
    redisReady: boolean;
  }> => {
    const [processorReady, storageReady, redisReady, snapshot] = await Promise.all([
      checkOperationsDependency(
        "pricing",
        () => processor.isReady(config.readinessTimeoutMs),
      ),
      checkOperationsDependency(
        "postgresql",
        () => storage.isReady(config.readinessTimeoutMs),
      ),
      checkOperationsDependency(
        "redis",
        () => idempotency.isReady(config.readinessTimeoutMs),
      ),
      readOperationsRuntime(),
    ]);
    return {
      snapshot,
      processorReady,
      postgresReady: storageReady && snapshot !== null,
      redisReady,
    };
  };

  app.addHook("onRequest", async (request) => {
    if (request.method === "POST" && request.url.split("?", 1)[0] === "/api/v1/orders") {
      activeOrderRequests.add(request.id);
    }
  });
  const releaseOrderRequest = async (request: { id: string }) => {
    activeOrderRequests.delete(request.id);
  };
  app.addHook("onResponse", releaseOrderRequest);
  app.addHook("onError", releaseOrderRequest);

  await app.register(cors, {
    origin: config.corsOrigins.includes("*") ? true : config.corsOrigins,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: [
      "content-type", "x-correlation-id", "idempotency-key",
      "x-lab-source", "x-lab-scenario", "x-run-id",
    ],
    exposedHeaders: ["x-correlation-id", "x-ratelimit-remaining", "retry-after"],
    maxAge: 86_400,
  });

  app.get("/healthz", async () => ({ status: "ok", service: "flashdrop-order-service" }));
  app.get("/readyz", async (_request, reply) => {
    const [processorReady, postgresReady, redisReady] = await Promise.all([
      processor.isReady(config.readinessTimeoutMs),
      storage.isReady(config.readinessTimeoutMs),
      idempotency.isReady(config.readinessTimeoutMs),
    ]);
    // Brokers are deliberately not on the HTTP acceptance critical path: the
    // transactional outbox relay catches up after either broker recovers.
    const ready = processorReady && postgresReady;
    return reply.code(ready ? 200 : 503).send({
      status: ready ? "ready" : "not-ready",
      dependencies: {
        brokers: messaging.isReady(),
        processor: processorReady,
        postgres: postgresReady,
        redis: redisReady,
      },
    });
  });
  app.get("/metrics", async (_request, reply) => {
    reply.header("content-type", metrics.registry.contentType);
    return metrics.registry.metrics();
  });
  app.get("/api/v1/runtime", async () => {
    const { processorReady, postgresReady, redisReady, snapshot } =
      await collectOperationsRuntime();
    return {
      service: "flashdrop-order-service",
      labScenariosEnabled: config.labScenariosEnabled,
      rateLimit: { requests: config.rateLimitRequests, windowMs: config.rateLimitWindowMs },
      dependencies: {
        brokers: messaging.isReady(),
        processor: processorReady,
        postgres: postgresReady,
        redis: redisReady,
      },
      storage: snapshot ?? {
        orders: { pending: null, confirmed: null, sold_out: null, failed: null },
        outbox: { pending: null, retrying: null, published: null },
        traces: null,
      },
    };
  });
  app.get("/operations/snapshot", async () => {
    const { snapshot, postgresReady, processorReady, redisReady } =
      await collectOperationsRuntime();
    const accepted = snapshot
      ? Object.values(snapshot.orders).reduce((total, count) => total + count, 0)
      : null;
    const finalized = snapshot
      ? snapshot.orders.confirmed + snapshot.orders.sold_out + snapshot.orders.failed
      : null;
    return {
      nodes: {
        "order-api": {
          inFlight: activeOrderRequests.size,
          total: accepted,
          healthy: postgresReady && processorReady,
        },
        redis: {
          inFlight: null,
          total: null,
          healthy: redisReady,
        },
        pricing: {
          inFlight: null,
          total: null,
          healthy: processorReady,
        },
        "outbox-relay": {
          inFlight: null,
          total: snapshot?.outbox.published ?? null,
          // The API process cannot prove that the separate relay process is alive.
          healthy: null,
        },
        "order-finalizer": {
          inFlight: null,
          total: finalized,
          // Finalizer liveness belongs to the relay process, not this sampler.
          healthy: null,
        },
        postgresql: {
          inFlight: null,
          total: accepted,
          healthy: postgresReady,
        },
      },
      postgres: { pendingOutbox: snapshot?.outbox.pending ?? null },
      outbox: snapshot?.outbox ?? { pending: null, retrying: null, published: null },
      orders: snapshot?.orders
        ?? { pending: null, confirmed: null, sold_out: null, failed: null },
    };
  });
  app.get("/openapi.json", async () => ({
    openapi: "3.1.0",
    info: { title: "FlashDrop Order API", version: "1.0.0" },
    paths: {
      "/api/v1/orders": {
        post: {
          operationId: "createFlashDropOrder",
          parameters: [
            { name: "Idempotency-Key", in: "header", required: true, schema: { type: "string", minLength: 8, maxLength: 128 } },
            { name: "X-Lab-Scenario", in: "header", required: false, schema: { type: "string", enum: LAB_SCENARIOS } },
            { name: "X-Run-Id", in: "header", required: false, schema: { type: "string", minLength: 1, maxLength: 128 } },
          ],
          requestBody: {
            required: true,
            content: { "application/json": { schema: OPENAPI_SCHEMAS.CreateOrderRequest } },
          },
          responses: {
            "202": {
              description: "Order durably accepted; inventory reservation continues asynchronously",
              content: { "application/json": { schema: OPENAPI_SCHEMAS.CreateOrderResponse } },
            },
          },
        },
      },
      "/api/v1/orders/{orderId}": {
        get: {
          operationId: "getFlashDropOrder",
          parameters: [{ name: "orderId", in: "path", required: true, schema: { type: "string" } }],
          responses: {
            "200": { description: "Current durable order state", content: { "application/json": { schema: OPENAPI_SCHEMAS.Order } } },
            "404": { description: "Order not found" },
          },
        },
      },
      "/api/v1/inventory": {
        get: {
          operationId: "listFlashDropInventory",
          responses: { "200": { description: "Real MySQL inventory", content: { "application/json": { schema: OPENAPI_SCHEMAS.InventoryListResponse } } } },
        },
      },
      "/api/v1/lab/inventory/reset": {
        post: {
          operationId: "resetFlashDropInventory",
          requestBody: { required: false, content: { "application/json": { schema: OPENAPI_SCHEMAS.InventoryResetRequest } } },
          responses: { "200": { description: "Reset MySQL inventory", content: { "application/json": { schema: OPENAPI_SCHEMAS.InventoryListResponse } } } },
        },
      },
    },
  }));

  app.get("/api/v1/inventory", async (request) => {
    const runId = requestRunId(request.headers);
    return runTraced({
      emitter: traceEmitter,
      traceId: request.id,
      runId,
      source: "traffic-mfe",
      target: "gateway",
      transport: "http",
      stage: "inventory.read.request",
      summary: "Traffic MFE requested the inventory snapshot",
      action: () => runTraced({
        emitter: traceEmitter,
        traceId: request.id,
        runId,
        source: "gateway",
        target: "inventory-worker",
        transport: "http",
        stage: "inventory.read",
        summary: "Gateway requested the inventory snapshot",
        action: () => inventory.list(),
      }),
    });
  });

  app.post("/api/v1/lab/inventory/reset", async (request, reply) => {
    if (!config.labScenariosEnabled) {
      const error = new Error("Lab controls are disabled");
      Object.assign(error, { statusCode: 403 });
      throw error;
    }
    const runId = requestRunId(request.headers);
    const reset = parseInventoryResetRequest(request.body);
    const result = await runTraced({
      emitter: traceEmitter,
      traceId: request.id,
      runId,
      source: "traffic-mfe",
      target: "gateway",
      transport: "http",
      stage: "inventory.reset.request",
      summary: "Traffic MFE requested an inventory reset",
      action: () => runTraced({
        emitter: traceEmitter,
        traceId: request.id,
        runId,
        source: "gateway",
        target: "inventory-worker",
        transport: "http",
        stage: "inventory.reset",
        summary: "Gateway requested the inventory reset",
        action: () => inventory.reset(reset),
      }),
    });
    return reply.send(result);
  });

  app.get<{ Params: { orderId: string } }>("/api/v1/orders/:orderId", async (request, reply) => {
    if (!isUlid(request.params.orderId)) {
      return reply.code(400).send({ error: "invalid_order_id", message: "orderId must be a ULID", traceId: request.id });
    }
    const order = await storage.getOrder(request.params.orderId);
    if (!order) {
      return reply.code(404).send({ error: "order_not_found", message: "FlashDrop order was not found", traceId: request.id });
    }
    return reply.send(order);
  });

  app.post("/api/v1/orders", async (request, reply) => {
    const parsed = CreateOrderRequestSchema.parse(request.body);
    const key = idempotencyKey(request.headers);
    const scenario = labScenario(request.headers, config.labScenariosEnabled);
    const runId = requestRunId(request.headers);
    const requestHash = fingerprint({ ...parsed, scenario });
    let ownsReservation = false;
    let reservationHeartbeat: NodeJS.Timeout | undefined;
    try {
      const reservation = await tracedCacheAction({
        emitter: traceEmitter,
        metrics,
        traceId: request.id,
        runId,
        stage: "idempotency.reserve",
        summary: "Redis idempotency reservation",
        action: () => idempotency.reserve(key, requestHash, request.id),
        payload: (result) => ({
          backend: result.backend,
          fallback: result.backend === "memory",
          hit: result.kind === "cached",
          state: result.kind,
        }),
      });
      if (reservation.kind === "cached") {
        metrics.idempotencyHits.inc({ backend: reservation.backend });
        metrics.requests.inc({ sku: parsed.sku, scenario, outcome: "idempotent" });
        reply.header("x-correlation-id", reservation.response.traceId);
        return reply.code(202).send(reservation.response);
      }
      ownsReservation = true;
      reservationHeartbeat = setInterval(() => {
        void idempotency.renew(key, requestHash, request.id).catch((error) => {
          request.log.warn({ err: error }, "idempotency reservation renewal failed");
        });
      }, Math.max(250, Math.floor(config.idempotencyInFlightTtlMs / 3)));
      reservationHeartbeat.unref();

      const rate = await tracedCacheAction({
        emitter: traceEmitter,
        metrics,
        traceId: request.id,
        runId,
        stage: "rate_limit.check",
        summary: "Redis customer rate-limit check",
        action: () => idempotency.consumeRateLimit(parsed.customerId),
        payload: (result) => ({
          backend: result.backend,
          fallback: result.backend === "memory",
          allowed: result.allowed,
          remaining: result.remaining,
        }),
      });
      reply.header("x-ratelimit-remaining", String(rate.remaining));
      if (!rate.allowed) throw new RateLimitExceededError(rate);

      const traceId = request.id;
      const acceptedAt = new Date().toISOString();
      const submittedByAirflow = request.headers["x-lab-source"] === "airflow";
      reply.header("x-correlation-id", traceId);
      await traceEmitter.emit({
        traceId,
        source: submittedByAirflow ? "airflow" : "traffic-mfe",
        target: "gateway",
        transport: submittedByAirflow ? "airflow" : "http",
        stage: submittedByAirflow ? "load.order.requested" : "order.requested",
        status: "succeeded",
        summary: submittedByAirflow ? "Airflow load order request received" : "FlashDrop HTTP order request received",
        ...(runId ? { runId } : {}),
        payload: { sku: parsed.sku, quantity: parsed.quantity, scenario },
      });

      const quote = await measuredAction(metrics, "http.quote", () =>
        runTraced({
          emitter: traceEmitter,
          traceId,
          runId,
          source: "gateway",
          target: "pricing-service",
          transport: "http",
          stage: "pricing.quote",
          summary: "Synchronous price quote",
          payload: { sku: parsed.sku, quantity: parsed.quantity },
          action: () => processor.quote({ traceId, order: parsed, scenario, runId }),
        }),
      );

      const orderId = ulid();
      const event = OrderEventEnvelopeSchema.parse({
        schemaVersion: 1,
        eventId: ulid(),
        eventType: "order.created",
        occurredAt: acceptedAt,
        traceId,
        orderId,
        ...(runId ? { runId } : {}),
        aggregateVersion: 1,
        data: {
          sku: parsed.sku,
          quantity: parsed.quantity,
          currency: quote.currency,
          totalCents: quote.totalCents,
          status: "pending",
        },
      });
      const command = InventoryReservationCommandSchema.parse({
        schemaVersion: 1,
        commandId: ulid(),
        traceId,
        orderId,
        ...(runId ? { runId } : {}),
        sku: parsed.sku,
        quantity: parsed.quantity,
        scenario,
        createdAt: acceptedAt,
      });
      const stored = await measuredAction(metrics, "postgres.order", () =>
        runTraced({
          emitter: traceEmitter,
          traceId,
          runId,
          orderId,
          source: "gateway",
          target: "postgresql",
          transport: "postgresql",
          stage: "order.persist",
          summary: "PostgreSQL order and transactional outbox commit",
          payload: { orderId, sku: parsed.sku },
          action: () => storage.createOrder({
            orderId,
            traceId,
            idempotencyKeyHash: hashExternalKey(key),
            requestHash,
            request: parsed,
            quote,
            scenario,
            event,
            command,
            createdAt: acceptedAt,
          }),
        }),
      );
      const response = responseFor(stored.order);
      await tracedCacheAction({
        emitter: traceEmitter,
        metrics,
        traceId,
        runId,
        stage: "idempotency.complete",
        summary: "Redis idempotency completion",
        action: () => idempotency.complete(key, requestHash, request.id, response),
        payload: (result) => ({ backend: result.backend, fallback: result.backend === "memory" }),
      });
      ownsReservation = false;
      metrics.requests.inc({ sku: parsed.sku, scenario, outcome: stored.created ? "accepted" : "persistent_idempotent" });
      return reply.code(202).send(response);
    } catch (error) {
      if (ownsReservation) {
        await idempotency.release(key, requestHash, request.id).catch((cleanupError) => {
          request.log.error({ err: cleanupError }, "idempotency reservation cleanup failed");
        });
      }
      throw error;
    } finally {
      if (reservationHeartbeat) clearInterval(reservationHeartbeat);
    }
  });

  app.setErrorHandler((error, request, reply) => {
    const traceId = request.id;
    reply.header("x-correlation-id", traceId);
    if (error instanceof ZodError) {
      return reply.code(400).send({
        error: "validation_error",
        message: "Request validation failed",
        traceId,
        details: error.issues.map((issue) => ({ path: issue.path.join("."), message: issue.message })),
      });
    }
    if (error instanceof IdempotencyConflictError || error instanceof PersistentIdempotencyConflictError) {
      return reply.code(409).send({ error: "idempotency_conflict", message: error.message, traceId });
    }
    if (error instanceof IdempotencyInProgressError) {
      reply.header("retry-after", "1");
      return reply.code(409).send({ error: "idempotency_in_progress", message: error.message, traceId });
    }
    if (error instanceof IdempotencyPreviousAttemptFailedError) {
      return reply.code(409).send({ error: "idempotency_previous_attempt_failed", message: error.message, traceId });
    }
    if (error instanceof RateLimitExceededError) {
      reply.header("retry-after", String(Math.max(1, Math.ceil(error.decision.retryAfterMs / 1_000))));
      return reply.code(429).send({ error: "rate_limited", message: error.message, traceId });
    }
    const normalizedError = error instanceof Error ? error : new Error(String(error));
    const candidateStatus = typeof error === "object" && error !== null && "statusCode" in error
      ? (error as { statusCode?: unknown }).statusCode : undefined;
    const statusCode = typeof candidateStatus === "number" && candidateStatus < 500 ? candidateStatus : 503;
    request.log.error({ err: error, traceId }, "request failed");
    return reply.code(statusCode).send({
      error: statusCode === 503 ? "dependency_unavailable" : "request_error",
      message: statusCode === 503 ? "A downstream dependency is unavailable" : normalizedError.message,
      traceId,
    });
  });

  return app;
}

async function tracedCacheAction<T extends { backend: IdempotencyBackend }>(options: {
  emitter: TraceEmitter;
  metrics: GatewayMetrics;
  traceId: string;
  runId?: string;
  orderId?: string;
  stage: "idempotency.reserve" | "idempotency.complete" | "rate_limit.check";
  summary: string;
  action: () => Promise<T>;
  payload: (result: T) => Record<string, unknown>;
}): Promise<T> {
  await options.emitter.emit({
    traceId: options.traceId, source: "gateway", target: "redis", transport: "redis",
    stage: options.stage, status: "started", summary: `${options.summary} started`,
    ...(options.runId ? { runId: options.runId } : {}),
    ...(options.orderId ? { orderId: options.orderId } : {}),
  });
  try {
    const result = await options.action();
    if (result.backend === "memory") options.metrics.idempotencyFallbacks.inc({ action: options.stage });
    await options.emitter.emit({
      traceId: options.traceId, source: "gateway", target: "redis", transport: "redis",
      stage: options.stage, status: result.backend === "memory" ? "failed" : "succeeded",
      summary: result.backend === "memory" ? `${options.summary} used local fallback` : `${options.summary} succeeded`,
      ...(options.runId ? { runId: options.runId } : {}),
      ...(options.orderId ? { orderId: options.orderId } : {}),
      payload: options.payload(result),
    });
    return result;
  } catch (error) {
    await options.emitter.emit({
      traceId: options.traceId, source: "gateway", target: "redis", transport: "redis",
      stage: options.stage, status: "failed", summary: `${options.summary} failed`,
      ...(options.runId ? { runId: options.runId } : {}),
      ...(options.orderId ? { orderId: options.orderId } : {}),
      payload: { error: error instanceof Error ? error.message : String(error) },
    });
    throw error;
  }
}

async function measuredAction<T>(metrics: GatewayMetrics, action: string, operation: () => Promise<T>): Promise<T> {
  const end = metrics.actionDuration.startTimer();
  try {
    const result = await operation();
    end({ action, outcome: "succeeded" });
    return result;
  } catch (error) {
    end({ action, outcome: "failed" });
    throw error;
  }
}
