import { OrderSchema, type Order, type TraceEvent } from "../src/contracts.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createApp } from "../src/app.js";
import type { GatewayConfig } from "../src/config.js";
import { IdempotencyStore, type GatewayIdempotencyCache } from "../src/idempotency.js";
import type { GatewayMessaging } from "../src/messaging.js";
import { GatewayMetrics } from "../src/metrics.js";
import type { ProcessorClient } from "../src/processor.js";
import type { CreateOrderInput, GatewayStorage } from "../src/storage.js";

const config: GatewayConfig = {
  nodeEnv: "test", logLevel: "silent", host: "127.0.0.1", port: 3_000,
  corsOrigins: ["*"], kafkaBrokers: ["unused:9092"], kafkaClientId: "test",
  kafkaSsl: false, rabbitUrl: "amqp://unused", redisUrl: "redis://unused:6379/0",
  redisConnectTimeoutMs: 100, redisCommandTimeoutMs: 50,
  redisKeyPrefix: "test:idempotency:", redisRateLimitPrefix: "test:rate-limit:",
  rateLimitRequests: 100, rateLimitWindowMs: 1_000,
  postgresUrl: "postgresql://unused:unused@unused:5432/unused",
  postgresPoolSize: 2, postgresConnectionTimeoutMs: 100,
  processorHttpUrl: "http://unused:8001",
  inventoryServiceUrl: "http://inventory-worker:3004", inventoryHttpTimeoutMs: 100,
  processorHttpTimeoutMs: 100, readinessTimeoutMs: 50, requestTimeoutMs: 1_000,
  idempotencyTtlMs: 10_000, idempotencyInFlightTtlMs: 1_000,
  idempotencyMaxEntries: 10, labScenariosEnabled: true,
  outboxPollMs: 100, outboxBatchSize: 100, outboxRetryMaxMs: 30_000,
  relayOperationsHost: "127.0.0.1", relayOperationsPort: 3_005,
};

function fakes() {
  const traces: TraceEvent[] = [];
  const created: CreateOrderInput[] = [];
  const orders = new Map<string, Order>();
  const messaging: GatewayMessaging = {
    start: vi.fn(), stop: vi.fn(), isReady: () => true,
    publishTrace: vi.fn(), publishKafkaMessage: vi.fn(), publishRabbitCommand: vi.fn(),
    publishOutbox: vi.fn(), consumeInventoryResults: vi.fn(),
  };
  const processor: ProcessorClient = {
    quote: vi.fn(async () => ({
      currency: "USD", unitPriceCents: 14_900, discountCents: 1_490,
      totalCents: 13_410, priceVersion: "drop-2026-08",
      quotedAt: "2026-08-30T10:00:00.000Z",
    })),
    isReady: vi.fn(async () => true),
    close: vi.fn(),
  };
  const inventoryItems = [{
    sku: "DROP-SNEAKER-RED" as const, available: 100, reserved: 0,
    updatedAt: "2026-08-30T10:00:00.000Z",
  }];
  const inventory = {
    list: vi.fn(async () => ({ items: inventoryItems })),
    reset: vi.fn(async () => ({ items: inventoryItems })),
  };
  const storage: GatewayStorage = {
    start: vi.fn(), stop: vi.fn(), isReady: vi.fn(async () => true),
    createOrder: vi.fn(async (input) => {
      created.push(input);
      const order = OrderSchema.parse({
        orderId: input.orderId, traceId: input.traceId,
        customerId: input.request.customerId, sku: input.request.sku,
        quantity: input.request.quantity,
        ...(input.request.couponCode ? { couponCode: input.request.couponCode } : {}),
        status: "pending", quote: input.quote,
        createdAt: input.createdAt, updatedAt: input.createdAt, version: 1,
      });
      orders.set(order.orderId, order);
      return { order, created: true };
    }),
    getOrder: async (orderId) => orders.get(orderId),
    getRuntime: async () => ({
      orders: { pending: orders.size, confirmed: 0, sold_out: 0, failed: 0 },
      outbox: { pending: created.length * 2, retrying: 0, published: 0 }, traces: traces.length,
    }),
    saveTrace: async (event) => { traces.push(event); },
    claimOutbox: async () => [], markOutboxPublished: vi.fn(), markOutboxFailed: vi.fn(),
    applyInventoryResult: async () => ({ duplicate: false }),
  };
  const local = new IdempotencyStore(
    config.idempotencyTtlMs, config.idempotencyMaxEntries, config.idempotencyInFlightTtlMs,
  );
  let rateAllowed = true;
  const idempotency: GatewayIdempotencyCache = {
    start: vi.fn(), stop: vi.fn(), isReady: vi.fn(async () => true),
    reserve: async (key, hash, owner) => {
      const result = local.reserve(key, hash, owner);
      return result.kind === "cached"
        ? { kind: "cached", backend: "redis", response: result.response }
        : { kind: "claimed", backend: "redis" };
    },
    complete: async (key, hash, owner, response) => {
      local.complete(key, hash, owner, response);
      return { backend: "redis" };
    },
    fail: async (key, hash, owner) => { local.fail(key, hash, owner); return { backend: "redis" }; },
    renew: async () => ({ backend: "redis" }),
    release: async (key, hash, owner) => { local.release(key, hash, owner); return { backend: "redis" }; },
    consumeRateLimit: async () => ({
      allowed: rateAllowed, backend: "redis", remaining: rateAllowed ? 99 : 0,
      retryAfterMs: rateAllowed ? 0 : 500,
    }),
  };
  return {
    messaging, processor, storage, idempotency, inventory, traces, created, orders,
    denyRate: () => { rateAllowed = false; },
  };
}

const apps: Awaited<ReturnType<typeof createApp>>[] = [];
afterEach(async () => Promise.all(apps.splice(0).map((app) => app.close())));

const request = {
  method: "POST" as const,
  url: "/api/v1/orders",
  headers: { "idempotency-key": "checkout-customer-42" },
  payload: {
    customerId: "customer-42", sku: "DROP-SNEAKER-RED",
    quantity: 1, couponCode: "DROP10",
  },
};

describe("FlashDrop Order API", () => {
  it("does not persist an order on pricing failure and allows retry with the same key", async () => {
    const fake = fakes();
    vi.mocked(fake.processor.quote).mockRejectedValueOnce(new Error("Pricing service returned HTTP 504"));
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const failed = await app.inject(request);
    expect(failed.statusCode).toBe(503);
    expect(failed.json().error).toBe("dependency_unavailable");
    expect(fake.created).toHaveLength(0);
    expect(fake.traces).toContainEqual(expect.objectContaining({
      transport: "http", stage: "pricing.quote", status: "failed",
    }));
    const retried = await app.inject(request);
    expect(retried.statusCode).toBe(202);
    expect(fake.created).toHaveLength(1);
  });

  it("durably accepts an order without publishing to a broker in the request", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject(request);
    expect(response.statusCode).toBe(202);
    expect(response.json()).toMatchObject({
      orderId: expect.stringMatching(/^[0-9A-HJKMNP-TV-Z]{26}$/),
      traceId: expect.stringMatching(/^[0-9A-HJKMNP-TV-Z]{26}$/),
      status: "pending", quote: { totalCents: 13_410 },
    });
    expect(fake.processor.quote).toHaveBeenCalledOnce();
    expect(fake.created).toHaveLength(1);
    expect(fake.messaging.publishKafkaMessage).not.toHaveBeenCalled();
    expect(fake.messaging.publishRabbitCommand).not.toHaveBeenCalled();
    expect(fake.created[0]?.event.eventType).toBe("order.created");
    expect(fake.created[0]?.command.scenario).toBe("normal");
  });

  it("returns current durable state through GET", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const accepted = await app.inject(request);
    const fetched = await app.inject({ method: "GET", url: accepted.json().links.self });
    expect(fetched.statusCode).toBe(200);
    expect(fetched.json()).toMatchObject({ orderId: accepted.json().orderId, status: "pending" });
  });

  it("deduplicates an identical idempotency key", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const first = await app.inject(request);
    const second = await app.inject(request);
    expect(second.statusCode).toBe(202);
    expect(second.json()).toEqual(first.json());
    expect(fake.created).toHaveLength(1);
  });

  it("passes a supported lab scenario and run id without putting customer data in traces", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject({
      ...request,
      headers: {
        ...request.headers,
        "x-lab-scenario": "inventory-retry",
        "x-run-id": "load-run-42",
      },
    });
    expect(response.statusCode).toBe(202);
    expect(fake.created[0]?.command.scenario).toBe("inventory-retry");
    expect(fake.traces.some((trace) => trace.runId === "load-run-42")).toBe(true);
    expect(JSON.stringify(fake.traces)).not.toContain("customer-42");
  });

  it("rate limits before pricing or PostgreSQL side effects", async () => {
    const fake = fakes();
    fake.denyRate();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject(request);
    expect(response.statusCode).toBe(429);
    expect(response.json().error).toBe("rate_limited");
    expect(fake.processor.quote).not.toHaveBeenCalled();
    expect(fake.created).toHaveLength(0);
  });

  it("requires an idempotency key and rejects invalid scenarios", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    expect((await app.inject({ ...request, headers: {} })).statusCode).toBe(400);
    expect((await app.inject({
      ...request,
      headers: { ...request.headers, "x-lab-scenario": "magic" },
    })).statusCode).toBe(400);
  });

  it("reports broker degradation without removing the durable API from readiness", async () => {
    const fake = fakes();
    fake.messaging.isReady = () => false;
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject({ method: "GET", url: "/readyz" });
    expect(response.statusCode).toBe(200);
    expect(response.json().dependencies.brokers).toBe(false);
  });

  it("returns the outbox-backed runtime snapshot", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    await app.inject(request);
    const response = await app.inject({ method: "GET", url: "/api/v1/runtime" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      service: "flashdrop-order-service",
      storage: { orders: { pending: 1 }, outbox: { pending: 2 } },
    });
  });

  it("exposes an honest operations snapshot for Event Hub sampling", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    await app.inject(request);
    const response = await app.inject({ method: "GET", url: "/operations/snapshot" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      nodes: {
        "order-api": { inFlight: 0, total: 1, healthy: true },
        redis: { inFlight: null, total: null, healthy: true },
        pricing: { inFlight: null, total: null, healthy: true },
        "outbox-relay": { inFlight: null, total: 0, healthy: null },
        "order-finalizer": { inFlight: null, total: 0, healthy: null },
        postgresql: { inFlight: null, total: 1, healthy: true },
      },
      postgres: { pendingOutbox: 2 },
      outbox: { pending: 2, retrying: 0, published: 0 },
      orders: { pending: 1, confirmed: 0, sold_out: 0, failed: 0 },
    });
  });

  it("keeps operations sampling available when the PostgreSQL runtime query fails", async () => {
    const fake = fakes();
    fake.storage.getRuntime = vi.fn(async () => { throw new Error("runtime query failed"); });
    fake.idempotency.isReady = vi.fn(async () => false);
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject({ method: "GET", url: "/operations/snapshot" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      nodes: {
        "order-api": { inFlight: 0, total: null, healthy: false },
        redis: { inFlight: null, total: null, healthy: false },
        pricing: { inFlight: null, total: null, healthy: true },
        "outbox-relay": { inFlight: null, total: null, healthy: null },
        "order-finalizer": { inFlight: null, total: null, healthy: null },
        postgresql: { inFlight: null, total: null, healthy: false },
      },
      postgres: { pendingOutbox: null },
      outbox: { pending: null, retrying: null, published: null },
      orders: { pending: null, confirmed: null, sold_out: null, failed: null },
    });
    expect(fake.processor.isReady).toHaveBeenCalledOnce();
    expect(fake.storage.isReady).toHaveBeenCalledOnce();
    expect(fake.idempotency.isReady).toHaveBeenCalledOnce();
  });

  it("keeps the legacy runtime fallback available when PostgreSQL sampling fails", async () => {
    const fake = fakes();
    fake.storage.getRuntime = vi.fn(async () => { throw new Error("runtime query failed"); });
    fake.processor.isReady = vi.fn(async () => false);
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const response = await app.inject({ method: "GET", url: "/api/v1/runtime" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      dependencies: { processor: false, postgres: false, redis: true },
      storage: {
        orders: { pending: null, confirmed: null, sold_out: null, failed: null },
        outbox: { pending: null, retrying: null, published: null },
        traces: null,
      },
    });
    expect(fake.processor.isReady).toHaveBeenCalledOnce();
    expect(fake.storage.isReady).toHaveBeenCalledOnce();
    expect(fake.idempotency.isReady).toHaveBeenCalledOnce();
  });

  it("proxies real inventory reads and lab resets", async () => {
    const fake = fakes();
    const app = await createApp({ config, ...fake, metrics: new GatewayMetrics() });
    apps.push(app);
    const listed = await app.inject({ method: "GET", url: "/api/v1/inventory" });
    expect(listed.statusCode).toBe(200);
    expect(listed.json().items[0]).toMatchObject({ sku: "DROP-SNEAKER-RED", available: 100 });
    const reset = await app.inject({
      method: "POST", url: "/api/v1/lab/inventory/reset",
      payload: { items: [{ sku: "DROP-SNEAKER-RED", available: 25 }] },
    });
    expect(reset.statusCode).toBe(200);
    expect(fake.inventory.reset).toHaveBeenCalledWith({
      items: [{ sku: "DROP-SNEAKER-RED", available: 25 }],
    });
  });
});
