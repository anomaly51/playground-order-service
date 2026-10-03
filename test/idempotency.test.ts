import { describe, expect, it, vi } from "vitest";
import {
  fingerprint,
  IdempotencyConflictError,
  IdempotencyInProgressError,
  IdempotencyPreviousAttemptFailedError,
  IdempotencyStore,
  RedisIdempotencyCache,
} from "../src/idempotency.js";
import type { GatewayConfig } from "../src/config.js";

const response = {
  orderId: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
  traceId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  status: "pending" as const,
  quote: {
    currency: "USD", unitPriceCents: 14_900, discountCents: 0,
    totalCents: 14_900, priceVersion: "drop-v1",
    quotedAt: "2026-08-28T10:00:00.000Z",
  },
  acceptedAt: "2026-08-28T10:00:00.000Z",
  links: { self: "/api/v1/orders/01ARZ3NDEKTSV4RRFFQ69G5FAW" },
};

const config: GatewayConfig = {
  nodeEnv: "test",
  logLevel: "silent",
  host: "127.0.0.1",
  port: 3_000,
  corsOrigins: ["*"],
  kafkaBrokers: ["unused:9092"],
  kafkaClientId: "test",
  kafkaSsl: false,
  rabbitUrl: "amqp://unused",
  redisUrl: "redis://unused:6379/0",
  redisConnectTimeoutMs: 100,
  redisCommandTimeoutMs: 50,
  redisKeyPrefix: "test:idempotency:",
  redisRateLimitPrefix: "test:rate-limit:",
  rateLimitRequests: 10,
  rateLimitWindowMs: 1_000,
  postgresUrl: "postgresql://unused:unused@unused:5432/unused",
  postgresPoolSize: 2,
  postgresConnectionTimeoutMs: 100,
  processorHttpUrl: "http://unused:8001",
  inventoryServiceUrl: "http://inventory-worker:3004",
  inventoryHttpTimeoutMs: 100,
  processorHttpTimeoutMs: 100,
  readinessTimeoutMs: 50,
  requestTimeoutMs: 1_000,
  idempotencyTtlMs: 10_000,
  idempotencyInFlightTtlMs: 1_000,
  idempotencyMaxEntries: 10,
  labScenariosEnabled: true,
  outboxPollMs: 100,
  outboxBatchSize: 100,
  outboxRetryMaxMs: 30_000,
  relayOperationsHost: "127.0.0.1",
  relayOperationsPort: 3_005,
};

class FakeRedisClient {
  isOpen = true;
  isReady = true;
  readonly values = new Map<string, string>();
  lastSetOptions: unknown;

  on() { return this; }
  withAbortSignal() { return this; }
  async connect() { this.isReady = true; return this; }
  async get(key: string) { return this.values.get(key) ?? null; }
  async set(key: string, value: string, options: { condition?: string }) {
    if (options.condition === "NX" && this.values.has(key)) return null;
    this.values.set(key, value);
    this.lastSetOptions = options;
    return "OK";
  }
  async eval(
    script: string,
    options: { keys: string[]; arguments: string[] },
  ) {
    const key = options.keys[0];
    if (!key) return 0;
    const raw = this.values.get(key);
    if (!raw) {
      if (script.includes("redis.call('SET'")) {
        this.values.set(key, options.arguments[2] ?? "");
        return 1;
      }
      return 0;
    }
    const entry = JSON.parse(raw) as {
      state?: string;
      owner?: string;
      fingerprint?: string;
    };
    if (
      entry.state !== "processing"
      || entry.owner !== options.arguments[0]
      || entry.fingerprint !== options.arguments[1]
    ) {
      return 0;
    }
    if (script.includes("redis.call('DEL'")) {
      this.values.delete(key);
    } else if (script.includes("redis.call('PEXPIRE'")) {
      return 1;
    } else {
      this.values.set(key, options.arguments[2] ?? "");
    }
    return 1;
  }
  async ping() { return "PONG"; }
  async close() { this.isOpen = false; this.isReady = false; }
  destroy() { this.isOpen = false; this.isReady = false; }
}

const logger = {
  info: vi.fn(),
  warn: vi.fn(),
};

describe("IdempotencyStore", () => {
  it("returns the same response for a matching request", () => {
    const store = new IdempotencyStore(1_000, 10);
    store.set("key", "same", response, 100);
    expect(store.get("key", "same", 101)).toEqual(response);
  });

  it("rejects key reuse with another payload", () => {
    const store = new IdempotencyStore(1_000, 10);
    store.set("key", "first", response, 100);
    expect(() => store.get("key", "second", 101)).toThrow(IdempotencyConflictError);
  });

  it("canonicalizes object keys", () => {
    expect(fingerprint({ b: 2, a: { d: 4, c: 3 } })).toBe(
      fingerprint({ a: { c: 3, d: 4 }, b: 2 }),
    );
  });

  it("stores only a fixed-size digest of the canonical request", () => {
    const digest = fingerprint({ message: "private lab message", mode: "full" });
    expect(digest).toMatch(/^[a-f0-9]{64}$/);
    expect(digest).not.toContain("private lab message");
  });

  it("allows only one in-flight owner for a key", () => {
    const store = new IdempotencyStore(1_000, 10, 100);
    expect(store.reserve("key", "same", "owner-1", 100)).toEqual({ kind: "claimed" });
    expect(() => store.reserve("key", "same", "owner-2", 101)).toThrow(
      IdempotencyInProgressError,
    );
    expect(() => store.reserve("key", "different", "owner-2", 101)).toThrow(
      IdempotencyConflictError,
    );
    store.release("key", "same", "owner-1");
    expect(store.reserve("key", "same", "owner-2", 102)).toEqual({ kind: "claimed" });
  });

  it("retains a tombstone when side effects may already have happened", () => {
    const store = new IdempotencyStore(1_000, 10, 100);
    store.reserve("key", "same", "owner-1", 100);
    expect(store.renew("key", "same", "owner-1", 150)).toBe(true);
    store.fail("key", "same", "owner-1", 160);
    expect(() => store.reserve("key", "same", "owner-2", 161)).toThrow(
      IdempotencyPreviousAttemptFailedError,
    );
  });
});

describe("RedisIdempotencyCache", () => {
  it("atomically reserves and shares completed responses through a hashed key", async () => {
    const client = new FakeRedisClient();
    const cache = new RedisIdempotencyCache(
      config,
      logger,
      client as never,
    );

    expect(await cache.reserve("external-key", "same", "owner-1")).toEqual({
      kind: "claimed",
      backend: "redis",
    });
    expect(client.lastSetOptions).toEqual({
      condition: "NX",
      expiration: { type: "PX", value: 1_000 },
    });
    expect(await cache.complete("external-key", "same", "owner-1", response)).toEqual({
      backend: "redis",
    });
    expect(await cache.reserve("external-key", "same", "owner-2")).toEqual({
      kind: "cached",
      backend: "redis",
      response,
    });
    expect([...client.values.keys()][0]).not.toContain("external-key");
    await expect(cache.reserve("external-key", "different", "owner-3")).rejects.toThrow(
      IdempotencyConflictError,
    );
  });

  it("rejects a second owner while the Redis reservation is active", async () => {
    const client = new FakeRedisClient();
    const cache = new RedisIdempotencyCache(config, logger, client as never);

    await cache.reserve("busy-key", "same", "owner-1");
    await expect(cache.reserve("busy-key", "same", "owner-2")).rejects.toThrow(
      IdempotencyInProgressError,
    );
    expect(await cache.release("busy-key", "same", "owner-1")).toEqual({
      backend: "redis",
    });
    expect(await cache.reserve("busy-key", "same", "owner-2")).toEqual({
      kind: "claimed",
      backend: "redis",
    });
  });

  it("renews a lease and writes an owner-safe failure tombstone", async () => {
    const client = new FakeRedisClient();
    const cache = new RedisIdempotencyCache(config, logger, client as never);

    await cache.reserve("failed-key", "same", "owner-1");
    expect(await cache.renew("failed-key", "same", "owner-1")).toEqual({
      backend: "redis",
    });
    expect(await cache.fail("failed-key", "same", "owner-1")).toEqual({
      backend: "redis",
    });
    await expect(cache.reserve("failed-key", "same", "owner-2")).rejects.toThrow(
      IdempotencyPreviousAttemptFailedError,
    );
  });

  it("keeps the single-replica lab available through the local fallback", async () => {
    const client = new FakeRedisClient();
    client.isReady = false;
    const cache = new RedisIdempotencyCache(
      config,
      logger,
      client as never,
    );

    expect(await cache.reserve("fallback-key", "same", "owner-1")).toEqual({
      kind: "claimed",
      backend: "memory",
    });
    expect(await cache.complete("fallback-key", "same", "owner-1", response)).toEqual({
      backend: "memory",
    });
    expect(await cache.reserve("fallback-key", "same", "owner-2")).toEqual({
      kind: "cached",
      backend: "memory",
      response,
    });
    expect(await cache.isReady(50)).toBe(false);
  });
});
