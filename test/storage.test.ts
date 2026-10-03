import { EventEmitter } from "node:events";
import type { Pool } from "pg";
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { hashExternalKey, PostgresGatewayStorage } from "../src/storage.js";

const now = "2026-08-30T10:00:00.000Z";
const pendingRow = {
  order_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
  trace_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  idempotency_key_hash: "a".repeat(64), request_hash: "b".repeat(64),
  customer_id: "customer-42", sku: "DROP-SNEAKER-RED", quantity: 1,
  coupon_code: null, status: "pending", currency: "USD",
  unit_price_cents: 14_900, discount_cents: 0, total_cents: 14_900,
  price_version: "drop-v1", quoted_at: now, failure_reason: null,
  version: 1, created_at: now, updated_at: now,
};

function config() {
  return loadConfig({ NODE_ENV: "test", POSTGRES_URL: "postgresql://lab:lab@postgres:5432/flashdrop" });
}

describe("PostgresGatewayStorage", () => {
  it("serializes API and relay schema initialization within one transaction", async () => {
    const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
    const release = vi.fn();
    const pool = { connect: vi.fn(async () => ({ query, release })), on: vi.fn() } as unknown as Pool;
    await new PostgresGatewayStorage(config(), { pool }).start();
    expect(query.mock.calls[0]).toEqual(["BEGIN"]);
    expect(query.mock.calls[1]).toEqual(["SELECT pg_advisory_xact_lock(51001, 1)"]);
    expect(query.mock.calls.at(-1)).toEqual(["COMMIT"]);
    expect(release).toHaveBeenCalledOnce();
  });

  it("rolls back and releases the bootstrap connection on schema failure", async () => {
    const failure = new Error("schema unavailable");
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("CREATE TABLE")) throw failure;
      return { rows: [], rowCount: 0 };
    });
    const release = vi.fn();
    const pool = { connect: vi.fn(async () => ({ query, release })), on: vi.fn() } as unknown as Pool;
    await expect(new PostgresGatewayStorage(config(), { pool }).start()).rejects.toThrow(failure);
    expect(query.mock.calls.at(-1)).toEqual(["ROLLBACK"]);
    expect(release).toHaveBeenCalledOnce();
  });

  it("handles idle pool disconnects without promoting them to uncaught exceptions", () => {
    const emitter = new EventEmitter();
    const pool = Object.assign(emitter, {
      query: vi.fn(), connect: vi.fn(), end: vi.fn(),
    }) as unknown as Pool;
    const logger = { error: vi.fn() };
    new PostgresGatewayStorage(config(), { pool, logger });
    const disconnect = new Error("terminating connection due to administrator command");

    expect(() => emitter.emit("error", disconnect)).not.toThrow();
    expect(logger.error).toHaveBeenCalledWith(
      { err: disconnect },
      "PostgreSQL idle client disconnected",
    );
  });

  it("commits an order and both broker outbox entries in one transaction", async () => {
    const clientQuery = vi.fn(async (sql: string) => {
      if (sql.includes("INSERT INTO flashdrop_orders")) return { rows: [pendingRow], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    });
    const release = vi.fn();
    const pool = {
      query: vi.fn(async () => ({ rows: [], rowCount: 1 })),
      connect: vi.fn(async () => ({ query: clientQuery, release })),
      end: vi.fn(), on: vi.fn(),
    } as unknown as Pool;
    const storage = new PostgresGatewayStorage(config(), { pool });

    const result = await storage.createOrder({
      orderId: pendingRow.order_id,
      traceId: pendingRow.trace_id,
      idempotencyKeyHash: pendingRow.idempotency_key_hash,
      requestHash: pendingRow.request_hash,
      request: { customerId: pendingRow.customer_id, sku: pendingRow.sku, quantity: 1 },
      quote: {
        currency: "USD", unitPriceCents: 14_900, discountCents: 0,
        totalCents: 14_900, priceVersion: "drop-v1", quotedAt: now,
      },
      scenario: "normal",
      event: {
        schemaVersion: 1, eventId: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
        eventType: "order.created", occurredAt: now, traceId: pendingRow.trace_id,
        orderId: pendingRow.order_id, aggregateVersion: 1,
        data: { sku: pendingRow.sku, quantity: 1, currency: "USD", totalCents: 14_900, status: "pending" },
      },
      command: {
        schemaVersion: 1, commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAY",
        traceId: pendingRow.trace_id, orderId: pendingRow.order_id,
        sku: pendingRow.sku, quantity: 1, scenario: "normal", createdAt: now,
      },
      createdAt: now,
    });

    expect(result.created).toBe(true);
    expect(result.order.status).toBe("pending");
    expect(clientQuery.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(clientQuery.mock.calls.filter(([sql]) => String(sql).includes("INSERT INTO flashdrop_outbox"))).toHaveLength(2);
    expect(clientQuery.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(release).toHaveBeenCalledOnce();
  });

  it("applies a result exactly once and stages a terminal Kafka event", async () => {
    const confirmedRow = { ...pendingRow, status: "confirmed", version: 2, updated_at: "2026-08-30T10:00:01.000Z" };
    const clientQuery = vi.fn(async (sql: string) => {
      if (sql.includes("INSERT INTO flashdrop_inbox")) return { rows: [], rowCount: 1 };
      if (sql.includes("SELECT * FROM flashdrop_orders")) return { rows: [pendingRow], rowCount: 1 };
      if (sql.includes("UPDATE flashdrop_orders")) return { rows: [confirmedRow], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    });
    const pool = {
      query: vi.fn(),
      connect: vi.fn(async () => ({ query: clientQuery, release: vi.fn() })),
      end: vi.fn(), on: vi.fn(),
    } as unknown as Pool;
    const storage = new PostgresGatewayStorage(config(), { pool });
    const result = await storage.applyInventoryResult({
      schemaVersion: 1, resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
      commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAY",
      traceId: pendingRow.trace_id, orderId: pendingRow.order_id,
      sku: pendingRow.sku, quantity: 1, status: "reserved", remainingStock: 99,
      processedAt: "2026-08-30T10:00:01.000Z",
    });
    expect(result).toMatchObject({ duplicate: false, order: { status: "confirmed", version: 2 } });
    expect(clientQuery.mock.calls.some(([sql]) => String(sql).includes("INSERT INTO flashdrop_outbox"))).toBe(true);
    expect(clientQuery.mock.calls.at(-1)?.[0]).toBe("COMMIT");
  });

  it("hashes external idempotency keys before persistence", () => {
    expect(hashExternalKey("private-checkout-key")).toMatch(/^[a-f0-9]{64}$/);
    expect(hashExternalKey("private-checkout-key")).not.toContain("private-checkout-key");
  });
});
