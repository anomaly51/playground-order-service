import { createHash } from "node:crypto";
import {
  KAFKA_TOPICS,
  OrderEventEnvelopeSchema,
  OrderSchema,
  TraceEventSchema,
  type CreateOrderRequest,
  type InventoryReservationCommand,
  type InventoryReservationResult,
  type LabScenario,
  type Order,
  type OrderEventEnvelope,
  type Quote,
  type TraceEvent,
} from "./contracts.js";
import { Pool, type PoolClient } from "pg";
import { ulid } from "ulid";
import type { GatewayConfig } from "./config.js";

export type OutboxTransport = "kafka" | "rabbitmq";

export interface OutboxRecord {
  outboxId: string;
  aggregateId: string;
  traceId: string;
  eventType: string;
  transport: OutboxTransport;
  destination: string;
  routingKey?: string;
  partitionKey: string;
  payload: unknown;
  attempts: number;
}

export interface CreateOrderInput {
  orderId: string;
  traceId: string;
  idempotencyKeyHash: string;
  requestHash: string;
  request: CreateOrderRequest;
  quote: Quote;
  scenario: LabScenario;
  event: OrderEventEnvelope;
  command: InventoryReservationCommand;
  createdAt: string;
}

export interface RuntimeSnapshot {
  orders: Record<"pending" | "confirmed" | "sold_out" | "failed", number>;
  outbox: { pending: number; retrying: number; published: number };
  traces: number;
}

export interface GatewayStorage {
  start(): Promise<void>;
  stop(): Promise<void>;
  isReady(timeoutMs: number): Promise<boolean>;
  createOrder(input: CreateOrderInput): Promise<{ order: Order; created: boolean }>;
  getOrder(orderId: string): Promise<Order | undefined>;
  getRuntime(): Promise<RuntimeSnapshot>;
  saveTrace(event: TraceEvent): Promise<void>;
  claimOutbox(limit: number): Promise<OutboxRecord[]>;
  markOutboxPublished(outboxId: string): Promise<void>;
  markOutboxFailed(outboxId: string, error: string, retryInMs: number): Promise<void>;
  applyInventoryResult(
    result: InventoryReservationResult,
  ): Promise<{ order?: Order; duplicate: boolean }>;
}

export interface GatewayStorageLogger {
  error(bindings: { err: unknown }, message: string): void;
}

export interface PostgresGatewayStorageOptions {
  pool?: Pool;
  logger?: GatewayStorageLogger;
}

export class PersistentIdempotencyConflictError extends Error {
  constructor() {
    super("Idempotency-Key was already persisted with a different order request");
    this.name = "PersistentIdempotencyConflictError";
  }
}

const CREATE_ORDERS = `
  CREATE TABLE IF NOT EXISTS flashdrop_orders (
    order_id CHAR(26) PRIMARY KEY,
    trace_id CHAR(26) NOT NULL UNIQUE,
    idempotency_key_hash CHAR(64) NOT NULL UNIQUE,
    request_hash CHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity BETWEEN 1 AND 5),
    coupon_code VARCHAR(32),
    status VARCHAR(16) NOT NULL,
    currency CHAR(3) NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    discount_cents INTEGER NOT NULL,
    total_cents INTEGER NOT NULL,
    price_version VARCHAR(64) NOT NULL,
    quoted_at TIMESTAMPTZ NOT NULL,
    failure_reason VARCHAR(512),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )
`;

const CREATE_OUTBOX = `
  CREATE TABLE IF NOT EXISTS flashdrop_outbox (
    outbox_id CHAR(26) PRIMARY KEY,
    aggregate_id CHAR(26) NOT NULL,
    trace_id CHAR(26) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    transport VARCHAR(16) NOT NULL CHECK (transport IN ('kafka', 'rabbitmq')),
    destination VARCHAR(255) NOT NULL,
    routing_key VARCHAR(128),
    partition_key VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error VARCHAR(1024),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_id, event_type, transport)
  )
`;

const CREATE_INBOX = `
  CREATE TABLE IF NOT EXISTS flashdrop_inbox (
    result_id CHAR(26) PRIMARY KEY,
    order_id CHAR(26) NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
`;

const CREATE_TRACES = `
  CREATE TABLE IF NOT EXISTS flashdrop_trace_events (
    event_id CHAR(26) PRIMARY KEY,
    trace_id CHAR(26) NOT NULL,
    stage VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
  )
`;

const CREATE_INDEXES = [
  "CREATE INDEX IF NOT EXISTS flashdrop_orders_status_idx ON flashdrop_orders (status, updated_at DESC)",
  "CREATE INDEX IF NOT EXISTS flashdrop_outbox_pending_idx ON flashdrop_outbox (available_at, created_at) WHERE published_at IS NULL",
  "CREATE INDEX IF NOT EXISTS flashdrop_trace_id_idx ON flashdrop_trace_events (trace_id, occurred_at)",
] as const;

function mapOrder(row: Record<string, unknown>): Order {
  return OrderSchema.parse({
    orderId: row.order_id,
    traceId: row.trace_id,
    customerId: row.customer_id,
    sku: row.sku,
    quantity: Number(row.quantity),
    ...(row.coupon_code ? { couponCode: row.coupon_code } : {}),
    status: row.status,
    quote: {
      currency: row.currency,
      unitPriceCents: Number(row.unit_price_cents),
      discountCents: Number(row.discount_cents),
      totalCents: Number(row.total_cents),
      priceVersion: row.price_version,
      quotedAt: new Date(row.quoted_at as string | number | Date).toISOString(),
    },
    ...(row.failure_reason ? { failureReason: row.failure_reason } : {}),
    createdAt: new Date(row.created_at as string | number | Date).toISOString(),
    updatedAt: new Date(row.updated_at as string | number | Date).toISOString(),
    version: Number(row.version),
  });
}

async function insertOutbox(client: PoolClient, record: {
  aggregateId: string;
  traceId: string;
  eventType: string;
  transport: OutboxTransport;
  destination: string;
  routingKey?: string;
  partitionKey: string;
  payload: unknown;
}): Promise<void> {
  await client.query(
    `INSERT INTO flashdrop_outbox (
       outbox_id, aggregate_id, trace_id, event_type, transport,
       destination, routing_key, partition_key, payload
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
     ON CONFLICT (aggregate_id, event_type, transport) DO NOTHING`,
    [
      ulid(), record.aggregateId, record.traceId, record.eventType, record.transport,
      record.destination, record.routingKey ?? null, record.partitionKey,
      JSON.stringify(record.payload),
    ],
  );
}

export function hashExternalKey(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export class PostgresGatewayStorage implements GatewayStorage {
  private readonly pool: Pool;
  private started = false;

  constructor(config: GatewayConfig, options: PostgresGatewayStorageOptions = {}) {
    this.pool = options.pool ?? new Pool({
      connectionString: config.postgresUrl,
      max: config.postgresPoolSize,
      connectionTimeoutMillis: config.postgresConnectionTimeoutMs,
      idleTimeoutMillis: 30_000,
      allowExitOnIdle: false,
      application_name: "flashdrop-order-service",
    });
    this.pool.on("error", (error) => {
      if (options.logger) {
        options.logger.error({ err: error }, "PostgreSQL idle client disconnected");
      } else {
        console.error("PostgreSQL idle client disconnected", error);
      }
    });
  }

  async start(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      // API and relay may bootstrap the same fresh database concurrently.
      await client.query("SELECT pg_advisory_xact_lock(51001, 1)");
      for (const statement of [CREATE_ORDERS, CREATE_OUTBOX, CREATE_INBOX, CREATE_TRACES, ...CREATE_INDEXES]) {
        await client.query(statement);
      }
      await client.query("COMMIT");
      this.started = true;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async createOrder(input: CreateOrderInput): Promise<{ order: Order; created: boolean }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const inserted = await client.query<Record<string, unknown>>(
        `INSERT INTO flashdrop_orders (
           order_id, trace_id, idempotency_key_hash, request_hash, customer_id,
           sku, quantity, coupon_code, status, currency, unit_price_cents,
           discount_cents, total_cents, price_version, quoted_at, version,
           created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,$10,$11,$12,$13,$14,1,$15,$15)
         ON CONFLICT (idempotency_key_hash) DO NOTHING
         RETURNING *`,
        [
          input.orderId, input.traceId, input.idempotencyKeyHash, input.requestHash,
          input.request.customerId, input.request.sku, input.request.quantity,
          input.request.couponCode ?? null, input.quote.currency, input.quote.unitPriceCents,
          input.quote.discountCents, input.quote.totalCents, input.quote.priceVersion,
          input.quote.quotedAt, input.createdAt,
        ],
      );
      if (inserted.rowCount === 0) {
        const existing = await client.query<Record<string, unknown>>(
          "SELECT * FROM flashdrop_orders WHERE idempotency_key_hash = $1",
          [input.idempotencyKeyHash],
        );
        const row = existing.rows[0];
        if (!row || row.request_hash !== input.requestHash) {
          throw new PersistentIdempotencyConflictError();
        }
        await client.query("COMMIT");
        return { order: mapOrder(row), created: false };
      }

      await insertOutbox(client, {
        aggregateId: input.orderId,
        traceId: input.traceId,
        eventType: input.event.eventType,
        transport: "kafka",
        destination: KAFKA_TOPICS.orders,
        partitionKey: input.orderId,
        payload: input.event,
      });
      await insertOutbox(client, {
        aggregateId: input.orderId,
        traceId: input.traceId,
        eventType: "inventory.reserve",
        transport: "rabbitmq",
        destination: "flashdrop.inventory.commands.v1",
        routingKey: "inventory.reserve",
        partitionKey: input.orderId,
        payload: input.command,
      });
      await client.query("COMMIT");
      return { order: mapOrder(inserted.rows[0]!), created: true };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async getOrder(orderId: string): Promise<Order | undefined> {
    const result = await this.pool.query<Record<string, unknown>>(
      "SELECT * FROM flashdrop_orders WHERE order_id = $1",
      [orderId],
    );
    return result.rows[0] ? mapOrder(result.rows[0]) : undefined;
  }

  async saveTrace(raw: TraceEvent): Promise<void> {
    const event = TraceEventSchema.parse(raw);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const inserted = await client.query(
        `INSERT INTO flashdrop_trace_events (event_id, trace_id, stage, status, payload, occurred_at)
         VALUES ($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT (event_id) DO NOTHING`,
        [event.id, event.traceId, event.stage, event.status, JSON.stringify(event), event.timestamp],
      );
      if ((inserted.rowCount ?? 0) > 0) {
        await insertOutbox(client, {
          aggregateId: event.id,
          traceId: event.traceId,
          eventType: `trace.${event.stage}.${event.status}`,
          transport: "kafka",
          destination: KAFKA_TOPICS.traces,
          partitionKey: event.traceId,
          payload: event,
        });
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async claimOutbox(limit: number): Promise<OutboxRecord[]> {
    const result = await this.pool.query<Record<string, unknown>>(
      `WITH picked AS (
         SELECT candidate.outbox_id
         FROM flashdrop_outbox candidate
         WHERE candidate.published_at IS NULL
           AND candidate.available_at <= NOW()
           AND (candidate.claimed_at IS NULL OR candidate.claimed_at < NOW() - INTERVAL '30 seconds')
           AND NOT EXISTS (
             SELECT 1 FROM flashdrop_outbox earlier
             WHERE earlier.aggregate_id = candidate.aggregate_id
               AND earlier.transport = candidate.transport
               AND earlier.published_at IS NULL
               AND earlier.created_at < candidate.created_at
           )
         ORDER BY candidate.created_at, candidate.outbox_id
         FOR UPDATE SKIP LOCKED
         LIMIT $1
       )
       UPDATE flashdrop_outbox item
       SET claimed_at = NOW(), attempts = item.attempts + 1
       FROM picked WHERE item.outbox_id = picked.outbox_id
       RETURNING item.*`,
      [limit],
    );
    return result.rows.map((row) => ({
      outboxId: String(row.outbox_id),
      aggregateId: String(row.aggregate_id),
      traceId: String(row.trace_id),
      eventType: String(row.event_type),
      transport: row.transport as OutboxTransport,
      destination: String(row.destination),
      ...(row.routing_key ? { routingKey: String(row.routing_key) } : {}),
      partitionKey: String(row.partition_key),
      payload: row.payload,
      attempts: Number(row.attempts),
    }));
  }

  async markOutboxPublished(outboxId: string): Promise<void> {
    await this.pool.query(
      "UPDATE flashdrop_outbox SET published_at=NOW(), claimed_at=NULL, last_error=NULL WHERE outbox_id=$1",
      [outboxId],
    );
  }

  async markOutboxFailed(outboxId: string, error: string, retryInMs: number): Promise<void> {
    await this.pool.query(
      `UPDATE flashdrop_outbox SET claimed_at=NULL, last_error=$2,
       available_at=NOW() + ($3 * INTERVAL '1 millisecond') WHERE outbox_id=$1`,
      [outboxId, error.slice(0, 1_024), retryInMs],
    );
  }

  async applyInventoryResult(
    result: InventoryReservationResult,
  ): Promise<{ order?: Order; duplicate: boolean }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const inbox = await client.query(
        `INSERT INTO flashdrop_inbox (result_id, order_id, payload)
         VALUES ($1,$2,$3::jsonb) ON CONFLICT (result_id) DO NOTHING`,
        [result.resultId, result.orderId, JSON.stringify(result)],
      );
      if (inbox.rowCount === 0) {
        await client.query("COMMIT");
        return { duplicate: true };
      }
      const selected = await client.query<Record<string, unknown>>(
        "SELECT * FROM flashdrop_orders WHERE order_id=$1 FOR UPDATE",
        [result.orderId],
      );
      const current = selected.rows[0];
      if (!current) throw new Error(`Order ${result.orderId} does not exist`);
      if (current.status !== "pending") {
        await client.query("COMMIT");
        return { order: mapOrder(current), duplicate: true };
      }
      const status = result.status === "reserved" ? "confirmed" : result.status;
      const reason = result.reason ?? (status === "sold_out" ? "Inventory is sold out" : null);
      const updated = await client.query<Record<string, unknown>>(
        `UPDATE flashdrop_orders SET status=$2, failure_reason=$3,
         version=version+1, updated_at=$4 WHERE order_id=$1 RETURNING *`,
        [result.orderId, status, reason, result.processedAt],
      );
      const order = mapOrder(updated.rows[0]!);
      const event = OrderEventEnvelopeSchema.parse({
        schemaVersion: 1,
        eventId: ulid(),
        eventType: status === "confirmed" ? "order.confirmed"
          : status === "sold_out" ? "order.sold_out" : "order.failed",
        occurredAt: result.processedAt,
        traceId: result.traceId,
        orderId: result.orderId,
        ...(result.runId ? { runId: result.runId } : {}),
        aggregateVersion: order.version,
        data: {
          sku: order.sku,
          quantity: order.quantity,
          currency: order.quote.currency,
          totalCents: order.quote.totalCents,
          status: order.status,
          ...(order.failureReason ? { reason: order.failureReason } : {}),
        },
      });
      await insertOutbox(client, {
        aggregateId: order.orderId,
        traceId: order.traceId,
        eventType: event.eventType,
        transport: "kafka",
        destination: KAFKA_TOPICS.orders,
        partitionKey: order.orderId,
        payload: event,
      });
      await client.query("COMMIT");
      return { order, duplicate: false };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async getRuntime(): Promise<RuntimeSnapshot> {
    const [orders, outbox, traces] = await Promise.all([
      this.pool.query<{ status: string; count: string }>(
        "SELECT status, COUNT(*)::text AS count FROM flashdrop_orders GROUP BY status",
      ),
      this.pool.query<{ pending: string; retrying: string; published: string }>(
        `SELECT
          COUNT(*) FILTER (WHERE published_at IS NULL)::text AS pending,
          COUNT(*) FILTER (WHERE published_at IS NULL AND attempts > 0)::text AS retrying,
          COUNT(*) FILTER (WHERE published_at IS NOT NULL)::text AS published
         FROM flashdrop_outbox`,
      ),
      this.pool.query<{ count: string }>("SELECT COUNT(*)::text AS count FROM flashdrop_trace_events"),
    ]);
    const counts = { pending: 0, confirmed: 0, sold_out: 0, failed: 0 };
    for (const row of orders.rows) {
      if (row.status in counts) counts[row.status as keyof typeof counts] = Number(row.count);
    }
    const outboxRow = outbox.rows[0] ?? { pending: "0", retrying: "0", published: "0" };
    return {
      orders: counts,
      outbox: {
        pending: Number(outboxRow.pending),
        retrying: Number(outboxRow.retrying),
        published: Number(outboxRow.published),
      },
      traces: Number(traces.rows[0]?.count ?? 0),
    };
  }

  async isReady(timeoutMs: number): Promise<boolean> {
    if (!this.started) return false;
    let timeout: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.pool.query("SELECT 1"),
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(() => reject(new Error("PostgreSQL readiness timed out")), timeoutMs);
          timeout.unref();
        }),
      ]);
      return true;
    } catch {
      return false;
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  async stop(): Promise<void> {
    this.started = false;
    await this.pool.end();
  }
}
