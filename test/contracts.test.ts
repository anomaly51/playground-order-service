import { describe, expect, it } from "vitest";
import {
  CreateOrderRequestSchema,
  CreateOrderResponseSchema,
  FLASHDROP_SKUS,
  InventoryReservationCommandSchema,
  InventoryReservationResultSchema,
  InventoryListResponseSchema,
  InventoryResetRequestSchema,
  KAFKA_TOPICS,
  LAB_SCENARIOS,
  OPENAPI_SCHEMAS,
  OrderEventEnvelopeSchema,
  RABBITMQ,
  TraceEventSchema,
} from "../src/contracts.js";

const traceId = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const orderId = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

describe("FlashDrop public contracts", () => {
  it("validates the strict order request", () => {
    expect(CreateOrderRequestSchema.parse({
      customerId: "customer-42",
      sku: "DROP-SNEAKER-RED",
      quantity: 2,
      couponCode: "DROP10",
    })).toMatchObject({ quantity: 2 });
    expect(() => CreateOrderRequestSchema.parse({
      customerId: "customer-42", sku: "UNKNOWN", quantity: 1,
    })).toThrow();
    expect(() => CreateOrderRequestSchema.parse({
      customerId: "customer-42", sku: FLASHDROP_SKUS[0], quantity: 6,
    })).toThrow();
    expect(() => CreateOrderRequestSchema.parse({
      customerId: "customer-42", sku: FLASHDROP_SKUS[0], quantity: 1, extra: true,
    })).toThrow();
  });

  it("exposes versioned FlashDrop broker names", () => {
    expect(KAFKA_TOPICS.orders).toBe("flashdrop.orders.v1");
    expect(RABBITMQ.queues.orderResults).toBe("flashdrop.order-service.inventory-results.v1");
    expect(LAB_SCENARIOS).toContain("inventory-dlq");
  });

  it("validates order, command and result envelopes", () => {
    expect(OrderEventEnvelopeSchema.safeParse({
      schemaVersion: 1,
      eventId: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      eventType: "order.created",
      occurredAt: "2026-08-30T10:00:00.000Z",
      traceId,
      orderId,
      aggregateVersion: 1,
      data: {
        sku: "DROP-SNEAKER-RED", quantity: 1, currency: "USD",
        totalCents: 14900, status: "pending",
      },
    }).success).toBe(true);
    const command = {
      schemaVersion: 1,
      commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAY",
      traceId,
      orderId,
      sku: "DROP-SNEAKER-RED",
      quantity: 1,
      scenario: "inventory-retry",
      createdAt: "2026-08-30T10:00:00.000Z",
    };
    expect(InventoryReservationCommandSchema.safeParse(command).success).toBe(true);
    expect(InventoryReservationResultSchema.safeParse({
      schemaVersion: 1,
      resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
      commandId: command.commandId,
      traceId,
      orderId,
      sku: command.sku,
      quantity: 1,
      status: "reserved",
      remainingStock: 49,
      processedAt: "2026-08-30T10:00:01.000Z",
    }).success).toBe(true);
  });

  it("supports first-class live-map correlation fields", () => {
    expect(TraceEventSchema.parse({
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      traceId,
      runId: "load-run-42",
      orderId,
      correlationId: "request-42",
      causationId: "command-42",
      timestamp: "2026-08-30T10:00:00.000Z",
      source: "gateway",
      target: "postgresql",
      stage: "order.persist",
      status: "succeeded",
      transport: "postgresql",
      summary: "Order committed",
    })).toMatchObject({ runId: "load-run-42", orderId });
  });

  it("validates real inventory read and reset contracts", () => {
    expect(InventoryListResponseSchema.safeParse({
      items: [{
        sku: "DROP-CAP-LIME", available: 10, reserved: 2,
        updatedAt: "2026-08-30T10:00:00.000Z",
      }],
    }).success).toBe(true);
    expect(InventoryResetRequestSchema.parse({
      items: [{ sku: "DROP-CAP-LIME", available: 25 }],
    })).toEqual({ items: [{ sku: "DROP-CAP-LIME", available: 25 }] });
  });

  it("exports self-contained OpenAPI schemas", () => {
    const document = JSON.stringify(OPENAPI_SCHEMAS);
    expect(document).not.toContain('"$ref":"#/definitions/');
    expect(OPENAPI_SCHEMAS.CreateOrderRequest).toMatchObject({ type: "object" });
    expect(CreateOrderResponseSchema.safeParse({
      orderId,
      traceId,
      status: "pending",
      quote: {
        currency: "USD", unitPriceCents: 14900, discountCents: 0,
        totalCents: 14900, priceVersion: "drop-2026-08", quotedAt: "2026-08-30T10:00:00.000Z",
      },
      acceptedAt: "2026-08-30T10:00:00.000Z",
      links: { self: `/api/v1/orders/${orderId}` },
    }).success).toBe(true);
  });
});
