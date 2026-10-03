import { z } from "zod";
import { zodToJsonSchema } from "zod-to-json-schema";

// Service-owned wire schemas; preserve the versioned payload format.

export const FLASHDROP_SKUS = [
  "DROP-SNEAKER-RED",
  "DROP-HOODIE-BLACK",
  "DROP-CAP-LIME",
] as const;

export const LAB_SCENARIOS = [
  "normal",
  "pricing-timeout",
  "pricing-error",
  "inventory-retry",
  "inventory-dlq",
] as const;

export const ORDER_STATUSES = [
  "pending",
  "confirmed",
  "sold_out",
  "failed",
] as const;

export const ORDER_EVENT_TYPES = [
  "order.created",
  "order.confirmed",
  "order.sold_out",
  "order.failed",
] as const;

export const TRACE_STATUSES = ["started", "succeeded", "failed", "retrying"] as const;

export const TRACE_TRANSPORTS = [
  "http",
  "kafka",
  "rabbitmq",
  "redis",
  "postgresql",
  "mysql",
  "airflow",
  "sse",
] as const;

export const KAFKA_TOPICS = {
  orders: "flashdrop.orders.v1",
  traces: "flashdrop.traces.v1",
  analytics: "flashdrop.analytics.v1",
  // Transitional alias for consumers that are migrated independently.
  messages: "flashdrop.orders.v1",
} as const;

export const RABBITMQ = {
  exchanges: {
    commands: "flashdrop.inventory.commands.v1",
    retry: "flashdrop.inventory.retry.v1",
    deadLetter: "flashdrop.inventory.dlx.v1",
    results: "flashdrop.inventory.results.v1",
  },
  queues: {
    commands: "flashdrop.inventory-worker.commands.v1",
    retry: "flashdrop.inventory-worker.retry.v1",
    deadLetter: "flashdrop.inventory-worker.dlq.v1",
    orderResults: "flashdrop.order-service.inventory-results.v1",
  },
  routingKeys: {
    process: "inventory.reserve",
    retry: "inventory.retry",
    deadLetter: "inventory.dead",
  },
} as const;

export const HTTP_LIMITS = {
  orderRequestBodyBytes: 16 * 1_024,
  // Transitional alias retained while the MFEs are migrated.
  messageRequestBodyBytes: 16 * 1_024,
} as const;

const IdentifierSchema = z.string().trim().min(3).max(64).regex(/^[A-Za-z0-9_-]+$/);

export const CreateOrderRequestSchema = z
  .object({
    customerId: IdentifierSchema,
    sku: z.enum(FLASHDROP_SKUS),
    quantity: z.number().int().min(1).max(5),
    couponCode: z.string().trim().min(1).max(32).regex(/^[A-Za-z0-9_-]+$/).optional(),
  })
  .strict();

export const QuoteSchema = z
  .object({
    currency: z.string().length(3),
    unitPriceCents: z.number().int().nonnegative(),
    discountCents: z.number().int().nonnegative(),
    totalCents: z.number().int().nonnegative(),
    priceVersion: z.string().min(1).max(64),
    quotedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export const CreateOrderResponseSchema = z
  .object({
    orderId: z.string().ulid(),
    traceId: z.string().ulid(),
    status: z.literal("pending"),
    quote: QuoteSchema,
    acceptedAt: z.string().datetime({ offset: true }),
    links: z.object({ self: z.string().min(1) }).strict(),
  })
  .strict();

export const OrderSchema = z
  .object({
    orderId: z.string().ulid(),
    traceId: z.string().ulid(),
    customerId: IdentifierSchema,
    sku: z.enum(FLASHDROP_SKUS),
    quantity: z.number().int().min(1).max(5),
    couponCode: z.string().max(32).optional(),
    status: z.enum(ORDER_STATUSES),
    quote: QuoteSchema,
    failureReason: z.string().max(512).optional(),
    createdAt: z.string().datetime({ offset: true }),
    updatedAt: z.string().datetime({ offset: true }),
    version: z.number().int().positive(),
  })
  .strict();

export const TraceEventSchema = z
  .object({
    id: z.string().ulid(),
    traceId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    orderId: z.string().ulid().optional(),
    correlationId: z.string().min(1).max(128).optional(),
    causationId: z.string().min(1).max(128).optional(),
    timestamp: z.string().datetime({ offset: true }),
    source: z.string().min(1).max(128),
    target: z.string().min(1).max(128).optional(),
    stage: z.string().min(1).max(128),
    status: z.enum(TRACE_STATUSES),
    transport: z.enum(TRACE_TRANSPORTS),
    summary: z.string().min(1).max(2_048),
    payload: z.record(z.unknown()).optional(),
  })
  .strict();

export const OrderEventEnvelopeSchema = z
  .object({
    schemaVersion: z.literal(1),
    eventId: z.string().ulid(),
    eventType: z.enum(ORDER_EVENT_TYPES),
    occurredAt: z.string().datetime({ offset: true }),
    traceId: z.string().ulid(),
    orderId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    aggregateVersion: z.number().int().positive(),
    data: z
      .object({
        sku: z.enum(FLASHDROP_SKUS),
        quantity: z.number().int().min(1).max(5),
        currency: z.string().length(3),
        totalCents: z.number().int().nonnegative(),
        status: z.enum(ORDER_STATUSES),
        reason: z.string().max(512).optional(),
      })
      .strict(),
  })
  .strict();

export const InventoryReservationCommandSchema = z
  .object({
    schemaVersion: z.literal(1),
    commandId: z.string().ulid(),
    traceId: z.string().ulid(),
    orderId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    sku: z.enum(FLASHDROP_SKUS),
    quantity: z.number().int().min(1).max(5),
    scenario: z.enum(LAB_SCENARIOS),
    createdAt: z.string().datetime({ offset: true }),
  })
  .strict();

export const InventoryReservationResultSchema = z
  .object({
    schemaVersion: z.literal(1),
    resultId: z.string().ulid(),
    commandId: z.string().ulid(),
    traceId: z.string().ulid(),
    orderId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    sku: z.enum(FLASHDROP_SKUS),
    quantity: z.number().int().min(1).max(5),
    status: z.enum(["reserved", "sold_out", "failed"]),
    remainingStock: z.number().int().nonnegative().optional(),
    reason: z.string().max(512).optional(),
    processedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export const InventoryItemSchema = z
  .object({
    sku: z.enum(FLASHDROP_SKUS),
    available: z.number().int().nonnegative(),
    reserved: z.number().int().nonnegative(),
    updatedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export const InventoryListResponseSchema = z
  .object({ items: z.array(InventoryItemSchema).max(FLASHDROP_SKUS.length) })
  .strict();

export const InventoryResetRequestSchema = z
  .object({
    items: z.array(z.object({
      sku: z.enum(FLASHDROP_SKUS),
      available: z.number().int().min(0).max(1_000_000),
    }).strict()).max(FLASHDROP_SKUS.length).optional(),
  })
  .strict();

export type LabScenario = (typeof LAB_SCENARIOS)[number];

export type CreateOrderRequest = z.infer<typeof CreateOrderRequestSchema>;

export type Quote = z.infer<typeof QuoteSchema>;

export type CreateOrderResponse = z.infer<typeof CreateOrderResponseSchema>;

export type Order = z.infer<typeof OrderSchema>;

export type TraceEvent = z.infer<typeof TraceEventSchema>;

export type OrderEventEnvelope = z.infer<typeof OrderEventEnvelopeSchema>;

export type InventoryReservationCommand = z.infer<typeof InventoryReservationCommandSchema>;

export type InventoryReservationResult = z.infer<typeof InventoryReservationResultSchema>;

export type InventoryListResponse = z.infer<typeof InventoryListResponseSchema>;

export type InventoryResetRequest = z.infer<typeof InventoryResetRequestSchema>;

const asOpenApiSchema = (schema: z.ZodTypeAny) =>
  zodToJsonSchema(schema, { target: "openApi3", $refStrategy: "none" });

export const OPENAPI_SCHEMAS = {
  CreateOrderRequest: asOpenApiSchema(CreateOrderRequestSchema),
  CreateOrderResponse: asOpenApiSchema(CreateOrderResponseSchema),
  Order: asOpenApiSchema(OrderSchema),
  TraceEvent: asOpenApiSchema(TraceEventSchema),
  OrderEventEnvelope: asOpenApiSchema(OrderEventEnvelopeSchema),
  InventoryReservationCommand: asOpenApiSchema(InventoryReservationCommandSchema),
  InventoryReservationResult: asOpenApiSchema(InventoryReservationResultSchema),
  InventoryListResponse: asOpenApiSchema(InventoryListResponseSchema),
  InventoryResetRequest: asOpenApiSchema(InventoryResetRequestSchema),
} as const;

export const MESSAGE_MODES = ["full", "kafka", "rabbit"] as const;

export const MetadataSchema = z.record(z.string().min(1).max(64), z.string().max(1_024));

export const MessageEnvelopeSchema = z.object({
  version: z.literal(1), messageId: z.string().ulid(), traceId: z.string().ulid(),
  message: z.string().min(1).max(4_096), mode: z.enum(MESSAGE_MODES),
  simulateFailure: z.boolean(), metadata: MetadataSchema.optional(),
  createdAt: z.string().datetime({ offset: true }), processorResult: z.string().max(8_192).optional(),
}).strict();

export type MessageEnvelope = z.infer<typeof MessageEnvelopeSchema>;
