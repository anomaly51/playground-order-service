import { TraceEventSchema, type TraceEvent } from "./contracts.js";
import { ulid } from "ulid";
import type { FastifyBaseLogger } from "fastify";

export type TraceEventInput = Omit<TraceEvent, "id" | "timestamp">;

export interface TraceSink {
  publishTrace(event: TraceEvent): Promise<void>;
}

export class TraceEmitter {
  constructor(
    private readonly sink: TraceSink,
    private readonly logger: FastifyBaseLogger,
  ) {}

  async emit(input: TraceEventInput): Promise<void> {
    const event = TraceEventSchema.parse({
      ...input,
      id: ulid(),
      timestamp: new Date().toISOString(),
    });
    try {
      await this.sink.publishTrace(event);
    } catch (error) {
      this.logger.error({ err: error, traceId: event.traceId, stage: event.stage }, "trace publish failed");
    }
  }
}

export async function runTraced<T>(options: {
  emitter: TraceEmitter;
  traceId: string;
  source: string;
  target: string;
  transport: TraceEvent["transport"];
  stage: string;
  summary: string;
  action: () => Promise<T>;
  payload?: Record<string, unknown>;
  runId?: string;
  orderId?: string;
  correlationId?: string;
  causationId?: string;
}): Promise<T> {
  const startedAt = performance.now();
  await options.emitter.emit({
    traceId: options.traceId,
    source: options.source,
    target: options.target,
    transport: options.transport,
    stage: options.stage,
    status: "started",
    summary: `${options.summary} started`,
    ...(options.runId ? { runId: options.runId } : {}),
    ...(options.orderId ? { orderId: options.orderId } : {}),
    ...(options.correlationId ? { correlationId: options.correlationId } : {}),
    ...(options.causationId ? { causationId: options.causationId } : {}),
    ...(options.payload ? { payload: options.payload } : {}),
  });
  try {
    const result = await options.action();
    await options.emitter.emit({
      traceId: options.traceId,
      source: options.source,
      target: options.target,
      transport: options.transport,
      stage: options.stage,
      status: "succeeded",
      summary: `${options.summary} succeeded`,
      ...(options.runId ? { runId: options.runId } : {}),
      ...(options.orderId ? { orderId: options.orderId } : {}),
      ...(options.correlationId ? { correlationId: options.correlationId } : {}),
      ...(options.causationId ? { causationId: options.causationId } : {}),
      payload: {
        ...options.payload,
        durationMs: Math.round((performance.now() - startedAt) * 100) / 100,
      },
    });
    return result;
  } catch (error) {
    await options.emitter.emit({
      traceId: options.traceId,
      source: options.source,
      target: options.target,
      transport: options.transport,
      stage: options.stage,
      status: "failed",
      summary: `${options.summary} failed`,
      ...(options.runId ? { runId: options.runId } : {}),
      ...(options.orderId ? { orderId: options.orderId } : {}),
      ...(options.correlationId ? { correlationId: options.correlationId } : {}),
      ...(options.causationId ? { causationId: options.causationId } : {}),
      payload: {
        ...options.payload,
        durationMs: Math.round((performance.now() - startedAt) * 100) / 100,
        error: error instanceof Error ? error.message : String(error),
      },
    });
    throw error;
  }
}
