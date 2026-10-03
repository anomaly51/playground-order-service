import { Counter, Histogram, Registry, collectDefaultMetrics } from "@prometheus-io/client";

export class GatewayMetrics {
  readonly registry = new Registry();
  readonly requests = new Counter({
    name: "flashdrop_gateway_requests_total",
    help: "FlashDrop order requests by SKU, scenario and outcome",
    labelNames: ["sku", "scenario", "outcome"] as const,
    registers: [this.registry],
  });
  readonly actionDuration = new Histogram({
    name: "flashdrop_gateway_action_duration_seconds",
    help: "Time spent in a downstream gateway action",
    labelNames: ["action", "outcome"] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });
  readonly publishFailures = new Counter({
    name: "flashdrop_gateway_publish_failures_total",
    help: "Failed broker publications",
    labelNames: ["transport"] as const,
    registers: [this.registry],
  });
  readonly idempotencyHits = new Counter({
    name: "flashdrop_gateway_idempotency_hits_total",
    help: "Requests served from the idempotency cache",
    labelNames: ["backend"] as const,
    registers: [this.registry],
  });
  readonly idempotencyFallbacks = new Counter({
    name: "flashdrop_gateway_idempotency_fallbacks_total",
    help: "Redis idempotency operations completed by the local fallback",
    labelNames: ["action"] as const,
    registers: [this.registry],
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: "flashdrop_gateway_process_" });
  }
}
