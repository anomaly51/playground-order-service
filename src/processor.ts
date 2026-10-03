import {
  QuoteSchema,
  type CreateOrderRequest,
  type LabScenario,
  type Quote,
} from "./contracts.js";
import type { GatewayConfig } from "./config.js";

export interface ProcessorClient {
  quote(input: {
    traceId: string;
    order: CreateOrderRequest;
    scenario: LabScenario;
    runId?: string;
  }): Promise<Quote>;
  isReady(timeoutMs: number): Promise<boolean>;
  close(): void;
}

type ProcessorConfig = Pick<GatewayConfig,
  "processorHttpUrl" | "processorHttpTimeoutMs" | "labScenariosEnabled"
>;

export class HttpProcessorClient implements ProcessorClient {
  private readonly shutdown = new AbortController();

  constructor(private readonly config: ProcessorConfig) {}

  async quote(input: {
    traceId: string;
    order: CreateOrderRequest;
    scenario: LabScenario;
    runId?: string;
  }): Promise<Quote> {
    const response = await fetch(`${this.config.processorHttpUrl}/api/v1/quotes`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "x-correlation-id": input.traceId,
      },
      body: JSON.stringify({
        ...input.order,
        traceId: input.traceId,
        scenario: this.config.labScenariosEnabled ? input.scenario : "normal",
        ...(input.runId ? { runId: input.runId } : {}),
      }),
      redirect: "error",
      signal: AbortSignal.any([
        this.shutdown.signal,
        AbortSignal.timeout(this.config.processorHttpTimeoutMs),
      ]),
    });
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`Pricing service returned HTTP ${response.status}`);
    }
    const quote = QuoteSchema.safeParse(await response.json());
    if (!quote.success) throw new Error("Pricing service returned an invalid quote");
    return quote.data;
  }

  async isReady(timeoutMs: number): Promise<boolean> {
    try {
      const response = await fetch(`${this.config.processorHttpUrl}/readyz`, {
        headers: { accept: "application/json" },
        redirect: "error",
        signal: AbortSignal.any([this.shutdown.signal, AbortSignal.timeout(timeoutMs)]),
      });
      if (!response.ok) {
        await response.body?.cancel();
        return false;
      }
      const body = await response.json() as { status?: string; http?: boolean } | null;
      return body?.status === "ready" && body.http === true;
    } catch {
      return false;
    }
  }

  close(): void {
    this.shutdown.abort();
  }
}
