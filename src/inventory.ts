import {
  InventoryListResponseSchema,
  InventoryResetRequestSchema,
  type InventoryListResponse,
  type InventoryResetRequest,
} from "./contracts.js";
import type { GatewayConfig } from "./config.js";

export interface InventoryClient {
  list(): Promise<InventoryListResponse>;
  reset(request: InventoryResetRequest): Promise<InventoryListResponse>;
}

export class HttpInventoryClient implements InventoryClient {
  constructor(private readonly config: GatewayConfig) {}

  list(): Promise<InventoryListResponse> {
    return this.request("GET", "/api/v1/inventory");
  }

  reset(request: InventoryResetRequest): Promise<InventoryListResponse> {
    return this.request("POST", "/api/v1/lab/inventory/reset", request);
  }

  private async request(
    method: "GET" | "POST",
    path: string,
    body?: InventoryResetRequest,
  ): Promise<InventoryListResponse> {
    const response = await fetch(`${this.config.inventoryServiceUrl}${path}`, {
      method,
      headers: { accept: "application/json", ...(body ? { "content-type": "application/json" } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(this.config.inventoryHttpTimeoutMs),
    });
    if (!response.ok) {
      const preview = (await response.text()).slice(0, 512);
      throw new Error(`Inventory service returned HTTP ${response.status}: ${preview}`);
    }
    return InventoryListResponseSchema.parse(await response.json());
  }
}

export function parseInventoryResetRequest(value: unknown): InventoryResetRequest {
  return InventoryResetRequestSchema.parse(value ?? {});
}
