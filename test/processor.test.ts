import { createServer, type RequestListener, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { HttpProcessorClient } from "../src/processor.js";
import { loadConfig } from "../src/config.js";

const servers: Server[] = [];
const clients: HttpProcessorClient[] = [];
const input = {
  traceId: "01K3TQ1YW4H3G2VJ8ZA0Z5X8RM",
  order: {
    customerId: "customer-42", sku: "DROP-SNEAKER-RED" as const,
    quantity: 2, couponCode: "DROP10",
  },
  scenario: "normal" as const,
  runId: "test-run-42",
};
const quote = {
  currency: "USD", unitPriceCents: 18_900, discountCents: 3_780,
  totalCents: 34_020, priceVersion: "flashdrop-2026-08",
  quotedAt: "2026-08-30T10:00:00.000Z",
};

async function clientFor(handler: RequestListener, timeout = 500, scenarios = true) {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing test server address");
  const client = new HttpProcessorClient({
    processorHttpUrl: `http://127.0.0.1:${address.port}`,
    processorHttpTimeoutMs: timeout,
    labScenariosEnabled: scenarios,
  });
  clients.push(client);
  return client;
}

afterEach(async () => {
  clients.splice(0).forEach((client) => client.close());
  await Promise.all(servers.splice(0).map((server) => {
    server.closeAllConnections();
    return new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }));
});

describe("HTTP pricing client", () => {
  it("posts JSON, correlation metadata and returns a validated quote", async () => {
    let received: unknown;
    let path: string | undefined;
    let method: string | undefined;
    let correlation: string | string[] | undefined;
    const client = await clientFor(async (request, response) => {
      let body = "";
      for await (const chunk of request) body += chunk;
      received = JSON.parse(body);
      path = request.url;
      method = request.method;
      correlation = request.headers["x-correlation-id"];
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify(quote));
    });
    expect(await client.quote(input)).toEqual(quote);
    expect(method).toBe("POST");
    expect(path).toBe("/api/v1/quotes");
    expect(correlation).toBe(input.traceId);
    expect(received).toEqual({ ...input.order, traceId: input.traceId, scenario: "normal", runId: input.runId });
  });

  it("disables fault injection when lab scenarios are disabled", async () => {
    let scenario: unknown;
    const client = await clientFor(async (request, response) => {
      let body = "";
      for await (const chunk of request) body += chunk;
      scenario = JSON.parse(body).scenario;
      response.end(JSON.stringify(quote));
    }, 500, false);
    await client.quote({ ...input, scenario: "pricing-error" });
    expect(scenario).toBe("normal");
  });

  it.each([400, 500, 504])("rejects HTTP %i without treating it as a quote", async (status) => {
    const client = await clientFor((_request, response) => {
      response.writeHead(status).end(JSON.stringify({ error: "pricing_error" }));
    });
    await expect(client.quote(input)).rejects.toThrow(`HTTP ${status}`);
  });

  it.each(["not-json", JSON.stringify({ ...quote, totalCents: "34020" }), "{}"]) (
    "rejects malformed upstream responses: %s", async (body) => {
      const client = await clientFor((_request, response) => response.end(body));
      await expect(client.quote(input)).rejects.toBeInstanceOf(Error);
    },
  );

  it("aborts a request that exceeds its deadline", async () => {
    const client = await clientFor(() => {}, 30);
    await expect(client.quote(input)).rejects.toMatchObject({ name: "TimeoutError" });
  });

  it("also bounds a response whose headers arrive but body stalls", async () => {
    const client = await clientFor((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.flushHeaders();
    }, 30);
    await expect(client.quote(input)).rejects.toBeInstanceOf(Error);
  });

  it.each([
    [200, { status: "ready", http: true, kafkaTelemetry: false }, true],
    [200, { status: "not-ready", http: false }, false],
    [200, { status: "ready" }, false],
    [503, { status: "ready", http: true }, false],
  ])("checks readiness status and body (%i, %j)", async (status, body, expected) => {
    let path: string | undefined;
    const client = await clientFor((request, response) => {
      path = request.url;
      response.writeHead(status).end(JSON.stringify(body));
    });
    expect(await client.isReady(100)).toBe(expected);
    expect(path).toBe("/readyz");
  });

  it("returns false on a readiness timeout", async () => {
    const client = await clientFor(() => {});
    expect(await client.isReady(30)).toBe(false);
  });

  it("cancels outstanding requests on close", async () => {
    let accepted!: () => void;
    const started = new Promise<void>((resolve) => { accepted = resolve; });
    const client = await clientFor(() => accepted());
    const pending = expect(client.quote(input)).rejects.toMatchObject({ name: "AbortError" });
    await started;
    client.close();
    await pending;
    expect(await client.isReady(100)).toBe(false);
  });

  it("validates and normalizes the configured HTTP endpoint", () => {
    expect(loadConfig({ PROCESSOR_HTTP_URL: "https://pricing.example/" }).processorHttpUrl)
      .toBe("https://pricing.example");
    expect(() => loadConfig({ PROCESSOR_HTTP_URL: "ftp://pricing.example" })).toThrow("http:// or https://");
  });
});
