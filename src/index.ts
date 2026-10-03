import { createApp } from "./app.js";
import { loadConfig } from "./config.js";
import { BrokerMessaging } from "./messaging.js";
import { GatewayMetrics } from "./metrics.js";
import { HttpProcessorClient } from "./processor.js";
import { PostgresGatewayStorage } from "./storage.js";
import { RedisIdempotencyCache } from "./idempotency.js";
import { HttpInventoryClient } from "./inventory.js";
import pino from "pino";

const config = loadConfig();
const metrics = new GatewayMetrics();

const bootstrapLogger = pino({
  name: "gateway",
  level: config.logLevel,
  redact: ["rabbitUrl", "redisUrl", "authorization", "cookie"],
});
const messaging = new BrokerMessaging(config, bootstrapLogger, metrics);
const processor = new HttpProcessorClient(config);
const storage = new PostgresGatewayStorage(config, { logger: bootstrapLogger });
const idempotency = new RedisIdempotencyCache(config, bootstrapLogger);
const inventory = new HttpInventoryClient(config);

await storage.start();
await idempotency.start();
const app = await createApp({
  config,
  messaging,
  processor,
  storage,
  idempotency,
  metrics,
  inventory,
});
let shuttingDown = false;

async function connectBrokers(): Promise<void> {
  let delayMs = 1_000;
  while (!shuttingDown && !messaging.isReady()) {
    try {
      await messaging.start();
      app.log.info("Outbox broker status probe connected");
      return;
    } catch (error) {
      app.log.warn({ err: error, retryInMs: delayMs }, "Brokers unavailable; HTTP acceptance remains enabled");
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      delayMs = Math.min(delayMs * 2, 30_000);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  app.log.info({ signal }, "graceful shutdown started");
  const forceExit = setTimeout(() => {
    app.log.fatal("graceful shutdown deadline exceeded");
    process.exit(1);
  }, 15_000).unref();
  await app.close();
  processor.close();
  await messaging.stop();
  await idempotency.stop();
  await storage.stop();
  clearTimeout(forceExit);
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
process.on("unhandledRejection", (error) => app.log.error({ err: error }, "unhandled rejection"));
process.on("uncaughtException", (error) => {
  app.log.fatal({ err: error }, "uncaught exception");
  void shutdown("uncaughtException");
});

await app.listen({ host: config.host, port: config.port });
void connectBrokers();
