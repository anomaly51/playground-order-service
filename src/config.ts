import { z } from "zod";

const booleanFromEnvironment = z
  .enum(["true", "false"])
  .default("false")
  .transform((value) => value === "true");

const schema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  LOG_LEVEL: z.string().default("info"),
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().min(1).max(65_535).default(3_000),
  CORS_ORIGINS: z.string().default("http://localhost:5173,http://localhost:4173"),
  KAFKA_BROKERS: z.string().default("kafka:9092"),
  KAFKA_CLIENT_ID: z.string().default("lab-gateway"),
  KAFKA_SSL: booleanFromEnvironment,
  KAFKA_SASL_MECHANISM: z.enum(["plain", "scram-sha-256", "scram-sha-512"]).optional(),
  KAFKA_SASL_USERNAME: z.string().optional(),
  KAFKA_SASL_PASSWORD: z.string().optional(),
  RABBITMQ_URL: z.string().default("amqp://guest:guest@rabbitmq:5672"),
  REDIS_URL: z.string().default("redis://redis:6379/0"),
  REDIS_CONNECT_TIMEOUT_MS: z.coerce.number().int().min(100).max(5_000).default(500),
  REDIS_COMMAND_TIMEOUT_MS: z.coerce.number().int().min(50).max(5_000).default(250),
  REDIS_KEY_PREFIX: z.string().min(1).max(128).default("flashdrop:idempotency:"),
  REDIS_RATE_LIMIT_PREFIX: z.string().min(1).max(128).default("flashdrop:rate-limit:"),
  RATE_LIMIT_REQUESTS: z.coerce.number().int().min(1).max(100_000).default(100),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().min(100).max(3_600_000).default(1_000),
  POSTGRES_URL: z.string().default("postgresql://airflow:airflow@postgres:5432/airflow"),
  POSTGRES_POOL_SIZE: z.coerce.number().int().min(1).max(100).default(10),
  POSTGRES_CONNECTION_TIMEOUT_MS: z.coerce.number().int().min(100).max(30_000).default(5_000),
  PROCESSOR_HTTP_URL: z.string().url().default("http://processor:8001"),
  INVENTORY_SERVICE_URL: z.string().url().default("http://rabbit-worker:3004"),
  INVENTORY_HTTP_TIMEOUT_MS: z.coerce.number().int().min(100).max(30_000).default(2_000),
  PROCESSOR_HTTP_TIMEOUT_MS: z.coerce.number().int().min(100).max(30_000).default(3_000),
  READINESS_TIMEOUT_MS: z.coerce.number().int().min(50).max(5_000).default(500),
  REQUEST_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(60_000).default(15_000),
  IDEMPOTENCY_TTL_MS: z.coerce.number().int().min(1_000).default(600_000),
  IDEMPOTENCY_IN_FLIGHT_TTL_MS: z.coerce.number().int().min(1_000).default(30_000),
  IDEMPOTENCY_MAX_ENTRIES: z.coerce.number().int().min(10).max(100_000).default(10_000),
  LAB_SCENARIOS_ENABLED: z.enum(["true", "false"]).default("true").transform((value) => value === "true"),
  OUTBOX_POLL_MS: z.coerce.number().int().min(25).max(60_000).default(100),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(1_000).default(100),
  OUTBOX_RETRY_MAX_MS: z.coerce.number().int().min(100).max(300_000).default(30_000),
  RELAY_OPERATIONS_HOST: z.string().default("0.0.0.0"),
  RELAY_OPERATIONS_PORT: z.coerce.number().int().min(1).max(65_535).default(3_005),
});

export interface GatewayConfig {
  nodeEnv: "development" | "test" | "production";
  logLevel: string;
  host: string;
  port: number;
  corsOrigins: string[];
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaSsl: boolean;
  kafkaSasl?: {
    mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
    username: string;
    password: string;
  };
  rabbitUrl: string;
  redisUrl: string;
  redisConnectTimeoutMs: number;
  redisCommandTimeoutMs: number;
  redisKeyPrefix: string;
  redisRateLimitPrefix: string;
  rateLimitRequests: number;
  rateLimitWindowMs: number;
  postgresUrl: string;
  postgresPoolSize: number;
  postgresConnectionTimeoutMs: number;
  processorHttpUrl: string;
  inventoryServiceUrl: string;
  inventoryHttpTimeoutMs: number;
  processorHttpTimeoutMs: number;
  readinessTimeoutMs: number;
  requestTimeoutMs: number;
  idempotencyTtlMs: number;
  idempotencyInFlightTtlMs: number;
  idempotencyMaxEntries: number;
  labScenariosEnabled: boolean;
  outboxPollMs: number;
  outboxBatchSize: number;
  outboxRetryMaxMs: number;
  relayOperationsHost: string;
  relayOperationsPort: number;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): GatewayConfig {
  const parsed = schema.parse(environment);
  const processorUrl = new URL(parsed.PROCESSOR_HTTP_URL);
  if (processorUrl.protocol !== "http:" && processorUrl.protocol !== "https:") {
    throw new Error("PROCESSOR_HTTP_URL must use http:// or https://");
  }
  const postgresUrl = new URL(parsed.POSTGRES_URL);
  if (postgresUrl.protocol !== "postgres:" && postgresUrl.protocol !== "postgresql:") {
    throw new Error("POSTGRES_URL must use postgres:// or postgresql://");
  }
  const redisUrl = new URL(parsed.REDIS_URL);
  if (redisUrl.protocol !== "redis:" && redisUrl.protocol !== "rediss:") {
    throw new Error("REDIS_URL must use redis:// or rediss://");
  }
  const kafkaSasl = parsed.KAFKA_SASL_MECHANISM
    ? {
        mechanism: parsed.KAFKA_SASL_MECHANISM,
        username: parsed.KAFKA_SASL_USERNAME ?? "",
        password: parsed.KAFKA_SASL_PASSWORD ?? "",
      }
    : undefined;

  if (kafkaSasl && (!kafkaSasl.username || !kafkaSasl.password)) {
    throw new Error("Kafka SASL username and password are required when SASL is enabled");
  }

  return {
    nodeEnv: parsed.NODE_ENV,
    logLevel: parsed.LOG_LEVEL,
    host: parsed.HOST,
    port: parsed.PORT,
    corsOrigins: parsed.CORS_ORIGINS.split(",").map((value) => value.trim()).filter(Boolean),
    kafkaBrokers: parsed.KAFKA_BROKERS.split(",").map((value) => value.trim()).filter(Boolean),
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaSsl: parsed.KAFKA_SSL,
    ...(kafkaSasl ? { kafkaSasl } : {}),
    rabbitUrl: parsed.RABBITMQ_URL,
    redisUrl: parsed.REDIS_URL,
    redisConnectTimeoutMs: parsed.REDIS_CONNECT_TIMEOUT_MS,
    redisCommandTimeoutMs: parsed.REDIS_COMMAND_TIMEOUT_MS,
    redisKeyPrefix: parsed.REDIS_KEY_PREFIX,
    redisRateLimitPrefix: parsed.REDIS_RATE_LIMIT_PREFIX,
    rateLimitRequests: parsed.RATE_LIMIT_REQUESTS,
    rateLimitWindowMs: parsed.RATE_LIMIT_WINDOW_MS,
    postgresUrl: parsed.POSTGRES_URL,
    postgresPoolSize: parsed.POSTGRES_POOL_SIZE,
    postgresConnectionTimeoutMs: parsed.POSTGRES_CONNECTION_TIMEOUT_MS,
    processorHttpUrl: parsed.PROCESSOR_HTTP_URL.replace(/\/+$/, ""),
    inventoryServiceUrl: parsed.INVENTORY_SERVICE_URL.replace(/\/$/, ""),
    inventoryHttpTimeoutMs: parsed.INVENTORY_HTTP_TIMEOUT_MS,
    processorHttpTimeoutMs: parsed.PROCESSOR_HTTP_TIMEOUT_MS,
    readinessTimeoutMs: parsed.READINESS_TIMEOUT_MS,
    requestTimeoutMs: parsed.REQUEST_TIMEOUT_MS,
    idempotencyTtlMs: parsed.IDEMPOTENCY_TTL_MS,
    idempotencyInFlightTtlMs: parsed.IDEMPOTENCY_IN_FLIGHT_TTL_MS,
    idempotencyMaxEntries: parsed.IDEMPOTENCY_MAX_ENTRIES,
    labScenariosEnabled: parsed.LAB_SCENARIOS_ENABLED,
    outboxPollMs: parsed.OUTBOX_POLL_MS,
    outboxBatchSize: parsed.OUTBOX_BATCH_SIZE,
    outboxRetryMaxMs: parsed.OUTBOX_RETRY_MAX_MS,
    relayOperationsHost: parsed.RELAY_OPERATIONS_HOST,
    relayOperationsPort: parsed.RELAY_OPERATIONS_PORT,
  };
}
