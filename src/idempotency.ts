import {
  CreateOrderResponseSchema,
  type CreateOrderResponse,
} from "./contracts.js";
import { createHash } from "node:crypto";
import { createClient } from "redis";
import type { GatewayConfig } from "./config.js";

type TerminalEntry =
  | {
      state: "completed";
      fingerprint: string;
      response: CreateOrderResponse;
      expiresAt: number;
    }
  | {
      state: "failed";
      fingerprint: string;
      expiresAt: number;
    };

interface ProcessingEntry {
  fingerprint: string;
  owner: string;
  expiresAt: number;
}

export class IdempotencyConflictError extends Error {
  constructor() {
    super("Idempotency-Key was already used with a different request");
    this.name = "IdempotencyConflictError";
  }
}

export class IdempotencyInProgressError extends Error {
  constructor() {
    super("A request with this Idempotency-Key is already being processed");
    this.name = "IdempotencyInProgressError";
  }
}

export class IdempotencyPreviousAttemptFailedError extends Error {
  constructor() {
    super("A previous request with this Idempotency-Key may have completed some side effects");
    this.name = "IdempotencyPreviousAttemptFailedError";
  }
}

export type IdempotencyBackend = "redis" | "memory";

export type IdempotencyReservation =
  | { kind: "claimed"; backend: IdempotencyBackend }
  | { kind: "cached"; backend: IdempotencyBackend; response: CreateOrderResponse };

export interface RateLimitDecision {
  allowed: boolean;
  backend: IdempotencyBackend;
  remaining: number;
  retryAfterMs: number;
}

export interface IdempotencyWrite {
  backend: IdempotencyBackend;
}

export interface GatewayIdempotencyCache {
  start(): Promise<void>;
  stop(): Promise<void>;
  isReady(timeoutMs: number): Promise<boolean>;
  reserve(
    key: string,
    fingerprint: string,
    owner: string,
  ): Promise<IdempotencyReservation>;
  complete(
    key: string,
    fingerprint: string,
    owner: string,
    response: CreateOrderResponse,
  ): Promise<IdempotencyWrite>;
  fail(
    key: string,
    fingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite>;
  renew(
    key: string,
    fingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite>;
  release(
    key: string,
    fingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite>;
  consumeRateLimit(subject: string): Promise<RateLimitDecision>;
}

type LocalReservation =
  | { kind: "claimed" }
  | { kind: "cached"; response: CreateOrderResponse };

export class IdempotencyStore {
  private readonly terminal = new Map<string, TerminalEntry>();
  private readonly processing = new Map<string, ProcessingEntry>();

  constructor(
    private readonly ttlMs: number,
    private readonly maxEntries: number,
    private readonly inFlightTtlMs = ttlMs,
  ) {}

  get(key: string, fingerprint: string, now = Date.now()): CreateOrderResponse | undefined {
    const entry = this.terminal.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= now) {
      this.terminal.delete(key);
      return undefined;
    }
    if (entry.fingerprint !== fingerprint) throw new IdempotencyConflictError();
    if (entry.state === "failed") throw new IdempotencyPreviousAttemptFailedError();
    this.terminal.delete(key);
    this.terminal.set(key, entry);
    return entry.response;
  }

  set(key: string, fingerprint: string, response: CreateOrderResponse, now = Date.now()): void {
    this.processing.delete(key);
    this.terminal.delete(key);
    this.terminal.set(key, {
      state: "completed",
      fingerprint,
      response,
      expiresAt: now + this.ttlMs,
    });
    this.trimTerminalEntries();
  }

  fail(key: string, fingerprint: string, owner: string, now = Date.now()): void {
    const current = this.processing.get(key);
    if (current) {
      if (current.fingerprint !== fingerprint) throw new IdempotencyConflictError();
      if (current.owner !== owner) throw new IdempotencyInProgressError();
    }
    this.processing.delete(key);
    this.terminal.delete(key);
    this.terminal.set(key, {
      state: "failed",
      fingerprint,
      expiresAt: now + this.ttlMs,
    });
    this.trimTerminalEntries();
  }

  renew(key: string, fingerprint: string, owner: string, now = Date.now()): boolean {
    const current = this.processing.get(key);
    if (
      !current
      || current.fingerprint !== fingerprint
      || current.owner !== owner
    ) {
      return false;
    }
    current.expiresAt = now + this.inFlightTtlMs;
    return true;
  }

  private trimTerminalEntries(): void {
    while (this.terminal.size > this.maxEntries) {
      const oldest = this.terminal.keys().next().value as string | undefined;
      if (!oldest) break;
      this.terminal.delete(oldest);
    }
  }

  reserve(
    key: string,
    fingerprint: string,
    owner: string,
    now = Date.now(),
  ): LocalReservation {
    const cached = this.get(key, fingerprint, now);
    if (cached) return { kind: "cached", response: cached };

    const current = this.processing.get(key);
    if (current?.expiresAt !== undefined && current.expiresAt <= now) {
      this.processing.delete(key);
    } else if (current) {
      if (current.fingerprint !== fingerprint) throw new IdempotencyConflictError();
      throw new IdempotencyInProgressError();
    }

    this.processing.set(key, {
      fingerprint,
      owner,
      expiresAt: now + this.inFlightTtlMs,
    });
    return { kind: "claimed" };
  }

  complete(
    key: string,
    fingerprint: string,
    owner: string,
    response: CreateOrderResponse,
    now = Date.now(),
  ): void {
    const current = this.processing.get(key);
    if (current) {
      if (current.fingerprint !== fingerprint) throw new IdempotencyConflictError();
      if (current.owner !== owner) throw new IdempotencyInProgressError();
    }
    this.set(key, fingerprint, response, now);
  }

  release(key: string, fingerprint: string, owner: string): void {
    const current = this.processing.get(key);
    if (
      current
      && current.fingerprint === fingerprint
      && current.owner === owner
    ) {
      this.processing.delete(key);
    }
  }
}

type RedisClient = ReturnType<typeof createClient>;

interface CacheLogger {
  info(bindings: Record<string, unknown>, message: string): void;
  warn(bindings: Record<string, unknown>, message: string): void;
}

interface RedisProcessingEntry {
  state: "processing";
  fingerprint: string;
  owner: string;
}

interface RedisCompletedEntry {
  state: "completed";
  fingerprint: string;
  response: CreateOrderResponse;
}

interface RedisFailedEntry {
  state: "failed";
  fingerprint: string;
}

type RedisEntry = RedisProcessingEntry | RedisCompletedEntry | RedisFailedEntry;

const TERMINAL_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
  return 1
end
local entry = cjson.decode(raw)
if entry.state ~= 'processing' or entry.owner ~= ARGV[1] or entry.fingerprint ~= ARGV[2] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
return 1
`;

const RENEW_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local entry = cjson.decode(raw)
if entry.state ~= 'processing' or entry.owner ~= ARGV[1] or entry.fingerprint ~= ARGV[2] then
  return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[3])
`;

const RELEASE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local entry = cjson.decode(raw)
if entry.state ~= 'processing' or entry.owner ~= ARGV[1] or entry.fingerprint ~= ARGV[2] then
  return 0
end
return redis.call('DEL', KEYS[1])
`;

const RATE_LIMIT_SCRIPT = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {count, ttl}
`;

function redisKey(prefix: string, externalKey: string): string {
  const digest = createHash("sha256").update(externalKey).digest("hex");
  return prefix + digest;
}

function parseRedisEntry(value: string): RedisEntry {
  const candidate = JSON.parse(value) as unknown;
  if (!candidate || typeof candidate !== "object") {
    throw new Error("Redis idempotency entry is not an object");
  }
  const record = candidate as Record<string, unknown>;
  if (typeof record.fingerprint !== "string") {
    throw new Error("Redis idempotency entry has no fingerprint");
  }
  if (record.state === "processing" && typeof record.owner === "string") {
    return {
      state: "processing",
      fingerprint: record.fingerprint,
      owner: record.owner,
    };
  }
  if (record.state === "completed") {
    return {
      state: "completed",
      fingerprint: record.fingerprint,
      response: CreateOrderResponseSchema.parse(record.response),
    };
  }
  if (record.state === "failed") {
    return {
      state: "failed",
      fingerprint: record.fingerprint,
    };
  }
  throw new Error("Redis idempotency entry has an unknown state");
}

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function withRedisCommand<T>(
  client: RedisClient,
  timeoutMs: number,
  message: string,
  operation: (abortableClient: RedisClient) => Promise<T>,
): Promise<T> {
  const controller = new AbortController();
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation(client.withAbortSignal(controller.signal)),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          const error = new Error(message);
          controller.abort(error);
          reject(error);
        }, timeoutMs);
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Shared idempotency cache with an owner-safe reservation and a bounded local
 * fallback. Redis prevents concurrent replicas from performing the same side
 * effects. During a Redis outage this single-replica lab keeps accepting
 * traffic, while explicitly reporting degraded idempotency in traces/readiness.
 */
export class RedisIdempotencyCache implements GatewayIdempotencyCache {
  private readonly client: RedisClient;
  private readonly fallback: IdempotencyStore;
  private readonly ttlMs: number;
  private readonly inFlightTtlMs: number;
  private readonly keyPrefix: string;
  private readonly connectTimeoutMs: number;
  private readonly commandTimeoutMs: number;
  private readonly rateLimitPrefix: string;
  private readonly rateLimitRequests: number;
  private readonly rateLimitWindowMs: number;
  private readonly localRateLimits = new Map<string, { count: number; resetAt: number }>();
  private connectPromise?: Promise<void>;

  constructor(
    config: GatewayConfig,
    private readonly logger: CacheLogger,
    client?: RedisClient,
  ) {
    this.ttlMs = config.idempotencyTtlMs;
    this.inFlightTtlMs = config.idempotencyInFlightTtlMs;
    this.keyPrefix = config.redisKeyPrefix;
    this.connectTimeoutMs = config.redisConnectTimeoutMs;
    this.commandTimeoutMs = config.redisCommandTimeoutMs;
    this.rateLimitPrefix = config.redisRateLimitPrefix;
    this.rateLimitRequests = config.rateLimitRequests;
    this.rateLimitWindowMs = config.rateLimitWindowMs;
    this.fallback = new IdempotencyStore(
      config.idempotencyTtlMs,
      config.idempotencyMaxEntries,
      config.idempotencyInFlightTtlMs,
    );
    this.client = client ?? createClient({
      url: config.redisUrl,
      disableOfflineQueue: true,
      commandsQueueMaxLength: config.idempotencyMaxEntries,
      socket: {
        connectTimeout: config.redisConnectTimeoutMs,
        reconnectStrategy: (retries) => Math.min(100 * 2 ** retries, 5_000),
      },
    });
    this.client.on("error", (error) => {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Redis idempotency cache error; local fallback remains active",
      );
    });
    this.client.on("ready", () => {
      this.logger.info({}, "Redis idempotency cache connected");
    });
  }

  async start(): Promise<void> {
    if (this.client.isOpen || this.connectPromise) return;
    const operation = this.client.connect().then(() => undefined);
    this.connectPromise = operation;
    void operation.finally(() => {
      if (this.connectPromise === operation) this.connectPromise = undefined;
    }).catch(() => undefined);
    try {
      await withTimeout(
        operation,
        this.connectTimeoutMs,
        "Redis initial connection timed out",
      );
    } catch (error) {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Gateway started with local idempotency fallback",
      );
    }
  }

  async reserve(
    key: string,
    requestFingerprint: string,
    owner: string,
  ): Promise<IdempotencyReservation> {
    if (this.client.isReady) {
      try {
        const storageKey = redisKey(this.keyPrefix, key);
        const processingValue = JSON.stringify({
          state: "processing",
          fingerprint: requestFingerprint,
          owner,
        } satisfies RedisProcessingEntry);

        for (let attempt = 0; attempt < 2; attempt += 1) {
          const claimed = await withRedisCommand(
            this.client,
            this.commandTimeoutMs,
            "Redis reservation timed out",
            (client) => client.set(storageKey, processingValue, {
              condition: "NX",
              expiration: { type: "PX", value: this.inFlightTtlMs },
            }),
          );
          if (claimed === "OK") {
            let local: LocalReservation;
            try {
              local = this.fallback.reserve(key, requestFingerprint, owner);
            } catch (error) {
              await this.release(key, requestFingerprint, owner);
              throw error;
            }
            if (local.kind === "cached") {
              await this.complete(key, requestFingerprint, owner, local.response);
              return { kind: "cached", backend: "memory", response: local.response };
            }
            return { kind: "claimed", backend: "redis" };
          }

          const value = await withRedisCommand(
            this.client,
            this.commandTimeoutMs,
            "Redis reservation lookup timed out",
            (client) => client.get(storageKey),
          );
          if (!value) continue;
          const entry = parseRedisEntry(value);
          if (entry.fingerprint !== requestFingerprint) {
            throw new IdempotencyConflictError();
          }
          if (entry.state === "processing") {
            throw new IdempotencyInProgressError();
          }
          if (entry.state === "failed") {
            throw new IdempotencyPreviousAttemptFailedError();
          }
          this.fallback.set(key, requestFingerprint, entry.response);
          return { kind: "cached", backend: "redis", response: entry.response };
        }
        throw new Error("Redis reservation changed before it could be inspected");
      } catch (error) {
        if (
          error instanceof IdempotencyConflictError
          || error instanceof IdempotencyInProgressError
          || error instanceof IdempotencyPreviousAttemptFailedError
        ) {
          throw error;
        }
        this.logger.warn(
          { err: error instanceof Error ? error.message : String(error) },
          "Redis idempotency reservation failed; using local fallback",
        );
      }
    }

    const local = this.fallback.reserve(key, requestFingerprint, owner);
    return local.kind === "cached"
      ? { kind: "cached", backend: "memory", response: local.response }
      : { kind: "claimed", backend: "memory" };
  }

  async complete(
    key: string,
    requestFingerprint: string,
    owner: string,
    response: CreateOrderResponse,
  ): Promise<IdempotencyWrite> {
    this.fallback.complete(key, requestFingerprint, owner, response);
    if (!this.client.isReady) return { backend: "memory" };
    try {
      const completedValue = JSON.stringify({
        state: "completed",
        fingerprint: requestFingerprint,
        response,
      } satisfies RedisCompletedEntry);
      const updated = await withRedisCommand(
        this.client,
        this.commandTimeoutMs,
        "Redis completion timed out",
        (client) => client.eval(TERMINAL_SCRIPT, {
          keys: [redisKey(this.keyPrefix, key)],
          arguments: [owner, requestFingerprint, completedValue, String(this.ttlMs)],
        }),
      );
      if (updated === 1) return { backend: "redis" };
      this.logger.warn(
        { owner },
        "Redis reservation ownership changed; local completed response retained",
      );
    } catch (error) {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Redis idempotency completion failed; local completed response retained",
      );
    }
    return { backend: "memory" };
  }

  async fail(
    key: string,
    requestFingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite> {
    this.fallback.fail(key, requestFingerprint, owner);
    if (!this.client.isReady) return { backend: "memory" };
    try {
      const failedValue = JSON.stringify({
        state: "failed",
        fingerprint: requestFingerprint,
      } satisfies RedisFailedEntry);
      const updated = await withRedisCommand(
        this.client,
        this.commandTimeoutMs,
        "Redis failure tombstone timed out",
        (client) => client.eval(TERMINAL_SCRIPT, {
          keys: [redisKey(this.keyPrefix, key)],
          arguments: [owner, requestFingerprint, failedValue, String(this.ttlMs)],
        }),
      );
      if (updated === 1) return { backend: "redis" };
      this.logger.warn(
        { owner },
        "Redis reservation ownership changed; local failure tombstone retained",
      );
    } catch (error) {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Redis failure tombstone write failed; local tombstone retained",
      );
    }
    return { backend: "memory" };
  }

  async renew(
    key: string,
    requestFingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite> {
    this.fallback.renew(key, requestFingerprint, owner);
    if (!this.client.isReady) return { backend: "memory" };
    try {
      const renewed = await withRedisCommand(
        this.client,
        this.commandTimeoutMs,
        "Redis reservation renewal timed out",
        (client) => client.eval(RENEW_SCRIPT, {
          keys: [redisKey(this.keyPrefix, key)],
          arguments: [owner, requestFingerprint, String(this.inFlightTtlMs)],
        }),
      );
      if (renewed === 1) return { backend: "redis" };
      this.logger.warn(
        { owner },
        "Redis reservation could not be renewed; local lease remains active",
      );
    } catch (error) {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Redis reservation renewal failed; local lease remains active",
      );
    }
    return { backend: "memory" };
  }

  async release(
    key: string,
    requestFingerprint: string,
    owner: string,
  ): Promise<IdempotencyWrite> {
    this.fallback.release(key, requestFingerprint, owner);
    if (!this.client.isReady) return { backend: "memory" };
    try {
      const released = await withRedisCommand(
        this.client,
        this.commandTimeoutMs,
        "Redis reservation release timed out",
        (client) => client.eval(RELEASE_SCRIPT, {
          keys: [redisKey(this.keyPrefix, key)],
          arguments: [owner, requestFingerprint],
        }),
      );
      return { backend: released === 1 ? "redis" : "memory" };
    } catch (error) {
      this.logger.warn(
        { err: error instanceof Error ? error.message : String(error) },
        "Redis reservation release failed; it will expire automatically",
      );
      return { backend: "memory" };
    }
  }

  async consumeRateLimit(subject: string): Promise<RateLimitDecision> {
    if (this.client.isReady) {
      try {
        const raw = await withRedisCommand(
          this.client,
          this.commandTimeoutMs,
          "Redis rate-limit check timed out",
          (client) => client.eval(RATE_LIMIT_SCRIPT, {
            keys: [redisKey(this.rateLimitPrefix, subject)],
            arguments: [String(this.rateLimitWindowMs)],
          }),
        );
        const values = raw as Array<number | string>;
        const count = Number(values[0]);
        const ttl = Math.max(0, Number(values[1]));
        if (!Number.isFinite(count) || !Number.isFinite(ttl)) {
          throw new Error("Redis returned an invalid rate-limit decision");
        }
        return {
          allowed: count <= this.rateLimitRequests,
          backend: "redis",
          remaining: Math.max(0, this.rateLimitRequests - count),
          retryAfterMs: count <= this.rateLimitRequests ? 0 : ttl,
        };
      } catch (error) {
        this.logger.warn(
          { err: error instanceof Error ? error.message : String(error) },
          "Redis rate limiter failed; using single-replica fallback",
        );
      }
    }

    const now = Date.now();
    const localSubject = createHash("sha256").update(subject).digest("hex");
    const current = this.localRateLimits.get(localSubject);
    const entry = !current || current.resetAt <= now
      ? { count: 1, resetAt: now + this.rateLimitWindowMs }
      : { count: current.count + 1, resetAt: current.resetAt };
    this.localRateLimits.set(localSubject, entry);
    while (this.localRateLimits.size > this.rateLimitRequests * 100) {
      const oldest = this.localRateLimits.keys().next().value as string | undefined;
      if (!oldest) break;
      this.localRateLimits.delete(oldest);
    }
    return {
      allowed: entry.count <= this.rateLimitRequests,
      backend: "memory",
      remaining: Math.max(0, this.rateLimitRequests - entry.count),
      retryAfterMs: entry.count <= this.rateLimitRequests ? 0 : entry.resetAt - now,
    };
  }

  async isReady(timeoutMs: number): Promise<boolean> {
    if (!this.client.isReady) return false;
    try {
      return await withRedisCommand(
        this.client,
        timeoutMs,
        "Redis readiness PING timed out",
        async (client) => (await client.ping()) === "PONG",
      );
    } catch {
      return false;
    }
  }

  async stop(): Promise<void> {
    if (!this.client.isOpen) return;
    try {
      await withTimeout(this.client.close(), 1_000, "Redis close timed out");
    } catch {
      this.client.destroy();
    }
  }
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, canonicalize(entry)]),
    );
  }
  return value;
}

export function fingerprint(value: unknown): string {
  const canonical = JSON.stringify(canonicalize(value));
  return createHash("sha256").update(canonical).digest("hex");
}
