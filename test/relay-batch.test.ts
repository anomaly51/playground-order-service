import { describe, expect, it, vi } from "vitest";
import { runTransportLanes } from "../src/relay-batch.js";
import type { OutboxRecord, OutboxTransport } from "../src/storage.js";

function record(
  outboxId: string,
  transport: OutboxTransport,
): OutboxRecord {
  return {
    outboxId,
    aggregateId: "01J00000000000000000000000",
    traceId: "01J00000000000000000000001",
    eventType: "test.event",
    transport,
    destination: `test.${transport}`,
    partitionKey: "order-1",
    payload: {},
    attempts: 1,
  };
}

describe("runTransportLanes", () => {
  it("lets RabbitMQ make progress while Kafka is blocked", async () => {
    let releaseKafka: (() => void) | undefined;
    const kafkaGate = new Promise<void>((resolve) => {
      releaseKafka = resolve;
    });
    const started: string[] = [];
    const completed: string[] = [];

    const operation = runTransportLanes([
      record("kafka-1", "kafka"),
      record("rabbit-1", "rabbitmq"),
      record("kafka-2", "kafka"),
      record("rabbit-2", "rabbitmq"),
    ], async (item) => {
      started.push(item.outboxId);
      if (item.outboxId === "kafka-1") await kafkaGate;
      completed.push(item.outboxId);
    });

    await vi.waitFor(() => {
      expect(completed).toEqual(["rabbit-1", "rabbit-2"]);
    });
    expect(started).not.toContain("kafka-2");

    releaseKafka?.();
    await operation;
    expect(completed).toEqual(["rabbit-1", "rabbit-2", "kafka-1", "kafka-2"]);
  });
});
