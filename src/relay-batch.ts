import type { OutboxRecord, OutboxTransport } from "./storage.js";

/**
 * Runs one ordered lane per transport. A slow or unavailable Kafka connection
 * therefore cannot delay RabbitMQ reservations (and vice versa), while records
 * for the same transport keep their database claim order.
 */
export async function runTransportLanes(
  records: OutboxRecord[],
  processRecord: (record: OutboxRecord) => Promise<void>,
): Promise<void> {
  const lanes = new Map<OutboxTransport, OutboxRecord[]>([
    ["kafka", []],
    ["rabbitmq", []],
  ]);
  for (const record of records) lanes.get(record.transport)?.push(record);

  await Promise.all(
    [...lanes.values()].map(async (lane) => {
      for (const record of lane) await processRecord(record);
    }),
  );
}
