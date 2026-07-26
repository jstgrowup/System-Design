import { Kafka, logLevel, Consumer, Producer } from "kafkajs";
import logger from "./logger";
import { config } from ".";

/**
 * Kafka client instance.
 * Configured with retry backoff for broker connection issues:
 * starts at 300ms, doubles up to 8 retries, capped at 30s between attempts.
 */
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR, // suppress kafkajs's default verbose logging
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 30000,
  },
});

/**
 * Kafka consumer instance — subscribed to whatever topics
 * kafka/consumer/search.consumer.ts wires it up to.
 * - groupId: every process running this service shares the same group id,
 *   so the partitions get load-balanced across instances instead of each
 *   instance receiving a duplicate copy of every message
 * - sessionTimeout: how long the broker waits without a heartbeat before
 *   considering this consumer dead and triggering a rebalance
 * - heartbeatInterval: how often this consumer pings the broker to prove
 *   it's still alive — must stay well below sessionTimeout
 */
const consumer: Consumer = kafka.consumer({
  groupId: "search-service-group-v2",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

/**
 * Kafka producer instance — used only for publishing to dead-letter-queue
 * topics when a consumed message fails processing, not for any regular
 * publish flow (this service only consumes, it doesn't produce domain events).
 * allowAutoTopicCreation creates DLQ topics on the fly if they don't exist yet.
 */
const producer: Producer = kafka.producer({
  allowAutoTopicCreation: true,
  retry: {
    retries: 3,
  },
});

// Tracks connection state so connect/disconnect calls are idempotent themselves
// (calling connect() twice or disconnect() when already disconnected is a no-op)
let isProducerConnected = false;

/**
 * Connects the DLQ producer to the Kafka cluster.
 * Safe to call multiple times — only connects once.
 */
const connectProducer = async (): Promise<void> => {
  if (!isProducerConnected) {
    await producer.connect();
    isProducerConnected = true;
    logger.info("Kafka producer connected (DLQ)");
  }
};

/**
 * Disconnects both the consumer and the DLQ producer.
 * Should be called during graceful shutdown — lets the consumer leave its
 * group cleanly (triggering an immediate rebalance instead of the group
 * waiting out sessionTimeout) and flushes any pending DLQ messages.
 */
const disconnectAll = async (): Promise<void> => {
  await consumer.disconnect();
  if (isProducerConnected) {
    await producer.disconnect();
    isProducerConnected = false;
  }
  logger.info("Kafka consumer disconnected");
};

export { kafka, consumer, producer, connectProducer, disconnectAll };
