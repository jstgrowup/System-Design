import { Kafka, logLevel, Producer } from "kafkajs";
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
 * Kafka producer instance.
 * - idempotent: true ensures messages aren't duplicated on retry (exactly-once semantics per partition)
 * - maxInFlightRequests must stay <= 5 for idempotency guarantees to hold
 * - allowAutoTopicCreation creates topics on the fly if they don't exist yet
 */
const producer: Producer = kafka.producer({
  allowAutoTopicCreation: true,
  transactionTimeout: 30000,
  idempotent: true,
  maxInFlightRequests: 5,
  retry: {
    retries: 5,
  },
});

// Tracks connection state so connect/disconnect calls are idempotent themselves
// (calling connect() twice or disconnect() when already disconnected is a no-op)
let isConnected = false;

/**
 * Connects the producer to the Kafka cluster.
 * Safe to call multiple times — only connects once.
 */
const connectProducer = async (): Promise<void> => {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    logger.info("Kafka producer connected");
  }
};

/**
 * Disconnects the producer from the Kafka cluster.
 * Safe to call multiple times — only disconnects if currently connected.
 * Should be called during graceful shutdown to flush pending messages.
 */
const disconnectProducer = async (): Promise<void> => {
  if (isConnected) {
    await producer.disconnect();
    isConnected = false;
    logger.info("Kafka producer disconnected");
  }
};

/**
 * Kafka consumer instance for SCHEDULE_CREATED / SCHEDULE_CANCELLED.
 * A dedicated group ID isolates this service's partition assignment/offsets
 * from every other consumer group in the cluster.
 */
const consumer = kafka.consumer({
  groupId: "inventory-service-group",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const disconnectConsumer = async (): Promise<void> => {
  await consumer.disconnect();
  logger.info("Kafka consumer disconnected");
};

const disconnectAll = async (): Promise<void> => {
  await disconnectProducer();
  await disconnectConsumer();
};

export {
  kafka,
  producer,
  consumer,
  connectProducer,
  disconnectProducer,
  disconnectConsumer,
  disconnectAll,
};
