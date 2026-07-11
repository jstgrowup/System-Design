import { Kafka, logLevel, Consumer, Producer } from "kafkajs";
import logger from "./logger";
import { config } from "./config";

/**
 * Kafka client instance for the notification service.
 * Retry config uses exponential backoff (multiplier: 2):
 * 300ms -> 600ms -> 1200ms ... capped at 30s, up to 10 attempts.
 */
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 10,
    maxRetryTime: 30000,
    multiplier: 2,
  },
});

/**
 * Consumer instance for this service's notification group.
 * - sessionTimeout: how long the broker waits before considering this consumer dead
 * - heartbeatInterval: how often the consumer pings the broker to stay alive
 *   (should be well below sessionTimeout, roughly 1/3 as a rule of thumb)
 */
const consumer: Consumer = kafka.consumer({
  groupId: "notification-service-group",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

/**
 * Producer instance used exclusively for publishing to a Dead Letter Queue (DLQ)
 * when message processing fails. Kept separate from the consumer's own lifecycle
 * since it's only needed on the failure path, not for normal consumption.
 */
const producer: Producer = kafka.producer({
  allowAutoTopicCreation: true,
  retry: { retries: 3 },
});

let isProducerConnected = false;

/**
 * Connects the DLQ producer. Safe to call multiple times — only connects once.
 * Called lazily rather than at startup since it's only needed when a message fails.
 */
const connectProducer = async (): Promise<void> => {
  if (!isProducerConnected) {
    await producer.connect();
    isProducerConnected = true;
    logger.info("Kafka producer connected (DLQ)");
  }
};

/**
 * Gracefully disconnects both the consumer and (if connected) the DLQ producer,
 * then exits the process. Wired up to SIGTERM/SIGINT so container orchestrators
 * (e.g. Kubernetes, Docker) can shut this down cleanly without losing in-flight work.
 */
const shutdown = async (): Promise<void> => {
  logger.info("Shutting down Kafka connections...");
  await consumer.disconnect();
  if (isProducerConnected) {
    await producer.disconnect();
    isProducerConnected = false;
  }
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

export { kafka, consumer, producer, connectProducer };
