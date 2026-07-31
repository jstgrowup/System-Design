import { Kafka, logLevel, Producer } from "kafkajs";
import logger from "./logger";
import { config } from "./index";

/**
 * Kafka client instance.
 * Configured with retry backoff for broker connection issues:
 * starts at 300ms, doubles up to 8 retries, capped at 30s between attempts.
 */
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 30000,
  },
});

/**
 * Kafka producer instance — payment-service only ever publishes
 * (payment.success / payment.failed), it never consumes anything.
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

let isConnected = false;

const connectProducer = async (): Promise<void> => {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    logger.info("Kafka producer connected");
  }
};

const disconnectProducer = async (): Promise<void> => {
  if (isConnected) {
    await producer.disconnect();
    isConnected = false;
    logger.info("Kafka producer disconnected");
  }
};

export { kafka, producer, connectProducer, disconnectProducer };
