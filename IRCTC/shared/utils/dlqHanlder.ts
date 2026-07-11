/**
 * Dead-Letter Queue (DLQ) handler for Kafka consumers.
 *
 * Wraps eachMessage processing with retry tracking. After DLQ_MAX_RETRIES
 * consecutive failures the message is forwarded to a per-service DLQ topic
 * and the consumer moves on instead of blocking forever.
 *
 * Usage (in any consumer):
 *   import { withDLQ } from "../../../../shared/utils/dlqHandler";
 *   await consumer.run({ eachMessage: withDLQ(producer, dlqTopic, logger, handler) });
 */

import { EachMessagePayload, Producer } from "kafkajs";
import { DLQ_MAX_RETRIES } from "../constants/kafka-topics";

interface Logger {
  info: (message: string, meta?: Record<string, unknown>) => void;
  error: (message: string, meta?: Record<string, unknown>) => void;
}

interface HandlerPayload<T = unknown, TTopic extends string = string> {
  topic: TTopic;
  partition: number;
  message: EachMessagePayload["message"];
  parsedValue: T;
}

type MessageHandler<T = unknown, TTopic extends string = string> = (
  payload: HandlerPayload<T, TTopic>,
) => Promise<void>;

/**
 * Wraps a message handler with retry tracking + DLQ forwarding.
 *
 * @param producer  Kafka producer (for sending to DLQ)
 * @param dlqTopic  DLQ topic name (e.g. KAFKA_TOPICS.DLQ_BOOKING)
 * @param logger    Winston logger (or compatible)
 * @param handler   async ({ topic, partition, message, parsedValue }) => void
 * @returns an eachMessage-compatible handler
 */
export function withDLQ<T = unknown, TTopic extends string = string>(
  producer: Producer,
  dlqTopic: string,
  logger: Logger,
  handler: MessageHandler<T, TTopic>,
) {
  const retryMap = new Map<string, number>();

  return async ({
    topic,
    partition,
    message,
  }: EachMessagePayload): Promise<void> => {
    const msgKey = `${topic}:${partition}:${message.offset}`;
    const attempt = (retryMap.get(msgKey) || 0) + 1;
    retryMap.set(msgKey, attempt);

    let parsedValue: T;
    try {
      parsedValue = JSON.parse(message.value?.toString() ?? "");
    } catch (parseErr) {
      const err = parseErr as Error;
      logger.error(`Unparseable message on ${topic}, sending to DLQ`, {
        partition,
        offset: message.offset,
        error: err.message,
      });
      await sendToDLQ(
        producer,
        dlqTopic,
        topic,
        partition,
        message,
        err,
        logger,
      );
      retryMap.delete(msgKey);
      return;
    }

    try {
      // topic comes from kafkajs as a plain string; cast to TTopic here since
      // you control which specific topics this consumer subscribes to
      await handler({
        topic: topic as TTopic,
        partition,
        message,
        parsedValue,
      });
      retryMap.delete(msgKey);
    } catch (error) {
      const err = error as Error;
      logger.error(
        `Error processing ${topic} (attempt ${attempt}/${DLQ_MAX_RETRIES})`,
        {
          error: err.message,
          partition,
          offset: message.offset,
        },
      );

      if (attempt >= DLQ_MAX_RETRIES) {
        logger.error(`Max retries exceeded for ${topic}, sending to DLQ`, {
          partition,
          offset: message.offset,
        });
        await sendToDLQ(
          producer,
          dlqTopic,
          topic,
          partition,
          message,
          err,
          logger,
        );
        retryMap.delete(msgKey);
      } else {
        throw error;
      }
    }
  };
}

/**
 * Publishes a failed message to the DLQ topic, tagging it with headers
 * describing where it originally came from and why it failed.
 */
async function sendToDLQ(
  producer: Producer,
  dlqTopic: string,
  originalTopic: string,
  partition: number,
  message: EachMessagePayload["message"],
  error: Error,
  logger: Logger,
): Promise<void> {
  try {
    await producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            "dlq-original-topic": originalTopic,
            "dlq-original-partition": String(partition),
            "dlq-original-offset": message.offset,
            "dlq-error": error.message,
            "dlq-timestamp": new Date().toISOString(),
          },
        },
      ],
    });
    logger.info(`Message sent to DLQ: ${dlqTopic}`, {
      originalTopic,
      partition,
      offset: message.offset,
    });
  } catch (dlqError) {
    const err = dlqError as Error;
    logger.error(`Failed to send message to DLQ ${dlqTopic}`, {
      error: err.message,
      originalTopic,
      partition,
      offset: message.offset,
    });
  }
}
