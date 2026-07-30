import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";

interface SeatAvailabilityUpdatedPayload {
  scheduleId: string;
  trainId: string;
  available: number;
  locked: number;
  booked: number;
}

const MAX_PUBLISH_RETRIES = 3;
const RETRY_DELAY_MS = 500;

/**
 * Wraps the shared Kafka producer with domain-specific helpers for
 * inventory-related events (seat availability changes). Lazily connects the
 * producer on first use rather than at import time.
 */
class InventoryProducer {
  private isInitialized: boolean;

  constructor() {
    this.isInitialized = false;
  }

  private async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  /**
   * Generic send helper with retries — availability updates keep search-service's
   * index accurate, so a transient broker error is worth retrying before giving up.
   */
  private async sendMessage<T>(
    topic: string,
    key: string | undefined,
    value: T,
  ) {
    await this.initialize();

    let lastError: Error | undefined;
    for (let attempt = 1; attempt <= MAX_PUBLISH_RETRIES; attempt++) {
      try {
        const result = await producer.send({
          topic,
          messages: [
            {
              key: key || `${topic}-${Date.now()}`,
              value: JSON.stringify(value),
              timestamp: Date.now().toString(),
            },
          ],
        });
        logger.info(`Message sent to topic: ${topic}`, {
          key,
          partition: result[0]?.partition,
          offset: result[0]?.offset,
        });
        return result;
      } catch (error) {
        lastError = error as Error;
        logger.error(
          `Failed to send message to ${topic} (attempt ${attempt}/${MAX_PUBLISH_RETRIES})`,
          { error: lastError.message, key },
        );
        if (attempt < MAX_PUBLISH_RETRIES) {
          await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * attempt));
        }
      }
    }

    logger.error(
      `All ${MAX_PUBLISH_RETRIES} publish attempts failed for ${topic}`,
      { key },
    );
    throw lastError;
  }

  async publishSeatAvailabilityUpdated(
    scheduleId: string,
    trainId: string,
    available: number,
    locked: number,
    booked: number,
  ) {
    return this.sendMessage<SeatAvailabilityUpdatedPayload>(
      KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
      `schedule-${scheduleId}`,
      { scheduleId, trainId, available, locked, booked },
    );
  }
}

export default new InventoryProducer();
