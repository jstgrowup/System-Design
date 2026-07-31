import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";

const MAX_PUBLISH_RETRIES = 3;
const RETRY_DELAY_MS = 500;

export interface BookingConfirmedEvent {
  bookingId: string;
  userId: string;
  email?: string;
  firstName?: string;
  scheduleId: string;
  trainNumber: string;
  trainName: string;
  fromStationName: string | null;
  toStationName: string | null;
  departureDate: Date;
  seats: { seatNumber: number; seatType: string; price: number }[];
  passengers: { name: string; age: number; gender: string }[];
  totalAmount: number;
}

export interface BookingCancelledEvent {
  bookingId: string;
  userId: string;
  email?: string;
  firstName?: string;
  scheduleId: string;
  reason: string;
  refundAmount: number;
}

export interface BookingFailedEvent {
  bookingId: string;
  userId: string;
  email?: string;
  firstName?: string;
  scheduleId: string;
  reason: string;
}

class BookingProducer {
  private isInitialized = false;

  async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  /**
   * Send a message with retries. Critical events (BOOKING_CONFIRMED, etc.)
   * must not be silently lost — callers should handle the thrown error.
   */
  async sendMessage<T extends object>(
    topic: string,
    key: string | undefined,
    value: T,
  ) {
    await this.initialize();

    let lastError: unknown;
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
        lastError = error;
        logger.error(
          `Failed to send message to ${topic} (attempt ${attempt}/${MAX_PUBLISH_RETRIES})`,
          { error: (error as Error).message, key },
        );
        if (attempt < MAX_PUBLISH_RETRIES) {
          await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * attempt));
        }
      }
    }

    logger.error(`All ${MAX_PUBLISH_RETRIES} publish attempts failed for ${topic}`, {
      key,
    });
    throw lastError;
  }

  async publishBookingConfirmed(data: BookingConfirmedEvent) {
    return this.sendMessage(
      KAFKA_TOPICS.BOOKING_CONFIRMED,
      `booking-${data.bookingId}`,
      { ...data, confirmedAt: new Date().toISOString() },
    );
  }

  async publishBookingCancelled(data: BookingCancelledEvent) {
    return this.sendMessage(
      KAFKA_TOPICS.BOOKING_CANCELLED,
      `booking-${data.bookingId}`,
      { ...data, cancelledAt: new Date().toISOString() },
    );
  }

  async publishBookingFailed(data: BookingFailedEvent) {
    return this.sendMessage(KAFKA_TOPICS.BOOKING_FAILED, `booking-${data.bookingId}`, {
      ...data,
      failedAt: new Date().toISOString(),
    });
  }
}

export default new BookingProducer();
