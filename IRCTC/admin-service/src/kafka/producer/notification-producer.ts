import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";

interface OtpEmailPayload {
  email: string;
  otp: string;
  ttlMinutes: number;
}

interface WelcomeEmailPayload {
  email: string;
  firstName: string;
}

/**
 * Wraps the shared Kafka producer with domain-specific helpers for
 * notification-related events (OTP emails, welcome emails, etc).
 * Lazily connects the producer on first use rather than at import time.
 */
class NotificationProducer {
  private isInitialized: boolean;

  constructor() {
    this.isInitialized = false;
  }

  /**
   * Ensures the shared Kafka producer is connected before sending.
   * Safe to call multiple times — only connects once per process lifetime.
   */
  private async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  /**
   * Generic send helper — publishes a single message to the given topic.
   * Falls back to a timestamp-based key if none is provided, to avoid
   * all messages landing on the same partition when no natural key exists.
   */
  private async sendMessage<T>(
    topic: string,
    key: string | undefined,
    value: T,
  ) {
    try {
      await this.initialize();

      const message = {
        topic,
        messages: [
          {
            key: key || `${topic}-${Date.now()}`,
            value: JSON.stringify(value),
            timestamp: Date.now().toString(),
          },
        ],
      };

      const result = await producer.send(message);

      logger.info(`Message sent to kafka topic: ${topic}`, {
        key,
        partition: result[0].partition,
        offset: result[0].offset,
      });

      return result;
    } catch (error) {
      const err = error as Error;
      logger.error(`Failed to send message to kafka topic: ${topic}`, {
        error: err.message,
        stack: err.stack,
        key,
      });
      throw error;
    }
  }

  /**
   * Publishes an OTP email event. Keyed by email so all OTP messages
   * for the same user land on the same partition, preserving order.
   */
  async sendOtpEmail({
    email,
    otp,
    ttlMinutes = 5,
  }: {
    email: string;
    otp: string;
    ttlMinutes: number;
  }) {
    return this.sendMessage<OtpEmailPayload>(
      KAFKA_TOPICS.OTP_EMAIL,
      `otp-${email}`,
      { email, otp, ttlMinutes },
    );
  }

  /**
   * Publishes a welcome email event, sent after successful account verification.
   */
  async sendWelcomeEmail(email: string, firstName: string) {
    return this.sendMessage<WelcomeEmailPayload>(
      KAFKA_TOPICS.WELCOME_EMAIL,
      `welcome-${email}`,
      { email, firstName },
    );
  }
}

export default new NotificationProducer();
