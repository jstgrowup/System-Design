import { consumer, producer, connectProducer } from "../config/kafka";
import emailService from "../services/email-service";
import {
  KAFKA_TOPICS,
  KafkaTopic,
} from "../../../shared/constants/kafka-topics";
import { withDLQ } from "../../../shared/utils/dlqHanlder";
import logger from "../config/logger";
import {
  BookingCancelledData,
  BookingConfirmedData,
  BookingFailedData,
} from "../templates";

interface OtpEmailData {
  email: string;
  otp: string;
  ttlMinutes?: number;
}

interface WelcomeEmailData {
  email: string;
  firstName: string;
}

/**
 * Consumes notification-related Kafka events and dispatches them
 * to the appropriate email-sending logic. Wrapped with a DLQ handler
 * so poison messages (repeated failures) get routed to a dead-letter
 * topic instead of blocking the consumer indefinitely.
 */
class EmailConsumer {
  /**
   * Connects the consumer + DLQ producer, subscribes to every known
   * notification topic, and starts processing messages.
   */
  async start(): Promise<void> {
    try {
      await consumer.connect();
      await connectProducer(); // needed for DLQ publishing

      logger.info("Email consumer connected to Kafka");

      await consumer.subscribe({
        topics: Object.values(KAFKA_TOPICS),
        fromBeginning: false,
      });

      await consumer.run({
        eachMessage: withDLQ(
          producer,
          KAFKA_TOPICS.DLQ_NOTIFICATION,
          logger,
          async ({
            topic,
            parsedValue,
          }: {
            topic: KafkaTopic;
            parsedValue: unknown;
          }) => {
            logger.info(`Processing message from topic: ${topic}`);
            await this.handleMessage(topic, parsedValue);
          },
        ),
      });

      logger.info("Email consumer is running and listening for messages...");
    } catch (error) {
      const err = error as Error;
      logger.error("Failed to start email consumer", { error: err.message });
      throw error;
    }
  }

  /**
   * Routes an incoming message to the correct handler based on its topic.
   * Unknown topics are logged and silently skipped rather than throwing,
   * since a new topic being added elsewhere shouldn't crash this consumer.
   */
  private async handleMessage(topic: KafkaTopic, data: unknown): Promise<void> {
    switch (topic) {
      case KAFKA_TOPICS.OTP_EMAIL:
        await this.handleOtpEmail(data as OtpEmailData);
        break;

      case KAFKA_TOPICS.WELCOME_EMAIL:
        await this.handleWelcomeEmail(data as WelcomeEmailData);
        break;

      case KAFKA_TOPICS.BOOKING_CONFIRMED:
        await this.handleBookingConfirmed(data as BookingConfirmedData);
        break;

      case KAFKA_TOPICS.BOOKING_FAILED:
        await this.handleBookingFailed(data as BookingFailedData);
        break;

      case KAFKA_TOPICS.BOOKING_CANCELLED:
        await this.handleBookingCancelled(data as BookingCancelledData);
        break;

      default:
        logger.warn(`Unknown topic: ${topic}`);
    }
  }

  /** Sends an OTP verification email. Defaults TTL to 5 minutes if not provided. */
  private async handleOtpEmail(data: OtpEmailData): Promise<void> {
    const { email, otp, ttlMinutes } = data;

    if (!email || !otp) {
      throw new Error("Missing required fields: email or otp");
    }

    await emailService.sendOtpEmail(email, otp, ttlMinutes || 5);
    logger.info(`OTP email sent to ${email}`);
  }

  /** Sends a welcome email after successful registration. */
  private async handleWelcomeEmail(data: WelcomeEmailData): Promise<void> {
    const { email, firstName } = data;

    if (!email || !firstName) {
      throw new Error("Missing required fields: email or firstName");
    }

    await emailService.sendWelcomeEmail(email, firstName);
    logger.info(`Welcome email sent to ${email}`);
  }

  /** Sends a booking-confirmed email. Skips silently if no email is present on the event. */
  private async handleBookingConfirmed(
    data: BookingConfirmedData,
  ): Promise<void> {
    // BookingConfirmedData has no `email` field — it comes from a separate source
    // on the event; adjust this once you confirm where email actually lives on
    // the real Kafka payload (see note below)
    const email = (data as unknown as { email?: string }).email;
    const { bookingId } = data;

    if (!email) {
      logger.warn(`Skipping booking-confirmed email — no email on event`, {
        bookingId,
      });
      return;
    }

    await emailService.sendBookingConfirmedEmail(email, data);
    logger.info(`Booking confirmed email sent to ${email}`, { bookingId });
  }

  /** Sends a booking-failed email. Skips silently if no email is present on the event. */
  private async handleBookingFailed(data: BookingFailedData): Promise<void> {
    const email = (data as unknown as { email?: string }).email;
    const { bookingId } = data;

    if (!email) {
      logger.warn(`Skipping booking-failed email — no email on event`, {
        bookingId,
      });
      return;
    }

    await emailService.sendBookingFailedEmail(email, data);
    logger.info(`Booking failed email sent to ${email}`, { bookingId });
  }

  /** Sends a booking-cancelled email. Skips silently if no email is present on the event. */
  private async handleBookingCancelled(
    data: BookingCancelledData,
  ): Promise<void> {
    const email = (data as unknown as { email?: string }).email;
    const { bookingId } = data;

    if (!email) {
      logger.warn(`Skipping booking-cancelled email — no email on event`, {
        bookingId,
      });
      return;
    }

    await emailService.sendBookingCancelledEmail(email, data);
    logger.info(`Booking cancelled email sent to ${email}`, { bookingId });
  }

  /** Gracefully disconnects the consumer, e.g. during shutdown. */
  async stop(): Promise<void> {
    await consumer.disconnect();
    logger.info("Email consumer disconnected");
  }
}

export default new EmailConsumer();
