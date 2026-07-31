import { consumer, producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";
import { withDLQ } from "../../../../shared/utils/dlqHanlder";
import { bookingService } from "../../services/booking.service";
import type {
  PaymentSuccessEventData,
  PaymentFailedEventData,
  ScheduleCancelledEventData,
} from "../../types";

const start = async (): Promise<void> => {
  await consumer.connect();
  await connectProducer(); // needed for DLQ publishing
  logger.info("Booking consumer connected");

  await consumer.subscribe({
    topics: [
      KAFKA_TOPICS.PAYMENT_SUCCESS,
      KAFKA_TOPICS.PAYMENT_FAILED,
      KAFKA_TOPICS.SCHEDULE_CANCELLED,
    ],
    fromBeginning: false,
  });

  await consumer.run({
    // parsedValue arrives as `unknown` from withDLQ — the `as` cast in each
    // branch below is a deliberate external-boundary assertion into that
    // topic's known event shape, not a blanket `any`.
    eachMessage: withDLQ<unknown>(
      producer,
      KAFKA_TOPICS.DLQ_BOOKING,
      logger,
      async ({ topic, partition, message, parsedValue }) => {
        logger.info(`Received message on topic: ${topic}`, {
          partition,
          offset: message.offset,
          key: message.key?.toString(),
        });

        switch (topic) {
          case KAFKA_TOPICS.PAYMENT_SUCCESS: {
            const data = parsedValue as PaymentSuccessEventData;
            await bookingService.handlePaymentSuccess(
              data.paymentOrderId,
              data.gatewayPaymentId,
              data.amount,
            );
            break;
          }

          case KAFKA_TOPICS.PAYMENT_FAILED: {
            const data = parsedValue as PaymentFailedEventData;
            await bookingService.handlePaymentFailure(data.paymentOrderId, data.reason);
            break;
          }

          case KAFKA_TOPICS.SCHEDULE_CANCELLED: {
            const eventData = parsedValue as ScheduleCancelledEventData;
            const data = "data" in eventData ? eventData.data : eventData;
            const scheduleId = data.scheduleId || data.id;
            await bookingService.handleScheduleCancelled(scheduleId);
            break;
          }

          default:
            logger.warn(`Unknown topic: ${topic}`);
        }
      },
    ),
  });

  logger.info("Booking consumer running");
};

export default { start };
