import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";

class PaymentProducer {
  private isInitialized = false;

  async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  async sendMessage<T extends object>(topic: string, key: string | undefined, value: T) {
    try {
      await this.initialize();
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
      logger.error(`Failed to send message to topic: ${topic}`, {
        error: (error as Error).message,
        key,
      });
      throw error;
    }
  }

  async publishPaymentSuccess(
    paymentOrderId: string,
    bookingId: string,
    gatewayPaymentId: string,
    amount: number,
  ) {
    return this.sendMessage(KAFKA_TOPICS.PAYMENT_SUCCESS, `payment-${paymentOrderId}`, {
      paymentOrderId,
      bookingId,
      gatewayPaymentId,
      amount,
      capturedAt: new Date().toISOString(),
    });
  }

  async publishPaymentFailed(paymentOrderId: string, bookingId: string, reason: string) {
    return this.sendMessage(KAFKA_TOPICS.PAYMENT_FAILED, `payment-${paymentOrderId}`, {
      paymentOrderId,
      bookingId,
      reason,
      failedAt: new Date().toISOString(),
    });
  }
}

export default new PaymentProducer();
