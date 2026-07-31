import type {
  GatewayOrderResult,
  GatewayPaymentResult,
  GatewayRefundResult,
  GatewayRefundFetchResult,
} from "../../types";

/**
 * Abstract base class defining the payment gateway interface.
 * All gateway implementations must extend this and implement every method.
 * This enables the adapter pattern — swap gateways without touching business logic.
 */
export abstract class BaseGateway {
  public readonly providerName: string;

  protected constructor(providerName: string) {
    this.providerName = providerName;
  }

  /**
   * Create a payment order with the gateway.
   * @param amount Amount in base currency (e.g., INR, not paise)
   * @param currency Currency code (e.g., "INR")
   * @param receipt Unique receipt/reference id (typically bookingId)
   * @param notes Additional metadata
   */
  abstract createOrder(
    amount: number,
    currency: string,
    receipt: string,
    notes?: Record<string, string>,
  ): Promise<GatewayOrderResult>;

  /** Verify a payment signature (client-side verification after checkout). */
  abstract verifyPaymentSignature(
    orderId: string,
    paymentId: string,
    signature: string,
  ): boolean;

  /** Verify a webhook signature from the gateway. */
  abstract verifyWebhookSignature(
    rawBody: string | Buffer,
    signature: string,
  ): boolean;

  /** Fetch payment details from the gateway. */
  abstract fetchPayment(paymentId: string): Promise<GatewayPaymentResult>;

  /** Initiate a refund. */
  abstract initiateRefund(
    paymentId: string,
    amount: number,
    notes?: Record<string, string>,
  ): Promise<GatewayRefundResult>;

  /** Fetch refund details from the gateway. */
  abstract fetchRefund(
    paymentId: string,
    refundId: string,
  ): Promise<GatewayRefundFetchResult>;
}
