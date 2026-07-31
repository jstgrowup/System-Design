import Razorpay from "razorpay";
import crypto from "crypto";
import { BaseGateway } from "./base.gateway";
import logger from "../../config/logger";
import { BadRequestError } from "../../utils/error";
import type {
  GatewayOrderResult,
  GatewayPaymentResult,
  GatewayRefundResult,
  GatewayRefundFetchResult,
} from "../../types";

// The `razorpay` package ships no bundled type declarations, so every SDK
// call below returns an effectively untyped value — each `as unknown as X`
// asserts it into the one shape this adapter actually reads, right at the
// call site, rather than letting an untyped value leak any further.

/** The subset of the Razorpay SDK's response shapes this adapter reads. */
interface RazorpayOrder {
  id: string;
  amount: number | string;
  currency: string;
  receipt?: string | null;
}
interface RazorpayPayment {
  status: string;
  amount: number | string;
  method: string;
}
interface RazorpayRefund {
  id: string;
  status: string;
  amount: number | string;
}

export class RazorpayGateway extends BaseGateway {
  private readonly keySecret: string;
  private readonly webhookSecret: string;
  private readonly client: Razorpay;

  constructor(keyId: string, keySecret: string, webhookSecret: string) {
    super("razorpay");
    this.keySecret = keySecret;
    this.webhookSecret = webhookSecret;
    this.client = new Razorpay({ key_id: keyId, key_secret: keySecret });
  }

  async createOrder(
    amount: number,
    currency: string,
    receipt: string,
    notes: Record<string, string> = {},
  ): Promise<GatewayOrderResult> {
    // Razorpay's API is paise-denominated (1 INR = 100 paise), but every
    // other amount in this system (PaymentOrder.amount, booking totals,
    // seat prices) is rupee-denominated — this conversion is scoped to this
    // one adapter so nothing outside it ever has to think in paise.
    const amountInPaise = Math.round(amount * 100);

    let order: RazorpayOrder;
    try {
      order = (await this.client.orders.create({
        amount: amountInPaise,
        currency,
        receipt,
        notes,
      })) as unknown as RazorpayOrder;
    } catch (err) {
      // Razorpay SDK throws plain objects, not Error instances
      const razorpayError = err as { error?: { description?: string }; message?: string };
      const description =
        razorpayError?.error?.description || razorpayError?.message || JSON.stringify(err);
      logger.error(`Razorpay createOrder failed: ${description}`);
      throw new BadRequestError(
        `Payment gateway error: ${description}`,
        "PAYMENT_GATEWAY_ERROR",
      );
    }

    logger.info(`Razorpay order created: ${order.id}`, { receipt, amount });

    return {
      gatewayOrderId: order.id,
      amount: Number(order.amount) / 100,
      currency: order.currency,
      receipt: order.receipt ?? receipt,
      rawResponse: order,
    };
  }

  verifyPaymentSignature(orderId: string, paymentId: string, signature: string): boolean {
    const body = `${orderId}|${paymentId}`;
    const expectedSignature = crypto
      .createHmac("sha256", this.keySecret)
      .update(body)
      .digest("hex");

    try {
      return crypto.timingSafeEqual(
        Buffer.from(expectedSignature, "hex"),
        Buffer.from(signature, "hex"),
      );
    } catch {
      return false;
    }
  }

  verifyWebhookSignature(rawBody: string | Buffer, signature: string): boolean {
    const body = typeof rawBody === "string" ? rawBody : rawBody.toString("utf8");
    const expectedSignature = crypto
      .createHmac("sha256", this.webhookSecret)
      .update(body)
      .digest("hex");

    try {
      return crypto.timingSafeEqual(
        Buffer.from(expectedSignature, "hex"),
        Buffer.from(signature, "hex"),
      );
    } catch {
      return false;
    }
  }

  async fetchPayment(paymentId: string): Promise<GatewayPaymentResult> {
    const payment = (await this.client.payments.fetch(
      paymentId,
    )) as unknown as RazorpayPayment;

    return {
      status: payment.status,
      amount: Number(payment.amount) / 100,
      method: payment.method,
      rawResponse: payment,
    };
  }

  async initiateRefund(
    paymentId: string,
    amount: number,
    notes: Record<string, string> = {},
  ): Promise<GatewayRefundResult> {
    // Same paise conversion as createOrder — see the comment there.
    const amountInPaise = Math.round(amount * 100);

    const refund = (await this.client.payments.refund(paymentId, {
      amount: amountInPaise,
      notes,
    })) as unknown as RazorpayRefund;

    logger.info(`Razorpay refund initiated: ${refund.id}`, { paymentId, amount });

    return {
      gatewayRefundId: refund.id,
      status: refund.status,
      amount: Number(refund.amount) / 100,
      rawResponse: refund,
    };
  }

  async fetchRefund(paymentId: string, refundId: string): Promise<GatewayRefundFetchResult> {
    const refund = (await this.client.payments.fetchRefund(
      paymentId,
      refundId,
    )) as unknown as RazorpayRefund;

    return {
      status: refund.status,
      amount: Number(refund.amount) / 100,
      rawResponse: refund,
    };
  }
}
