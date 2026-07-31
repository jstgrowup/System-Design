import axios, { AxiosError, AxiosInstance } from "axios";
import { config } from "../config";
import logger from "../config/logger";
import type {
  PaymentOrder,
  PaymentVerifyResult,
  PaymentRefundResult,
} from "../types";

const client: AxiosInstance = axios.create({
  baseURL: config.PAYMENT_SERVICE_URL,
  timeout: 10000,
  headers: {
    "Content-Type": "application/json",
    "x-internal-service-key": config.INTERNAL_SERVICE_KEY,
  },
});

interface ClientErrorShape {
  status: number;
  message: string;
  code?: string;
}

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = (error as AxiosError).response?.status;
      if (status && status >= 400 && status < 500) throw error;

      if (attempt < maxRetries) {
        const delay = 200 * Math.pow(2, attempt - 1);
        logger.warn(`Payment client retry ${attempt}/${maxRetries} after ${delay}ms`, {
          error: (error as Error).message,
        });
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

export function extractError(error: unknown): ClientErrorShape {
  const axiosError = error as AxiosError<{ message?: string; error?: string }>;
  if (axiosError.response?.data) {
    return {
      status: axiosError.response.status,
      message: axiosError.response.data.message || axiosError.message,
      code: axiosError.response.data.error,
    };
  }
  return {
    status: 500,
    message: (error as Error).message,
    code: "PAYMENT_SERVICE_ERROR",
  };
}

export const paymentClient = {
  async createPaymentOrder(
    bookingId: string,
    amount: number,
    userId: string,
    idempotencyKey: string,
  ): Promise<PaymentOrder> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: PaymentOrder }>("/orders", {
        bookingId,
        amount,
        userId,
        idempotencyKey,
      });
      return data.data;
    });
  },

  async getPaymentStatus(paymentOrderId: string): Promise<PaymentVerifyResult> {
    return withRetry(async () => {
      const { data } = await client.get<{ data: PaymentVerifyResult }>(
        `/orders/${paymentOrderId}`,
      );
      return data.data;
    });
  },

  async verifyPayment(
    paymentOrderId: string,
    gatewayPaymentId: string,
    gatewaySignature: string,
  ): Promise<PaymentVerifyResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: PaymentVerifyResult }>(
        `/orders/${paymentOrderId}/verify`,
        { gatewayPaymentId, gatewaySignature },
      );
      return data.data;
    });
  },

  async initiateRefund(
    paymentOrderId: string,
    amount: number,
    reason: string,
    idempotencyKey: string,
  ): Promise<PaymentRefundResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: PaymentRefundResult }>(
        "/refunds",
        { paymentOrderId, amount, reason, idempotencyKey },
      );
      return data.data;
    });
  },
};
