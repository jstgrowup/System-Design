// ─── Gateway adapter contract (implemented by each concrete gateway) ────────

export interface GatewayOrderResult {
  gatewayOrderId: string;
  amount: number;
  currency: string;
  receipt: string;
  rawResponse: unknown;
}

export interface GatewayPaymentResult {
  status: string;
  amount: number;
  method: string;
  rawResponse: unknown;
}

export interface GatewayRefundResult {
  gatewayRefundId: string;
  status: string;
  amount: number;
  rawResponse: unknown;
}

export interface GatewayRefundFetchResult {
  status: string;
  amount: number;
  rawResponse: unknown;
}

// ─── Razorpay webhook payload shapes (only the fields this service reads) ───

export interface RazorpayPaymentEntity {
  id: string;
  order_id: string;
  error_description?: string;
  error_reason?: string;
  acquirer_data?: { auth_code?: string };
}

export interface RazorpayRefundEntity {
  id: string;
}

export interface RazorpayWebhookPayload {
  event: string;
  payload: {
    payment?: { entity: RazorpayPaymentEntity };
    refund?: { entity: RazorpayRefundEntity };
  };
}

// ─── Service-layer DTOs (what the HTTP surface returns) ─────────────────────

export interface CreatePaymentOrderResult {
  paymentOrderId: string;
  gatewayOrderId: string | null;
  amount: number;
  currency: string;
  status: string;
  gatewayProvider: string;
  keyId: string | undefined;
}

export interface VerifyAndCaptureResult {
  paymentOrderId: string;
  status: string;
  gatewayPaymentId: string | null;
  message?: string;
}

export interface RefundResult {
  refundId: string;
  paymentOrderId: string;
  status: string;
  amount: number;
  gatewayRefundId: string | null;
}

/**
 * The result of processing one webhook event — deliberately loose (a grab-bag
 * of optional fields) because Razorpay sends many event types and this
 * service only reacts meaningfully to three of them; everything else is
 * acknowledged with `{ status: "ignored", event }` so Razorpay stops retrying.
 */
export interface WebhookHandlingResult {
  status: string;
  event?: string;
  reason?: string;
  paymentOrderId?: string;
  currentStatus?: string;
  gatewayRefundId?: string;
}
