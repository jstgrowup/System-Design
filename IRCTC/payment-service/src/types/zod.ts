import { z } from "zod";

export const zCreatePaymentOrder = z.object({
  bookingId: z.string({ error: "bookingId is required" }).min(1, "bookingId is required"),
  amount: z
    .number({ error: "amount is required" })
    .positive("Amount must be greater than 0"),
  userId: z.string({ error: "userId is required" }).min(1, "userId is required"),
  idempotencyKey: z
    .string({ error: "idempotencyKey is required" })
    .min(1, "idempotencyKey is required"),
});
export type CreatePaymentOrderBodyType = z.infer<typeof zCreatePaymentOrder>;

export const zVerifyAndCapture = z.object({
  gatewayPaymentId: z
    .string({ error: "gatewayPaymentId is required" })
    .min(1, "gatewayPaymentId is required"),
  gatewaySignature: z
    .string({ error: "gatewaySignature is required" })
    .min(1, "gatewaySignature is required"),
});
export type VerifyAndCaptureBodyType = z.infer<typeof zVerifyAndCapture>;

export const zInitiateRefund = z.object({
  paymentOrderId: z
    .string({ error: "paymentOrderId is required" })
    .min(1, "paymentOrderId is required"),
  amount: z
    .number({ error: "amount is required" })
    .positive("Amount must be greater than 0"),
  reason: z.string().optional(),
  idempotencyKey: z
    .string({ error: "idempotencyKey is required" })
    .min(1, "idempotencyKey is required"),
});
export type InitiateRefundBodyType = z.infer<typeof zInitiateRefund>;
