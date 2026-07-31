import { Request, Response } from "express";
import asyncHandler from "../utils/asyncHandler";
import { paymentService } from "../services/payment.service";
import { zCreatePaymentOrder, zVerifyAndCapture, zInitiateRefund } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

/**
 * POST /orders — internal only, called by booking-service's saga.
 */
const createPaymentOrder = asyncHandler(async (req: Request, res: Response) => {
  const result = zCreatePaymentOrder.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { bookingId, amount, userId, idempotencyKey } = result.data;

  const order = await paymentService.createPaymentOrder(bookingId, amount, userId, idempotencyKey);

  res.status(201).json({ success: true, data: order });
});

/**
 * GET /orders/:paymentOrderId — internal only.
 */
const getPaymentOrder = asyncHandler(
  async (req: Request<{ paymentOrderId: string }>, res: Response) => {
    const { paymentOrderId } = req.params;

    const order = await paymentService.getPaymentOrder(paymentOrderId);

    res.status(200).json({ success: true, data: order });
  },
);

/**
 * POST /orders/:paymentOrderId/verify — internal only, called by
 * booking-service after the client completes checkout.
 */
const verifyAndCapturePayment = asyncHandler(
  async (req: Request<{ paymentOrderId: string }>, res: Response) => {
    const { paymentOrderId } = req.params;

    const result = zVerifyAndCapture.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, { message: formatZodError(result.error) });
    }

    const { gatewayPaymentId, gatewaySignature } = result.data;

    const capture = await paymentService.verifyAndCapturePayment(
      paymentOrderId,
      gatewayPaymentId,
      gatewaySignature,
    );

    res.status(200).json({ success: true, data: capture });
  },
);

/**
 * POST /refunds — internal only.
 */
const initiateRefund = asyncHandler(async (req: Request, res: Response) => {
  const result = zInitiateRefund.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { paymentOrderId, amount, reason, idempotencyKey } = result.data;

  const refund = await paymentService.initiateRefund(paymentOrderId, amount, reason, idempotencyKey);

  res.status(201).json({ success: true, data: refund });
});

export const paymentController = {
  createPaymentOrder,
  getPaymentOrder,
  verifyAndCapturePayment,
  initiateRefund,
};
