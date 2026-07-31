import { Request, Response } from "express";
import asyncHandler from "../utils/asyncHandler";
import { paymentService } from "../services/payment.service";
import logger from "../config/logger";

/**
 * Razorpay webhook handler.
 * IMPORTANT: This endpoint receives the raw request body (not JSON-parsed)
 * for signature verification — the route mounts `express.raw()` ahead of
 * this handler, so `req.body` is a `Buffer` here, unlike every other route
 * in this service.
 */
const razorpayWebhook = asyncHandler(async (req: Request, res: Response) => {
  const signature = req.headers["x-razorpay-signature"];

  if (!signature || Array.isArray(signature)) {
    logger.warn("Webhook received without a valid signature header");
    return res.status(400).json({ status: "error", message: "Missing signature" });
  }

  const rawBody = req.body as Buffer;

  const result = await paymentService.handleWebhook(rawBody, signature);

  logger.info("Webhook processed", { result });

  // Always return 200 to the gateway to prevent retries for processed events.
  // `result.status` is always present (every branch of handleWebhook sets
  // one), so there's no separate "ok" fallback needed here — this is just
  // `result` with the HTTP status forced to 200 regardless of its content.
  res.status(200).json(result);
});

export const webhookController = { razorpayWebhook };
