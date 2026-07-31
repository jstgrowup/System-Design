import { Router, raw } from "express";
import { webhookController } from "../controllers/webhook.controller";

const router = Router();

// Public: Razorpay calls this endpoint with payment events.
// IMPORTANT: uses express.raw() so we get the raw body for signature verification.
router.post(
  "/webhooks/razorpay",
  raw({ type: "application/json" }),
  webhookController.razorpayWebhook,
);

export default router;
