import { Router } from "express";
import { internalAuth } from "../middlewares/internal-auth.middleware";
import { paymentController } from "../controllers/payment.controller";

const router = Router();

// Internal routes — called by booking-service
router.post("/orders", internalAuth, paymentController.createPaymentOrder);
router.get("/orders/:paymentOrderId", internalAuth, paymentController.getPaymentOrder);
router.post(
  "/orders/:paymentOrderId/verify",
  internalAuth,
  paymentController.verifyAndCapturePayment,
);
router.post("/refunds", internalAuth, paymentController.initiateRefund);

export default router;
