import { Router } from "express";
import { bookingController } from "../controllers/booking.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// All booking routes require authentication (user context from the gateway).
// Mounted at root in server.ts, so these resolve to /bookings, /bookings/:id, etc.
router.post("/bookings", getUserContext, bookingController.createBooking);
router.get("/bookings", getUserContext, bookingController.getUserBookings);
router.get("/bookings/:bookingId", getUserContext, bookingController.getBooking);
router.post(
  "/bookings/:bookingId/verify-payment",
  getUserContext,
  bookingController.verifyPayment,
);
router.post(
  "/bookings/:bookingId/cancel",
  getUserContext,
  bookingController.cancelBooking,
);

export default router;
