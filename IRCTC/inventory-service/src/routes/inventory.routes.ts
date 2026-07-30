import { Router, Request, Response, NextFunction } from "express";
import { inventoryController } from "../controllers/inventory.controller";
import { getUserContext } from "../middlewares/user-context.middleware";
import { internalAuth } from "../middlewares/internal-auth.middleware";
import { config } from "../config";

const router = Router();

/**
 * Allows either an end-user request that's already passed through the
 * gateway (x-user-id header) or a direct internal-service call (shared
 * secret header) — booking-service needs to read seat status without a
 * logged-in user's JWT in hand.
 */
function userOrInternal(req: Request, res: Response, next: NextFunction) {
  const serviceKey = req.headers["x-internal-service-key"];
  if (serviceKey && serviceKey === config.INTERNAL_SERVICE_KEY) {
    req.user = { id: "internal-service" };
    return next();
  }
  return getUserContext(req, res, next);
}

// Public: aggregate availability (used by search results)
router.get(
  "/schedules/:scheduleId/availability",
  inventoryController.getScheduleAvailability,
);

// Authenticated OR internal: individual seat statuses
router.get(
  "/schedules/:scheduleId/seats",
  userOrInternal,
  inventoryController.getScheduleSeats,
);

// Internal only: called by booking-service during the create/cancel-booking saga
router.post("/seats/lock", internalAuth, inventoryController.lockSeats);
router.post("/seats/unlock", internalAuth, inventoryController.unlockSeats);
router.post("/seats/confirm", internalAuth, inventoryController.confirmSeats);
router.post(
  "/seats/cancel-booking",
  internalAuth,
  inventoryController.cancelBooking,
);

export default router;
