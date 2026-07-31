import { Router } from "express";
import { stationController } from "../controllers/station.controller";
import { getUserContext } from "../middlewares/user-context.middleware";
import { internalAuth } from "../middlewares/internal-auth.middleware";

const router = Router();

// Mounted at /stations in server.ts, so this resolves to POST /stations/station.
router.post("/station", getUserContext, stationController.createStation);

// Internal-only — behind a shared secret, not getUserContext, since callers
// are other services (booking-service), not requests proxied from the gateway.
router.get(
  "/station/internal/:stationId",
  internalAuth,
  stationController.getStationByIdInternal,
);

export default router;
