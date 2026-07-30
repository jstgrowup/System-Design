import { Router } from "express";
import { stationController } from "../controllers/station.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// Mounted at /stations in server.ts, so this resolves to POST /stations/station.
router.post("/station", getUserContext, stationController.createStation);

export default router;
