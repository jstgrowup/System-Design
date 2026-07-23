import { Router } from "express";
import { stationController } from "../controllers/station.controller";

const router = Router();

// Mounted at /stations in server.ts, so this resolves to POST /stations/station.
// No auth/user-context middleware is applied here — anything that can reach
// this service can create a station.
router.post("/station", stationController.createStation);

export default router;
