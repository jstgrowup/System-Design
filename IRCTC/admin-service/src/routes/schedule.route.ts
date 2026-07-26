import { Router } from "express";
import { scheduleController } from "../controllers/schedule.controller";

const router = Router();

// Mounted at /stations in server.ts, so this resolves to POST /stations/station.
// No auth/user-context middleware is applied here — anything that can reach
// this service can create a station.
router.post("/schedule", scheduleController.createSchedule);

export default router;
