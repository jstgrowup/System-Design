import { Router } from "express";
import { scheduleController } from "../controllers/schedule.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// Mounted at /schedules in server.ts, so this resolves to POST /schedules/schedule.
router.post("/schedule", getUserContext, scheduleController.createSchedule);

export default router;
