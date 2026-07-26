import { Router } from "express";
import { scheduleController } from "../controllers/schedule.controller";

const router = Router();

// Not mounted anywhere: server.ts only app.use()s station.route.ts and
// train.routes.ts, never this file. So while this defines POST /schedule,
// there is currently no HTTP path that reaches scheduleController
// .createSchedule — the whole schedule-creation feature is dead code from
// the outside, reachable only by importing this router directly (e.g. in
// a test) rather than through the running server.
router.post("/schedule", scheduleController.createSchedule);

export default router;
