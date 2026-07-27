import { Router } from "express";
import { searchController } from "../controllers/search.controller";

const router = Router();

// Not mounted anywhere: server.ts only app.use()s station.route.ts and
// train.routes.ts, never this file. So while this defines POST /schedule,
// there is currently no HTTP path that reaches scheduleController
// .createSchedule — the whole schedule-creation feature is dead code from
// the outside, reachable only by importing this router directly (e.g. in
// a test) rather than through the running server.
// GET /search/trains?from=Delhi&to=Mumbai&date=2025-07-15
router.get("/trains", searchController.searchTrains);

// GET /search/autocomplete?q=del
router.get("/autocomplete", searchController.autoComplete);

// Debug endpoints
router.get("/debug/stations", searchController.debugStations);
router.get("/debug/trains", searchController.debugTrains);

export default router;
