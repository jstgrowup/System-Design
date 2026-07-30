import { Router } from "express";
import { searchController } from "../controllers/search.controller";

const router = Router();

// Mounted at root in index.ts (the gateway strips the first path segment
// before forwarding, so no "/search" prefix is needed here).
// GET /search/trains?from=Delhi&to=Mumbai&date=2025-07-15
router.get("/trains", searchController.searchTrains);

// GET /search/autocomplete?q=del
router.get("/autocomplete", searchController.autoComplete);

// Debug endpoints
router.get("/debug/stations", searchController.debugStations);
router.get("/debug/trains", searchController.debugTrains);

export default router;
