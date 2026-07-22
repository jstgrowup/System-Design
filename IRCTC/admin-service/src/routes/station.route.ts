import { Router } from "express";
import { stationController } from "../controllers/station.controller";

const router = Router();

router.post("/station", stationController.createStation);

export default router;
