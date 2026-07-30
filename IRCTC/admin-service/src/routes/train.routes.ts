import { Router } from "express";
import { trainController } from "../controllers/train.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// Mounted at /trains in server.ts.
router.post("/train", getUserContext, trainController.createTrain); // POST /trains/train — create train + seats
router.post("/route", getUserContext, trainController.createRoute); // POST /trains/route — define a train's route
router.get("/train/:trainId", getUserContext, trainController.getTrainById); // GET /trains/train/:trainId — fetch a train with seats + route

export default router;
