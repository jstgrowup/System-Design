import { Router } from "express";
import { trainController } from "../controllers/train.controller";

const router = Router();

// Mounted at /trains in server.ts. No auth/user-context middleware is
// applied here — anything that can reach this service can create a train
// or a route.
router.post("/train", trainController.createTrain); // POST /trains/train — create train + seats
router.post("/route", trainController.createRoute); // POST /trains/route — define a train's route
// POST /trains/route/:id — intended as "get train by id", but broken: this
// should be a GET, and the param is named :id while the controller reads
// req.params.trainId, so it's always undefined. See trainController
// .getTrainById's doc comment for the full picture.
router.post("/route/:id", trainController.getTrainById);

export default router;
