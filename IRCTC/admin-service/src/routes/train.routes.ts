import { Router } from "express";
import { trainController } from "../controllers/train.controller";

const router = Router();

// Mounted at /trains in server.ts, so this resolves to POST /trains/train.
// No auth/user-context middleware is applied here — anything that can reach
// this service can create a train.
router.post("/train", trainController.createTrain);

export default router;
