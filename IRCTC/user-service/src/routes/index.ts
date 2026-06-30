import { Router } from "express";
import { authController } from "../controllers/auth.controller";

const router = Router();

router.post("/", authController.sendOtp);

export default router;
