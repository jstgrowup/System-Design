import { Router } from "express";
import { authController } from "../controllers/auth.controller";

const router = Router();

router.post("/send-otp", authController.sendOtp);

export default router;
