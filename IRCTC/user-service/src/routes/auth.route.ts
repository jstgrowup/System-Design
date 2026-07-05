import { Router } from "express";
import { authController } from "../controllers/auth.controller";

const router = Router();

router.post("/send-otp", authController.sendOtp);
router.post("/verify-otp", authController.verifyOtp);

export default router;
