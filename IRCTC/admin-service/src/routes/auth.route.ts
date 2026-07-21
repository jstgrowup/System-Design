import { Router } from "express";
import { authController } from "../controllers/station.controller";

const router = Router();

router.post("/send-otp", authController.sendOtp);
router.post("/verify-otp", authController.verifyOtp);
router.post("/login", authController.login);
router.post("/refresh", authController.rotateRefreshToken);

export default router;
