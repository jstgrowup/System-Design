import { Router } from "express";
import { userController } from "../controllers/user.controller";
import { requireAuth } from "../middlewares/auth.middleware";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

router.post("/profile", requireAuth, getUserContext, userController.getProfile);
router.put(
  "/profile",
  requireAuth,
  getUserContext,
  userController.updateProfile,
);
router.delete(
  "/profile",
  requireAuth,
  getUserContext,
  userController.deleteProfile,
);
export default router;
