import { Router } from "express";
import { userController } from "../controllers/user.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

router.post("/profile", getUserContext, userController.getProfile);
router.put("/profile", getUserContext, userController.updateProfile);
router.delete("/profile", getUserContext, userController.deleteProfile);
export default router;
