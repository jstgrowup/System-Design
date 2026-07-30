import { Router } from "express";
import { userController } from "../controllers/user.controller";
import { getUserContext } from "../middlewares/user-context.middleware";
import { internalAuth } from "../middlewares/internal-auth.middleware";

const router = Router();

// Internal only: called by other services (e.g. booking-service) with a
// shared secret, not a logged-in user's JWT.
router.get("/internal/:userId", internalAuth, userController.getUserByIdInternal);

router.post("/profile", getUserContext, userController.getProfile);
router.put("/profile", getUserContext, userController.updateProfile);
router.delete("/profile", getUserContext, userController.deleteProfile);
export default router;
