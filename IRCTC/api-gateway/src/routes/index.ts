import express from "express";
import { requireAuth } from "../middlewares/auth.middleware";
import {
  combinedRateLimit,
  endpointRateLimit,
} from "../middlewares/rate-limiting.middleware";
import { createProxy } from "../services/proxy";
import { config } from "../config";
export const gatewayRouter = express.Router();
const userServiceProxy = createProxy(
  "userService",
  config.SERVICES.USER_SERVICE_URL,
);
gatewayRouter.post(
  "/users/auth/login",
  endpointRateLimit(10, 900000),
  userServiceProxy,
); //10 requests per 15min
gatewayRouter.get(
  "/users/user/profile",
  requireAuth,
  combinedRateLimit(),
  userServiceProxy,
);
gatewayRouter.get("/gateway/health", (req, res) => {
  return res
    .status(200)
    .json({ success: true, message: true, timestamp: new Date().toString() });
});
