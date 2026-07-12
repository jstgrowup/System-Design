// ============================================
// API Gateway Routes
// ============================================
// This file defines all routes handled by the API Gateway.
// Each route forwards requests to appropriate microservices.

import express from "express";
import { requireAuth } from "../middlewares/auth.middleware";
import {
  combinedRateLimit,
  endpointRateLimit,
} from "../middlewares/rate-limiting.middleware";
import { createProxy } from "../services/proxy";
import { config } from "../config";

// Create router instance for all gateway routes
export const gatewayRouter = express.Router();

// Create proxy handler for user service
// This will forward requests to USER_SERVICE_URL (http://localhost:4001 by default)
const userServiceProxy = createProxy(
  "userService",
  config.SERVICES.USER_SERVICE_URL,
);

// ============================================
// ROUTE 1: User Login (Unauthenticated)
// ============================================
// POST /api/users/auth/login
// No authentication required (login creates token)
// Rate limited: 10 requests per 15 minutes per IP (prevents brute force)
gatewayRouter.post(
  "/users/auth/login",
  endpointRateLimit(10, 900000),  // 10 requests, 900000ms = 15 minutes
  userServiceProxy,               // Forward to user service /auth/login
);

// ============================================
// ROUTE 2: Get User Profile (Authenticated)
// ============================================
// GET /api/users/user/profile
// Requires valid JWT token in Authorization header
// Rate limited: Combined IP + user-based (IP: 100/15min, User: 1000/15min)
gatewayRouter.get(
  "/users/user/profile",
  requireAuth,              // Check JWT token
  combinedRateLimit(),      // Apply IP + user rate limiting
  userServiceProxy,         // Forward to user service /user/profile
);

// ============================================
// ROUTE 3: Gateway Health Check
// ============================================
// GET /api/gateway/health
// Returns gateway status (no proxying needed)
gatewayRouter.get("/gateway/health", (req, res) => {
  return res
    .status(200)
    .json({
      success: true,
      message: "Gateway is healthy",
      timestamp: new Date().toString()
    });
});
