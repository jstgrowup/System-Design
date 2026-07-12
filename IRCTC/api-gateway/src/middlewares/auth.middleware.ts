// ============================================
// JWT Authentication Middleware
// ============================================
// Applied selectively per-route in routes/index.ts (e.g. requireAuth before
// combinedRateLimit() on GET /users/user/profile). Routes that don't list it
// — like POST /users/auth/login — remain publicly reachable.

import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { config } from "../config";
import logger from "../config/logger";
import { UnauthorizedError } from "../utils/error";

interface AccessTokenPayload {
  id: string;
}

// Extend Express's Request type so req.user is recognized project-wide
declare global {
  namespace Express {
    interface Request {
      user?: {
        id: string;
      };
    }
  }
}

/**
 * Middleware to verify access token from Authorization header.
 * This is going to be our authentication mechanism which will authenticate user.
 * Extracts user ID and attaches it to request headers for downstream services.
 */
export function requireAuth(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  try {
    let accessToken: string | undefined;

    // ============================================
    // STEP 1: Try Authorization Header
    // ============================================
    // Format: "Authorization: Bearer <token>" — used by mobile clients
    // and service-to-service calls.
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      accessToken = authHeader.split(" ")[1];
    }

    // ============================================
    // STEP 2: Fall Back to httpOnly Cookie
    // ============================================
    // Browser clients that store the token in a cookie instead of
    // manually attaching an Authorization header.
    if (!accessToken && req.cookies) {
      accessToken = req.cookies.accessToken;
    }

    // ============================================
    // STEP 3: Reject if No Token Found
    // ============================================
    if (!accessToken) {
      throw new UnauthorizedError("Authorization token missing");
    }

    // ============================================
    // STEP 4: Verify Token Signature & Expiry
    // ============================================
    // jwt.verify throws TokenExpiredError / JsonWebTokenError on failure,
    // which are caught below and translated into UnauthorizedError.
    const payload = jwt.verify(
      accessToken,
      config.JWT_ACCESS_SECRET,
    ) as AccessTokenPayload;

    // ============================================
    // STEP 5: Validate Payload Shape
    // ============================================
    if (!payload.id) {
      throw new UnauthorizedError("Invalid token payload");
    }

    // ============================================
    // STEP 6: Attach User Context to Request
    // ============================================
    // Makes req.user.id available to later middlewares in this request
    // (e.g. userRateLimit() reads req.user.id to key its Redis limit).
    req.user = {
      id: payload.id,
    };

    // ============================================
    // STEP 7: Forward Identity to Downstream Service
    // ============================================
    // The gateway strips its own auth header handling; this custom header
    // is how the downstream microservice learns who the caller is without
    // having to verify the JWT itself.
    req.headers["x-user-id"] = payload.id.toString();

    logger.debug(`User ${payload.id} authenticated successfully`);

    // ============================================
    // STEP 8: Continue to Next Middleware/Route
    // ============================================
    next();
  } catch (err) {
    const error = err as Error;

    // Map specific jsonwebtoken error types to clearer client-facing codes
    // so the frontend can distinguish "expired, please refresh" from
    // "invalid, please log in again".
    if (error.name === "TokenExpiredError") {
      return next(
        new UnauthorizedError("Access token expired", "TOKEN_EXPIRED"),
      );
    }
    if (error.name === "JsonWebTokenError") {
      return next(
        new UnauthorizedError("Invalid access token", "TOKEN_INVALID"),
      );
    }
    // Anything else (e.g. UnauthorizedError thrown above) passes through
    // to the global error middleware unchanged.
    return next(error);
  }
}
