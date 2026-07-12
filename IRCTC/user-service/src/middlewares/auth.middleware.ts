import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { config } from "../config";
import { UnauthorizedError } from "../utils/error";
import logger from "../config/logger";

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
function requireAuth(req: Request, res: Response, next: NextFunction): void {
  try {
    let accessToken: string | undefined;

    // 1. Try Authorization header (service-to-service / mobile clients)
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      accessToken = authHeader.split(" ")[1];
    }

    // 2. Fall back to httpOnly cookie (browser clients)
    if (!accessToken && req.cookies) {
      accessToken = req.cookies.accessToken;
    }

    if (!accessToken) {
      throw new UnauthorizedError("Authorization token missing");
    }

    // Verify access token
    const payload = jwt.verify(
      accessToken,
      config.JWT_ACCESS_SECRET,
    ) as AccessTokenPayload;

    if (!payload.id) {
      throw new UnauthorizedError("Invalid token payload");
    }

    // Attach user context to request for downstream services
    req.user = {
      id: payload.id,
    };

    // Add user ID to headers for proxied requests
    req.headers["x-user-id"] = payload.id.toString();

    logger.debug(`User ${payload.id} authenticated successfully`);
    next();
  } catch (err) {
    const error = err as Error;

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
    return next(error);
  }
}

export { requireAuth };
