import { Request, Response, NextFunction } from "express";
import { config } from "../config";
import { ForbiddenError } from "../utils/error";

/**
 * Validates that the request comes from another backend service (not a
 * browser client) by checking a shared secret header. Used on routes that
 * other services need to call directly (e.g. booking-service resolving a
 * station's name) without going through the gateway's JWT flow.
 */
export function internalAuth(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const serviceKey = req.headers["x-internal-service-key"];

  if (!serviceKey || serviceKey !== config.INTERNAL_SERVICE_KEY) {
    return next(new ForbiddenError("Invalid or missing internal service key"));
  }

  next();
}
