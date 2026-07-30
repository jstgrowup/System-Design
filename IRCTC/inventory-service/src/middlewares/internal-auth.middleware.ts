import { Request, Response, NextFunction } from "express";
import { config } from "../config";
import { ForbiddenError } from "../utils/error";

/**
 * Validates that the request comes from another backend service (not a
 * browser client) by checking a shared secret header. Used on routes that
 * mutate seat state (lock/unlock/confirm/cancel) so only booking-service can
 * call them directly — end users go through booking-service, not here.
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
