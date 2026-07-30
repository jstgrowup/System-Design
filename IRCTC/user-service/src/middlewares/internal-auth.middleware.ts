import { Request, Response, NextFunction } from "express";
import { config } from "../config";
import { ForbiddenError } from "../utils/error";

/**
 * Validates that the request comes from another backend service (not a
 * browser client) by checking a shared secret header. Used on the internal
 * user-lookup route so other services (e.g. booking-service, resolving the
 * user attached to a booking) can read a user's profile without a JWT.
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
