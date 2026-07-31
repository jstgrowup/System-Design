import { Request, Response, NextFunction } from "express";
import { config } from "../config";
import { ForbiddenError } from "../utils/error";

/**
 * Validates that the request comes from another backend service (not a
 * browser client) by checking a shared secret header. Every route this
 * service exposes except the public webhook is behind this — booking-service
 * is the only intended caller.
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
