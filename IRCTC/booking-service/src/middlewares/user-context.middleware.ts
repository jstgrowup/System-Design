import { Request, Response, NextFunction } from "express";
import { UnauthorizedError } from "../utils/error";

/**
 * Extract user context from the gateway's x-user-id header, set after JWT
 * verification. Every booking route requires this — end users only ever
 * reach this service through the gateway.
 */
export function getUserContext(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const userId = req.headers["x-user-id"];

  if (!userId) {
    return next(
      new UnauthorizedError("User context missing - must come through gateway"),
    );
  }

  req.user = { id: Array.isArray(userId) ? userId[0] : userId };
  next();
}
