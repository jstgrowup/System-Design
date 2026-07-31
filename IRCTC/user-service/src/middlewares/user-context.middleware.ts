import { Request, Response, NextFunction } from "express";
import { UnauthorizedError } from "../utils/error";

/**
 * Extracts the caller's user ID from the x-user-id header set by the API
 * gateway after it verifies the JWT. This middleware performs no verification
 * of its own — it trusts the header outright — so it must only sit behind
 * routes reachable exclusively through the gateway, never exposed directly.
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
