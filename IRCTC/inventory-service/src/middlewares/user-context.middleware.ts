import { Request, Response, NextFunction } from "express";
import { UnauthorizedError } from "../utils/error";

/**
 * Extract user context from gateway headers.
 * Gateway sets x-user-id after JWT verification (discussed in video).
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
