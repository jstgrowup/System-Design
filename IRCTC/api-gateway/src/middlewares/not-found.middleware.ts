import { Request, Response, NextFunction } from "express";
import { NotFoundError } from "../utils/error";

/**
 * Catch-all handler for unmatched routes — should be registered after
 * all other routes but before the global error middleware.
 */
export function notFound(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  next(new NotFoundError(`Route ${req.method} ${req.path} not found`));
}
