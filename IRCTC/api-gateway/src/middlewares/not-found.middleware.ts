// ============================================
// 404 Not Found Middleware
// ============================================
// Registered in index.ts right after `app.use("/api", gatewayRouter)`.
// Express only reaches this handler if no earlier route matched the request,
// so its job is simply to turn "no match" into a proper NotFoundError.

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
  // Pass a NotFoundError (404) to the next error-handling middleware
  // (errorMiddleware) instead of responding directly — keeps error
  // formatting centralized in one place.
  next(new NotFoundError(`Route ${req.method} ${req.path} not found`));
}
