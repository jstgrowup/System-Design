// ============================================
// Request Logging Middleware
// ============================================
// Registered early in index.ts (right after helmet), so it wraps
// every single request that reaches the gateway.

import { Request, Response, NextFunction } from "express";
import logger from "../config/logger";

/**
 * Logs every incoming request (debug level) and its completion (info level),
 * including status code and total processing time.
 */
export const reqLogger = (
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  // Log as soon as the request arrives (before it's processed)
  logger.debug(`[${req.method}] ${req.originalUrl}`);

  // Record start time to measure total request duration
  const start = Date.now();

  // "finish" fires once the response has been fully sent to the client —
  // this is how we know the final status code and total time taken,
  // regardless of which route/middleware/proxy ultimately handled it.
  res.on("finish", () => {
    const duration = Date.now() - start;
    logger.info(
      `[${req.method}] ${req.originalUrl} - status: ${res.statusCode} - ${duration}ms`,
    );
  });

  next();
};
