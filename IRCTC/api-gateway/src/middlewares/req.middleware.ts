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
  logger.debug(`[${req.method}] ${req.originalUrl}`);

  const start = Date.now();

  res.on("finish", () => {
    const duration = Date.now() - start;
    logger.info(
      `[${req.method}] ${req.originalUrl} - status: ${res.statusCode} - ${duration}ms`,
    );
  });

  next();
};
