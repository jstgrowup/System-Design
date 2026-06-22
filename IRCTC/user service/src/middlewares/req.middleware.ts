import { Request, Response, NextFunction } from "express";
import logger from "../config/logger";

export const reqLogger = (
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  logger.debug(`[${req.method}] ${req.originalUrl}`);

  const start: number = Date.now();

  res.on("finish", (): void => {
    const duration: number = Date.now() - start;
    logger.info(
      `[${req.method}] ${req.originalUrl} - status: ${res.statusCode} - ${duration}ms`,
    );
  });

  next();
};
