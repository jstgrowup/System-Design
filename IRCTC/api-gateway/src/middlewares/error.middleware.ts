import { Request, Response, NextFunction } from "express";
import { AppError } from "../utils/error";
import { config } from "../config";
import logger from "../config/logger";

/**
 * Global error-handling middleware — must be registered last, after all routes.
 * Known AppError instances are returned with their intended status code and code.
 * Anything else is treated as unexpected and returned as a generic 500.
 */
export const errorMiddleware = (
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      error: err.code,
      message: err.message,
    });
    return;
  }

  console.error("UNHANDLED ERROR:", err);

  if (config.NODE_ENV !== "production") {
    logger.error({
      message: err.message,
      stack: err.stack,
      path: req.path,
      method: req.method,
      body: req.body,
      query: req.query,
    });
  }

  res.status(500).json({
    success: false,
    error: "SERVER_ERROR",
    message: "Internal Server Error",
  });
};
