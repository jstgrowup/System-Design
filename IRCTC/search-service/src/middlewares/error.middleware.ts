// ============================================
// Global Error Handling Middleware
// ============================================
// Catches all errors from routes and middlewares, formats them consistently,
// and sends appropriate HTTP responses back to clients.
// IMPORTANT: Must be registered LAST in middleware chain (after all other middleware)

import { Request, Response, NextFunction } from "express";
import { AppError } from "../utils/error";
import { config } from "../config";
import logger from "../config/logger";

/**
 * Global error handler - processes all errors thrown in the application
 *
 * Error Handling Strategy:
 * 1. If error is AppError → Return custom status code and error code
 * 2. If error is unexpected → Return generic 500 error
 * 3. Always log detailed error information (except in production)
 */
export const errorMiddleware = (
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  // ============================================
  // STEP 1: Check if error is an expected AppError
  // ============================================
  if (err instanceof AppError) {
    // Return error with correct HTTP status code and error code
    // Examples:
    // - UnauthorizedError (401)
    // - TooManyRequestsError (429)
    // - NotFoundError (404)
    // - ServiceUnavailableError (503)
    res.status(err.statusCode).json({
      success: false,
      error: err.code,           // Machine-readable error code
      message: err.message,      // Human-readable message
    });
    return;
  }

  // ============================================
  // STEP 2: Handle Unexpected Errors
  // ============================================
  console.error("UNHANDLED ERROR:", err);

  // Log detailed error info (but not in production for security)
  if (config.NODE_ENV !== "production") {
    logger.error({
      message: err.message,
      stack: err.stack,            // Stack trace for debugging
      path: req.path,              // Request path
      method: req.method,          // HTTP method
      body: req.body,              // Request body (might reveal issue)
      query: req.query,            // Query parameters
    });
  }

  // ============================================
  // STEP 3: Return Generic Error to Client
  // ============================================
  // Return 500 Internal Server Error without exposing details
  // This prevents information leakage about server internals
  res.status(500).json({
    success: false,
    error: "SERVER_ERROR",
    message: "Internal Server Error",
  });
};
