import { Request, Response, NextFunction } from "express";
import { AppError } from "../utils/error";

// Note: Express requires all 4 parameters (err, req, res, next) to recognize this as an error-handling middleware.
const errorHandler = (
  err: Error | AppError,
  req: Request,
  res: Response,
  next: NextFunction,
): void | Response => {
  // Check if the error is a trusted operational error (instance of our custom AppError)
  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      error: err.code,
      message: err.message,
    });
  }

  // Fallback for unhandled, unknown programming errors (e.g., database connection failure, syntax bug)
  console.error("UNHANDLED ERROR:", err);

  return res.status(500).json({
    success: false,
    error: "INTERNAL_SERVER_ERROR",
    message: "Something went wrong on our end.",
  });
};

export default errorHandler;
