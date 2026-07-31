export class AppError extends Error {
  public statusCode: number;
  public code: string;

  constructor(message: string, statusCode: number, code: string) {
    super(message);
    this.statusCode = statusCode;
    this.code = code; // custom error code

    // Maintains proper stack trace for where our error was thrown (only available on V8 environments like Node.js)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
}

export class BadRequestError extends AppError {
  constructor(message: string, code: string = "BAD_REQUEST") {
    super(message, 400, code);
  }
}

export class UnauthorizedError extends AppError {
  constructor(message: string, code: string = "UNAUTHORIZED") {
    super(message, 401, code);
  }
}

export class ForbiddenError extends AppError {
  constructor(message: string, code: string = "FORBIDDEN") {
    super(message, 403, code);
  }
}

export class NotFoundError extends AppError {
  constructor(message: string, code: string = "NOT_FOUND") {
    super(message, 404, code);
  }
}

export class ConflictError extends AppError {
  constructor(message: string, code: string = "CONFLICT") {
    super(message, 409, code);
  }
}

export class TooManyRequestsError extends AppError {
  constructor(message: string, code: string = "TOO_MANY_REQUESTS") {
    super(message, 429, code);
  }
}

export class InternalServerError extends AppError {
  constructor(
    message: string = "Internal Server Error",
    code: string = "SERVER_ERROR",
  ) {
    super(message, 500, code);
  }
}

/**
 * Thrown by the optimistic-lock (CAS) helper when a booking's `version` no
 * longer matches what the caller expected — meaning another process (the
 * expiry job, a payment webhook, a user cancel) already transitioned this
 * booking first. Callers catch this specifically to bail out silently
 * instead of double-processing the same booking.
 */
export class StaleStateError extends ConflictError {
  constructor(
    message: string = "Booking state changed by another process",
    code: string = "STALE_STATE",
  ) {
    super(message, code);
  }
}
