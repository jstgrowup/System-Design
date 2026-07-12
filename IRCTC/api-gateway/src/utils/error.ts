// ============================================
// Custom Error Classes
// ============================================
// Every error thrown here extends AppError, so errorMiddleware.ts can
// distinguish "expected" errors (with an intended status/code) from
// truly unexpected exceptions (which fall back to a generic 500).

// Base class — carries the HTTP status code and a machine-readable code
// alongside the human-readable message inherited from Error.
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

// 400 — malformed/invalid request body, params, or query
export class BadRequestError extends AppError {
  constructor(message: string, code: string = "BAD_REQUEST") {
    super(message, 400, code);
  }
}

// 401 — missing, invalid, or expired auth token (thrown by auth.middleware.ts)
export class UnauthorizedError extends AppError {
  constructor(message: string, code: string = "UNAUTHORIZED") {
    super(message, 401, code);
  }
}

// 403 — caller is authenticated but not allowed to perform this action
export class ForbiddenError extends AppError {
  constructor(message: string, code: string = "FORBIDDEN") {
    super(message, 403, code);
  }
}

// 404 — used by not-found.middleware.ts for unmatched routes
export class NotFoundError extends AppError {
  constructor(message: string, code: string = "NOT_FOUND") {
    super(message, 404, code);
  }
}

// 409 — request conflicts with existing state (e.g. duplicate resource)
export class ConflictError extends AppError {
  constructor(message: string, code: string = "CONFLICT") {
    super(message, 409, code);
  }
}

// 429 — thrown by rate-limiting.middleware.ts when a limit is exceeded.
// Carries retryAfter (seconds) so clients know how long to back off.
export class TooManyRequestsError extends AppError {
  public retryAfter: number;

  constructor(
    message: string,
    retryAfter: number = 60,
    code: string = "TOO_MANY_REQUESTS",
  ) {
    super(message, 429, code);
    this.retryAfter = retryAfter;
  }
}

// 500 — generic internal error (errorMiddleware.ts also falls back to this
// shape directly for any error that isn't an AppError instance)
export class InternalServerError extends AppError {
  constructor(
    message: string = "Internal Server Error",
    code: string = "SERVER_ERROR",
  ) {
    super(message, 500, code);
  }
}

// 503 — downstream service is down or its circuit breaker is OPEN
// (thrown by services/proxy.ts)
export class ServiceUnavailableError extends AppError {
  constructor(message: string, errorCode: string = "SERVICE_UNAVAILABLE") {
    super(message, 503, errorCode);
  }
}

// 504 — downstream service didn't respond within SERVICE_TIMEOUT_MS
// (thrown by services/proxy.ts)
export class GatewayTimeoutError extends AppError {
  constructor(message: string, errorCode: string = "GATEWAY_TIMEOUT") {
    super(message, 504, errorCode);
  }
}
