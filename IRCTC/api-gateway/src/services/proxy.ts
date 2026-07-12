import axios, { AxiosRequestConfig, AxiosResponse } from "axios";
import { Request, Response, NextFunction } from "express";
import { config } from "../config";
import { ServiceUnavailableError, GatewayTimeoutError } from "../utils/error";
import logger from "../config/logger";

type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

interface CircuitBreakerStatus {
  service: string;
  state: CircuitState;
  failureCount: number;
  nextAttempt: string | null;
}

/**
 * Circuit Breaker implementation.
 * Prevents cascading failures when downstream services are down.
 */
class CircuitBreaker {
  private serviceName: string;
  private failureCount: number;
  private threshold: number;
  private timeout: number;
  private state: CircuitState;
  private nextAttempt: number;

  constructor(
    serviceName: string,
    threshold: number = config.CIRCUIT_BREAKER_THRESHOLD,
    timeout: number = config.CIRCUIT_BREAKER_TIMEOUT,
  ) {
    this.serviceName = serviceName;
    this.failureCount = 0;
    this.threshold = threshold;
    this.timeout = timeout;
    this.state = "CLOSED";
    this.nextAttempt = Date.now();
  }

  async execute<T>(request: () => Promise<T>): Promise<T> {
    if (this.state === "OPEN") {
      if (Date.now() < this.nextAttempt) {
        throw new ServiceUnavailableError(
          `Service ${this.serviceName} is temporarily unavailable. Circuit breaker is OPEN.`,
        );
      }
      // Try to close the circuit
      this.state = "HALF_OPEN";
      logger.info(`Circuit breaker HALF_OPEN for ${this.serviceName}`);
    }

    try {
      const response = await request();
      this.onSuccess();
      return response;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }

  private onSuccess(): void {
    this.failureCount = 0;
    if (this.state === "HALF_OPEN") {
      this.state = "CLOSED";
      logger.info(`Circuit breaker CLOSED for ${this.serviceName}`);
    }
  }

  private onFailure(): void {
    this.failureCount++;
    if (this.failureCount >= this.threshold) {
      this.state = "OPEN";
      this.nextAttempt = Date.now() + this.timeout;
      logger.error(
        `Circuit breaker OPEN for ${this.serviceName}. Next attempt at ${new Date(this.nextAttempt).toISOString()}`,
      );
    }
  }

  getState(): CircuitBreakerStatus {
    return {
      service: this.serviceName,
      state: this.state,
      failureCount: this.failureCount,
      nextAttempt:
        this.state === "OPEN" ? new Date(this.nextAttempt).toISOString() : null,
    };
  }
}

// Circuit breakers for each service
const circuitBreakers: Record<string, CircuitBreaker> = {
  userService: new CircuitBreaker("user-service"),
  searchService: new CircuitBreaker("search-service"),
  adminService: new CircuitBreaker("admin-service"),
  notificationService: new CircuitBreaker("notification-service"),
  bookingService: new CircuitBreaker("booking-service"),
  paymentService: new CircuitBreaker("payment-service"),
  inventoryService: new CircuitBreaker("inventory-service"),
};

interface ForwardedResponse {
  status: number;
  data: unknown;
  headers: Record<string, unknown>;
}
function normalizeHeaders(headers: Request["headers"]): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (key === "host" || key === "content-length") continue;
    if (Array.isArray(value)) {
      result[key] = value.join(", ");
    } else if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}
/**
 * Forwards a request to a downstream service, wrapped in the given circuit breaker.
 */
async function forwardRequest(
  serviceUrl: string,
  path: string,
  method: string,
  data: unknown,
  headers: Request["headers"],
  circuitBreaker: CircuitBreaker,
): Promise<ForwardedResponse> {
  const url = `${serviceUrl}${path}`;
  logger.info(url);
  // http://localhost:4001/auth/login

  const requestConfig: AxiosRequestConfig = {
    method,
    url,
    timeout: config.SERVICE_TIMEOUT_MS,
    headers: normalizeHeaders(headers),
    validateStatus: () => true,
    maxRedirects: 5,
  };

  // Add data based on method
  if (method !== "GET" && method !== "DELETE" && data) {
    requestConfig.data = data;
  }

  // For GET and DELETE, add params if data exists
  if ((method === "GET" || method === "DELETE") && data) {
    requestConfig.params = data;
  }

  logger.debug(`Forwarding ${method} ${url}`, {
    headers: requestConfig.headers,
    hasData: !!data,
    timeout: config.SERVICE_TIMEOUT_MS,
  });

  try {
    const response: AxiosResponse = await circuitBreaker.execute(() =>
      axios(requestConfig),
    );

    logger.debug(`Response from ${url}:`, {
      status: response.status,
      statusText: response.statusText,
    });

    return {
      status: response.status,
      data: response.data,
      headers: response.headers,
    };
  } catch (error) {
    const err = error as {
      message: string;
      code?: string;
      response?: {
        status: number;
        data: unknown;
        headers: Record<string, unknown>;
      };
    };

    logger.error(`Error forwarding to ${serviceUrl}:`, {
      message: err.message,
      code: err.code,
      url,
      method,
      timeout: config.SERVICE_TIMEOUT_MS,
    });

    if (err.code === "ECONNABORTED" || err.code === "ETIMEDOUT") {
      throw new GatewayTimeoutError(
        `Request to ${serviceUrl} timed out after ${config.SERVICE_TIMEOUT_MS}ms`,
      );
    }

    if (err.code === "ECONNREFUSED") {
      throw new ServiceUnavailableError(
        `Cannot connect to ${serviceUrl}. Service may be down.`,
      );
    }

    if (err.response) {
      logger.error(`Service error from ${serviceUrl}:`, {
        status: err.response.status,
        data: err.response.data,
      });

      return {
        status: err.response.status,
        data: err.response.data,
        headers: err.response.headers,
      };
    }

    // Network error or service down — you would have seen this in video
    logger.error(`Network error while calling ${serviceUrl}:`, err.message);
    throw new ServiceUnavailableError(
      `Service temporarily unavailable: ${err.message}`,
    );
  }
}

/**
 * Proxy middleware factory — creates an Express handler that forwards
 * requests to the given downstream service through its circuit breaker.
 */
function createProxy(serviceName: string, serviceUrl: string) {
  const circuitBreaker = circuitBreakers[serviceName];

  if (!circuitBreaker) {
    throw new Error(`No circuit breaker found for service: ${serviceName}`);
  }

  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      // Extract path (remove /api prefix only)
      // Gateway: /api/users/auth/login -> Service: /auth/login
      // Gateway: /api/users/user/profile -> Service: /user/profile
      logger.info(req.path);
      const pathParts = req.path.split("/").filter(Boolean);
      logger.info(pathParts);

      // Remove 'users' (first part), keep the rest
      // ['users', 'auth', 'login'] -> ['auth', 'login'] -> '/auth/login'
      const servicePath = "/" + pathParts.slice(1).join("/");
      logger.info(servicePath);

      const result = await forwardRequest(
        serviceUrl,
        servicePath +
          (req.url.includes("?")
            ? req.url.substring(req.url.indexOf("?"))
            : ""),
        req.method,
        req.body,
        req.headers,
        circuitBreaker,
      );

      // Forward response headers (except some)
      const excludeHeaders = [
        "connection",
        "keep-alive",
        "transfer-encoding",
        "host",
      ];
      Object.keys(result.headers).forEach((key) => {
        if (!excludeHeaders.includes(key.toLowerCase())) {
          res.setHeader(key, result.headers[key] as string);
        }
      });

      // Send response
      res.status(result.status).json(result.data);
    } catch (err) {
      next(err);
    }
  };
}

/**
 * Health check endpoint helper — returns the current state of every circuit breaker.
 */
function getCircuitBreakerStatus(): CircuitBreakerStatus[] {
  return Object.values(circuitBreakers).map((cb) => cb.getState());
}

export {
  createProxy,
  CircuitBreaker,
  circuitBreakers,
  getCircuitBreakerStatus,
};
