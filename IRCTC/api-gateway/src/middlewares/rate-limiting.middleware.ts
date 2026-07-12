import { Request, Response, NextFunction } from "express";
import { redis } from "../config/redis";
import { config } from "../config";
import { TooManyRequestsError } from "../utils/error";
import logger from "../config/logger";

/**
 * Rate limiting strategies:
 * 1. IP-based rate limiting (for unauthenticated users)
 * 2. User-based rate limiting (for authenticated users)
 * 3. Endpoint-specific rate limiting
 */

interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetTime?: number;
  retryAfter?: number;
}

interface RateLimitOptions {
  max?: number;
  windowMs?: number;
}

/**
 * Generic rate limiter using a sliding window algorithm backed by a Redis sorted set.
 */
async function rateLimiter(
  key: string,
  maxRequests: number,
  windowMs: number,
): Promise<RateLimitResult> {
  const now = Date.now();
  const windowStart = now - windowMs;

  try {
    // Use Redis pipeline for atomic operations
    const pipeline = redis.pipeline();

    // Remove old entries outside the current window
    pipeline.zremrangebyscore(key, 0, windowStart);

    // Add current request
    pipeline.zadd(key, now, `${now}-${Math.random()}`);

    // Count requests in current window
    pipeline.zcard(key);

    // Set expiry on the key
    pipeline.expire(key, Math.ceil(windowMs / 1000));

    const results = await pipeline.exec();

    if (!results) {
      throw new Error("Redis pipeline returned no results");
    }

    // Get the count from the third command (index 2)
    const requestCount = results[2][1] as number;

    if (requestCount > maxRequests) {
      const oldestRequest = await redis.zrange(key, 0, 0, "WITHSCORES");
      const resetTime = parseInt(oldestRequest[1], 10) + windowMs;
      const retryAfter = Math.ceil((resetTime - now) / 1000);

      return {
        allowed: false,
        remaining: 0,
        resetTime,
        retryAfter,
      };
    }

    return {
      allowed: true,
      remaining: maxRequests - requestCount,
      resetTime: windowStart + windowMs,
    };
  } catch (err) {
    logger.error("Rate limiter error:", err);
    // Fail open - allow request if Redis is down
    return { allowed: true, remaining: maxRequests };
  }
}

/**
 * IP-based rate limiting middleware.
 * Default: 100 requests per 15 minutes.
 */
export function ipRateLimit(options: RateLimitOptions = {}) {
  const maxRequests = options.max || config.RATE_LIMIT_MAX_REQUESTS;
  const windowMs = options.windowMs || config.RATE_LIMIT_WINDOW_MS;

  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    const key = `ratelimit:ip:${ip}`;

    const result = await rateLimiter(key, maxRequests, windowMs);

    // Set rate limit headers
    res.setHeader("X-RateLimit-Limit", maxRequests);
    res.setHeader("X-RateLimit-Remaining", result.remaining);
    if (result.resetTime) {
      res.setHeader(
        "X-RateLimit-Reset",
        new Date(result.resetTime).toISOString(),
      );
    }

    if (!result.allowed) {
      res.setHeader("Retry-After", result.retryAfter ?? 0);
      logger.warn(`Rate limit exceeded for IP: ${ip}`);
      return next(
        new TooManyRequestsError(
          `Too many requests. Please try again in ${result.retryAfter} seconds`,
        ),
      );
    }

    next();
  };
}

/**
 * User-based rate limiting middleware.
 * Should be used after authentication middleware.
 * Default: 1000 requests per 15 minutes (more lenient than IP-based).
 */
export function userRateLimit(options: RateLimitOptions = {}) {
  const maxRequests = options.max || config.RATE_LIMIT_MAX_REQUESTS * 10;
  const windowMs = options.windowMs || config.RATE_LIMIT_WINDOW_MS;

  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    // Skip if no user authenticated
    if (!req.user || !req.user.id) {
      return next();
    }

    const userId = req.user.id;
    const key = `ratelimit:user:${userId}`;

    const result = await rateLimiter(key, maxRequests, windowMs);

    res.setHeader("X-RateLimit-Limit", maxRequests);
    res.setHeader("X-RateLimit-Remaining", result.remaining);
    if (result.resetTime) {
      res.setHeader(
        "X-RateLimit-Reset",
        new Date(result.resetTime).toISOString(),
      );
    }

    if (!result.allowed) {
      res.setHeader("Retry-After", result.retryAfter ?? 0);
      logger.warn(`Rate limit exceeded for user: ${userId}`);
      return next(
        new TooManyRequestsError(
          `Too many requests. Please try again in ${result.retryAfter} seconds`,
        ),
      );
    }

    next();
  };
}

/**
 * Endpoint-specific rate limiting.
 * Example: POST /api/auth/send-otp - 5 requests per hour.
 */
export function endpointRateLimit(maxRequests: number, windowMs: number) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    const endpoint = `${req.method}:${req.path}`;
    const key = `ratelimit:endpoint:${endpoint}:${ip}`;

    const result = await rateLimiter(key, maxRequests, windowMs);

    res.setHeader("X-RateLimit-Limit", maxRequests);
    res.setHeader("X-RateLimit-Remaining", result.remaining);
    if (result.resetTime) {
      res.setHeader(
        "X-RateLimit-Reset",
        new Date(result.resetTime).toISOString(),
      );
    }

    if (!result.allowed) {
      res.setHeader("Retry-After", result.retryAfter ?? 0);
      logger.warn(
        `Endpoint rate limit exceeded for ${endpoint} from IP: ${ip}`,
      );
      return next(
        new TooManyRequestsError(
          `Too many requests to this endpoint. Please try again in ${result.retryAfter} seconds`,
        ),
      );
    }

    next();
  };
}

/**
 * Combined rate limiting strategy.
 * Applies both IP and user-based rate limiting.
 */
export function combinedRateLimit(
  ipOptions: RateLimitOptions = {},
  userOptions: RateLimitOptions = {},
) {
  const ipLimiter = ipRateLimit(ipOptions);
  const userLimiter = userRateLimit(userOptions);

  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    // Apply IP rate limit first
    await ipLimiter(req, res, (err?: unknown) => {
      if (err) return next(err as Error);

      // Then apply user rate limit if authenticated
      userLimiter(req, res, next);
    });
  };
}
