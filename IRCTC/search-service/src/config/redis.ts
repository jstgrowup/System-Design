// ============================================
// Redis Client - Singleton Connection
// ============================================
// Manages single Redis connection for the entire application.
// Used for: rate limiting storage, caching, sessions.
// Pattern: Singleton (ensures only one connection)

import Redis from "ioredis";
import { config } from ".";
import logger from "./logger";

class RedisClient {
  // Singleton instance - shared across application
  private static instance: Redis;

  // Track connection status for health checks
  private static isConnected = false;

  // Prevent direct instantiation - use getInstance()
  private constructor() {
    // prevent direct instantiation
  }

  // ============================================
  // Get or Create Redis Connection
  // ============================================
  // Returns the singleton Redis instance.
  // Creates connection on first call.
  static getInstance(): Redis {
    if (!RedisClient.instance) {
      // First time - create the Redis connection
      RedisClient.instance = new Redis(config.REDIS_URL as string, {
        // Exponential backoff retry strategy
        // 1st retry: 50ms, 2nd: 100ms, 3rd: 150ms... max 2000ms
        retryStrategy: (times: number) => {
          const delay = Math.min(times * 50, 2000);
          return delay;
        },
        // Maximum 3 retries per request before failing
        maxRetriesPerRequest: 3,
      });
      // Set up event listeners for connection events
      RedisClient.setupEventListeners();
    }
    return RedisClient.instance;
  }

  private static setupEventListeners(): void {
    RedisClient.instance.on("connect", () => {
      RedisClient.isConnected = true;
      logger.info("Connected to Redis");
    });

    RedisClient.instance.on("error", (error: Error) => {
      RedisClient.isConnected = false;
      logger.error("Redis connection error", error);
    });

    RedisClient.instance.on("close", () => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection closed");
    });

    RedisClient.instance.on("reconnecting", () => {
      logger.warn("Reconnecting to Redis...");
    });

    RedisClient.instance.on("ready", () => {
      logger.warn("Redis client is ready");
    });

    RedisClient.instance.on("end", () => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection ended");
    });
  }

  // Gracefully closes the Redis connection.
  // NOTE: not currently called from index.ts's gracefulShutdown() — that
  // function only closes the HTTP server, so the Redis connection is left
  // to close on process exit rather than being shut down explicitly.
  static async closeConnection(): Promise<void> {
    if (RedisClient.instance) {
      try {
        await RedisClient.instance.quit();
        logger.info("Redis connection closed");
      } catch (error) {
        logger.error("Error closing Redis connection: ", error);
      }
    }
  }

  // Cheap synchronous health check based on the last connection event seen
  // (not currently used anywhere, but available for a future /health check)
  static isReady(): boolean {
    return RedisClient.isConnected;
  }

  // Active health check — actually round-trips a PING command to Redis
  // rather than relying on cached connection state (also currently unused)
  static async testConnection(): Promise<boolean> {
    try {
      await RedisClient.instance.ping();
      return true;
    } catch (error) {
      logger.error("Redis connection test failed: ", error);
      return false;
    }
  }
}

// Export both the singleton instance and the class
export const redis = RedisClient.getInstance();
export { RedisClient };
