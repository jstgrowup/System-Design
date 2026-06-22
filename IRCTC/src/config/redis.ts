import { Redis } from "ioredis";
import { config } from "./index.js";
import logger from "./logger.js";

export class RedisClient {
  private static instance: Redis | null = null;
  private static isConnected: boolean = false;

  // Prevent direct initialization via the 'new' keyword from outside the class
  private constructor() {}

  public static getInstance(): Redis {
    if (!RedisClient.instance) {
      RedisClient.instance = new Redis(config.REDIS_URL, {
        retryStrategy: (times: number): number | null => {
          const delay = Math.min(times * 50, 2000);
          return delay;
        },
        maxRetriesPerRequest: 3,
      });

      RedisClient.setupEventListeners();
    }
    return RedisClient.instance;
  }

  private static setupEventListeners(): void {
    if (!RedisClient.instance) return;

    RedisClient.instance.on("connect", (): void => {
      RedisClient.isConnected = true;
      logger.info("Connected to Redis");
    });

    RedisClient.instance.on("error", (error: Error): void => {
      RedisClient.isConnected = false;
      logger.error("Redis connection error", error);
    });

    RedisClient.instance.on("close", (): void => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection closed");
    });

    RedisClient.instance.on("reconnecting", (): void => {
      logger.warn("Reconnecting to Redis...");
    });

    RedisClient.instance.on("ready", (): void => {
      logger.warn("Redis client is ready");
    });

    RedisClient.instance.on("end", (): void => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection ended");
    });
  }

  public static async closeConnection(): Promise<void> {
    if (RedisClient.instance) {
      try {
        await RedisClient.instance.quit();
        logger.info("Redis connection closed explicitly via quit");
      } catch (error) {
        logger.error("Error closing Redis connection: ", error as Error);
      }
    }
  }

  public static isReady(): boolean {
    return RedisClient.isConnected;
  }

  public static async testConnection(): Promise<boolean> {
    if (!RedisClient.instance) {
      return false;
    }
    try {
      await RedisClient.instance.ping();
      return true;
    } catch (error) {
      logger.error("Redis connection test failed: ", error as Error);
      return false;
    }
  }
}

// Named exports replacing old module.exports syntax
export const redis: Redis = RedisClient.getInstance();
