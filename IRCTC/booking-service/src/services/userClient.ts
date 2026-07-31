import axios, { AxiosError, AxiosInstance } from "axios";
import { config } from "../config";
import logger from "../config/logger";
import type { InternalUser } from "../types";

const client: AxiosInstance = axios.create({
  baseURL: config.USER_SERVICE_URL,
  timeout: 5000,
  headers: {
    "Content-Type": "application/json",
    "x-internal-service-key": config.INTERNAL_SERVICE_KEY,
  },
});

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = (error as AxiosError).response?.status;
      if (status && status >= 400 && status < 500) throw error;

      if (attempt < maxRetries) {
        const delay = 200 * Math.pow(2, attempt - 1);
        logger.warn(`User client retry ${attempt}/${maxRetries} after ${delay}ms`, {
          error: (error as Error).message,
        });
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

export const userClient = {
  async getUserById(userId: string): Promise<InternalUser> {
    return withRetry(async () => {
      const { data } = await client.get<{ data: InternalUser }>(
        `/user/internal/${userId}`,
      );
      return data.data;
    });
  },
};
