import axios, { AxiosError, AxiosInstance } from "axios";
import { config } from "../config";
import logger from "../config/logger";
import type {
  InventoryAvailability,
  InventorySeatFilters,
  InventorySeatsResult,
  InventoryLockResult,
  InventoryUnlockResult,
  InventoryConfirmResult,
  InventoryCancelResult,
} from "../types";

const client: AxiosInstance = axios.create({
  baseURL: config.INVENTORY_SERVICE_URL,
  timeout: 10000,
  headers: {
    "Content-Type": "application/json",
    "x-internal-service-key": config.INTERNAL_SERVICE_KEY,
  },
});

interface ClientErrorShape {
  status: number;
  message: string;
  code?: string;
}

/**
 * Retry wrapper with exponential backoff. Only retries server/network
 * errors — a 4xx from the downstream service means the request itself was
 * wrong, so retrying it would just fail the same way again.
 */
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
        logger.warn(`Inventory client retry ${attempt}/${maxRetries} after ${delay}ms`, {
          error: (error as Error).message,
        });
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

/** Extracts a normalized error shape from an axios error for callers to inspect. */
export function extractError(error: unknown): ClientErrorShape {
  const axiosError = error as AxiosError<{ message?: string; error?: string }>;
  if (axiosError.response?.data) {
    return {
      status: axiosError.response.status,
      message: axiosError.response.data.message || axiosError.message,
      code: axiosError.response.data.error,
    };
  }
  return {
    status: 500,
    message: (error as Error).message,
    code: "INVENTORY_SERVICE_ERROR",
  };
}

export const inventoryClient = {
  async getAvailability(scheduleId: string): Promise<InventoryAvailability> {
    return withRetry(async () => {
      const { data } = await client.get<{ data: InventoryAvailability }>(
        `/schedules/${scheduleId}/availability`,
      );
      return data.data;
    });
  },

  async getSeats(
    scheduleId: string,
    filters: InventorySeatFilters = {},
  ): Promise<InventorySeatsResult> {
    return withRetry(async () => {
      const params: Record<string, string | number> = {};
      if (filters.status) params.status = filters.status;
      if (filters.seatType) params.seatType = filters.seatType;
      if (filters.fromSeq) params.fromSeq = filters.fromSeq;
      if (filters.toSeq) params.toSeq = filters.toSeq;

      const { data } = await client.get<{ data: InventorySeatsResult }>(
        `/schedules/${scheduleId}/seats`,
        { params },
      );
      return data.data;
    });
  },

  async holdSeats(
    scheduleId: string,
    seatIds: string[],
    userId: string,
    ttlSeconds: number,
    fromSeq?: number,
    toSeq?: number,
  ): Promise<InventoryLockResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: InventoryLockResult }>(
        "/seats/lock",
        { scheduleId, seatIds, userId, ttlSeconds, fromSeq, toSeq },
      );
      return data.data;
    });
  },

  async releaseSeats(
    scheduleId: string,
    seatIds: string[],
    userId: string,
    fromSeq?: number | null,
    toSeq?: number | null,
  ): Promise<InventoryUnlockResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: InventoryUnlockResult }>(
        "/seats/unlock",
        { scheduleId, seatIds, userId, fromSeq, toSeq },
      );
      return data.data;
    });
  },

  async confirmSeats(
    scheduleId: string,
    seatIds: string[],
    userId: string,
    bookingId: string,
    fromSeq?: number | null,
    toSeq?: number | null,
  ): Promise<InventoryConfirmResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: InventoryConfirmResult }>(
        "/seats/confirm",
        { scheduleId, seatIds, userId, bookingId, fromSeq, toSeq },
      );
      return data.data;
    });
  },

  async cancelBooking(
    scheduleId: string,
    bookingId: string,
    userId: string,
  ): Promise<InventoryCancelResult> {
    return withRetry(async () => {
      const { data } = await client.post<{ data: InventoryCancelResult }>(
        "/seats/cancel-booking",
        { scheduleId, bookingId, userId },
      );
      return data.data;
    });
  },
};
