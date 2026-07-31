import axios, { AxiosError, AxiosInstance } from "axios";
import { config } from "../config";
import logger from "../config/logger";
import type { InternalStation } from "../types";

const client: AxiosInstance = axios.create({
  baseURL: config.ADMIN_SERVICE_URL,
  timeout: 5000,
  headers: {
    "Content-Type": "application/json",
    "x-internal-service-key": config.INTERNAL_SERVICE_KEY,
  },
});

const STATION_CACHE_TTL_MS = 10 * 60 * 1000;

interface CacheEntry {
  value: InternalStation;
  expiresAt: number;
}

const stationCache = new Map<string, CacheEntry>();

function cacheGet(stationId: string): InternalStation | null {
  const entry = stationCache.get(stationId);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    stationCache.delete(stationId);
    return null;
  }
  return entry.value;
}

function cacheSet(stationId: string, value: InternalStation): void {
  stationCache.set(stationId, {
    value,
    expiresAt: Date.now() + STATION_CACHE_TTL_MS,
  });
}

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
        logger.warn(`Station client retry ${attempt}/${maxRetries} after ${delay}ms`, {
          error: (error as Error).message,
        });
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

export const stationClient = {
  async getStationById(stationId: string | null): Promise<InternalStation | null> {
    if (!stationId) return null;

    const cached = cacheGet(stationId);
    if (cached) return cached;

    const station = await withRetry(async () => {
      // admin-service mounts stationRoutes at /stations, and the route
      // itself is /station/internal/:stationId — so the full path is
      // /stations/station/internal/:stationId.
      const { data } = await client.get<{ data: InternalStation }>(
        `/stations/station/internal/${stationId}`,
      );
      return data.data;
    });

    if (station) cacheSet(stationId, station);
    return station;
  },
};
