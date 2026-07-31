import { readFileSync } from "fs";
import { resolve } from "path";

// require("../../package.json") would type package.json's export as `any`;
// reading + parsing it manually keeps the `any` confined to this one
// external-boundary assertion instead of leaking into Config.SERVICE_NAME.
const packageJsonPath = resolve(process.cwd(), "./package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8")) as {
  name: string;
};

interface Config {
  SERVICE_NAME: string;
  PORT: number;
  NODE_ENV: string;
  LOG_LEVEL: string;
  DATABASE_URL: string | undefined;
  ALLOWED_ORIGINS: string | undefined;
  KAFKA_BROKER: string | undefined;
  KAFKA_CLIENT_ID: string | undefined;
  REDIS_URL: string;

  // Inter-service communication
  INVENTORY_SERVICE_URL: string;
  PAYMENT_SERVICE_URL: string;
  USER_SERVICE_URL: string;
  ADMIN_SERVICE_URL: string;
  INTERNAL_SERVICE_KEY: string | undefined;

  // Booking TTL
  BOOKING_TTL_SECONDS: number;
  LOCK_TTL_SECONDS: number;
  BOOKING_EXPIRY_CHECK_INTERVAL_MS: number;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4005,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  DATABASE_URL: process.env.DATABASE_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  REDIS_URL: process.env.REDIS_URL as string,

  INVENTORY_SERVICE_URL:
    process.env.INVENTORY_SERVICE_URL || "http://localhost:4007",
  PAYMENT_SERVICE_URL:
    process.env.PAYMENT_SERVICE_URL || "http://localhost:4006",
  USER_SERVICE_URL: process.env.USER_SERVICE_URL || "http://localhost:4001",
  ADMIN_SERVICE_URL: process.env.ADMIN_SERVICE_URL || "http://localhost:4003",
  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,

  BOOKING_TTL_SECONDS: Number(process.env.BOOKING_TTL_SECONDS) || 600,
  LOCK_TTL_SECONDS: Number(process.env.LOCK_TTL_SECONDS) || 600,
  BOOKING_EXPIRY_CHECK_INTERVAL_MS:
    Number(process.env.BOOKING_EXPIRY_CHECK_INTERVAL_MS) || 30000,
};
