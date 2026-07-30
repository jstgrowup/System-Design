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
  INTERNAL_SERVICE_KEY: string | undefined;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4003,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  DATABASE_URL: process.env.DATABASE_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,
};
