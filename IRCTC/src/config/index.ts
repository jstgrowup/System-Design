import dotenv from "dotenv";
import { readFileSync } from "fs";
import { resolve } from "path";

dotenv.config();

// Safely parse package.json for the service name in ES Modules
const packageJsonPath = resolve(process.cwd(), "./package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));

interface Config {
  SERVICE_NAME: string;
  PORT: number;
  NODE_ENV: string;
  LOG_LEVEL: string;
  REDIS_URL: string;
  ALLOWED_ORIGINS: string;
  DATABASE_URL: string;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4001,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  REDIS_URL: process.env.REDIS_URL || "redis://irctcpass@redis:6379",
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS || "http://localhost:4000",
  DATABASE_URL: process.env.DATABASE_URL || "postgresql://postgres:password@localhost:5432/irctc",
};
