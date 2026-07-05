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
  KAFKA_BROKER: string;
  KAFKA_CLIENT_ID?: string;
  OTP_TTL: number;
  OTP_RATE_MAX_PER_HOUR: number;
  OTP_MAX_VERIFY_ATTEMPTS: number;
  OTP_HMAC_SECRET: string;
  JWT_ACCESS_SECRET: string;
  JWT_REFRESH_SECRET: string;
  ACCESS_TOKEN_EXP: string;
  REFRESH_TOKEN_EXP: string;
  ACCESS_TOKEN_EXP_SEC: number;
  REFRESH_TOKEN_EXP_SEC: number;
  REDIS_USER_TTL: number;
  MAIL_SEND?: string;
  SENDGRID_API_KEY?: string;
  GOOGLE_CLIENT_ID?: string;
  GOOGLE_CLIENT_SECRET?: string;
  INTERNAL_SERVICE_KEY?: string;
  RESEND_API_KEY?: string;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT),
  NODE_ENV: process.env.NODE_ENV!,
  LOG_LEVEL: process.env.LOG_LEVEL!,
  DATABASE_URL: process.env.DATABASE_URL!,
  REDIS_URL: process.env.REDIS_URL!,
  KAFKA_BROKER: process.env.KAFKA_BROKER!,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS!,

  OTP_TTL: Number(process.env.OTP_TTL),
  OTP_RATE_MAX_PER_HOUR: Number(process.env.OTP_RATE_MAX_PER_HOUR),
  OTP_MAX_VERIFY_ATTEMPTS: Number(process.env.OTP_MAX_VERIFY_ATTEMPTS),
  OTP_HMAC_SECRET: process.env.OTP_HMAC_SECRET!,

  JWT_ACCESS_SECRET: process.env.JWT_ACCESS_SECRET!,
  JWT_REFRESH_SECRET: process.env.JWT_REFRESH_SECRET!,
  ACCESS_TOKEN_EXP: process.env.ACCESS_TOKEN_EXP!,
  REFRESH_TOKEN_EXP: process.env.REFRESH_TOKEN_EXP!,
  ACCESS_TOKEN_EXP_SEC: Number(process.env.ACCESS_TOKEN_EXP_SEC),
  REFRESH_TOKEN_EXP_SEC: Number(process.env.REFRESH_TOKEN_EXP_SEC),
  REDIS_USER_TTL: Number(process.env.REDIS_USER_TTL),

  MAIL_SEND: process.env.MAIL_SEND,
  SENDGRID_API_KEY: process.env.SENDGRID_API_KEY,

  GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,

  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,
  RESEND_API_KEY: process.env.RESEND_API_KEY,
};
