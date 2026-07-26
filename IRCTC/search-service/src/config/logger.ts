// ============================================
// Winston Logger Setup
// ============================================
// Single shared logger instance used across the whole gateway
// (index.ts, middlewares, services) for consistent log formatting.

import winston from "winston";
import { config } from ".";

const logger = winston.createLogger({
  // NOTE: config.LOG_LEVEL is hardcoded to "4" in config/index.ts, which is not
  // a valid Winston level string (winston expects "debug" | "info" | "warn" | "error" | "silly").
  // Passing an unrecognized level generally falls back to logging everything.
  level: config.LOG_LEVEL,

  // Attaches the service name to every log line (useful when aggregating logs
  // from multiple services in one place)
  defaultMeta: { service: config.SERVICE_NAME },

  format: winston.format.combine(
    winston.format.timestamp(),
    // Custom line format: [timestamp] [level] [service]: message
    winston.format.printf(({ level, message, timestamp, service }) => {
      return `[${timestamp}] [${level}] [${service}]: ${message}`;
    }),
  ),

  // Logs go to stdout/stderr only — no file transport configured
  transports: [new winston.transports.Console()],
});

export default logger;
