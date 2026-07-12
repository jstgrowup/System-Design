// ============================================
// Configuration Module
// ============================================
// Centralizes all configuration from environment variables.
// Provides type-safe config object throughout the application.

import packageJson from "../../package.json";

// Microservices URLs configuration
interface ServiceUrls {
  USER_SERVICE_URL: string;              // User authentication & profile service
  SEARCH_SERVICE_URL: string;            // Search & discovery service
  ADMIN_SERVICE_URL: string;             // Admin operations service
  NOTIFICATION_SERVICE_URL: string;      // Email & notifications service
  BOOKING_SERVICE_URL: string;           // Booking & reservations service
  PAYMENT_SERVICE_URL: string;           // Payment processing service
  INVENTORY_SERVICE_URL: string;         // Inventory management service
}

// Complete application configuration interface
interface Config {
  PORT: number | string;                 // Server port (default: 4000)
  SERVICE_NAME: string;                  // Service identifier for logging
  LOG_LEVEL: string;                     // Winston logging level
  NODE_ENV: string;                      // Environment type (dev/prod)
  REDIS_URL: string | undefined;         // Redis connection string
  ALLOWED_ORIGINS: string;               // CORS whitelist (comma-separated)
  JWT_ACCESS_SECRET: string;             // Secret for signing access tokens (REQUIRED)
  JWT_REFRESH_SECRET: string;            // Secret for signing refresh tokens (REQUIRED)
  ACCESS_TOKEN_EXP: string | undefined;  // Access token expiry (string format)
  REFRESH_TOKEN_EXP: string | undefined; // Refresh token expiry (string format)
  ACCESS_TOKEN_EXP_SEC: number;          // Access token TTL in seconds (default: 900 = 15min)
  REFRESH_TOKEN_EXP_SEC: number;         // Refresh token TTL in seconds (default: 604800 = 7days)
  RATE_LIMIT_WINDOW_MS: number;          // Rate limit window duration in milliseconds
  RATE_LIMIT_MAX_REQUESTS: number;       // Max requests per window per IP/user
  SERVICES: ServiceUrls;                 // Downstream microservices URLs
  SERVICE_TIMEOUT_MS: number;            // Timeout for service calls (default: 60000 = 60sec)
  CIRCUIT_BREAKER_THRESHOLD: number;     // Failures before opening circuit (default: 5)
  CIRCUIT_BREAKER_TIMEOUT: number;       // Wait time before retrying (default: 60000ms)
}

// ============================================
// Initialize Configuration Object
// ============================================
// Load values from environment variables with sensible defaults
const config: Config = {
  // Server configuration
  PORT: process.env.PORT || 4000,
  LOG_LEVEL: "4",  // Winston log levels: 0=debug, 1=info, 2=warn, 3=error, 4=silent
  SERVICE_NAME: packageJson.name,
  NODE_ENV: process.env.NODE_ENV || "development",

  // Cache & storage
  REDIS_URL: process.env.REDIS_URL,

  // CORS security - comma-separated list of allowed origins
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS || "http://localhost:3000",

  // ============================================
  // JWT Secrets (REQUIRED - no defaults!)
  // ============================================
  // These MUST be provided via environment variables
  JWT_ACCESS_SECRET: process.env.JWT_ACCESS_SECRET as string,
  JWT_REFRESH_SECRET: process.env.JWT_REFRESH_SECRET as string,
  ACCESS_TOKEN_EXP: process.env.ACCESS_TOKEN_EXP,
  REFRESH_TOKEN_EXP: process.env.REFRESH_TOKEN_EXP,

  // Token expiry times in seconds
  ACCESS_TOKEN_EXP_SEC: parseInt(process.env.ACCESS_TOKEN_EXP_SEC || "900", 10),        // 15 minutes
  REFRESH_TOKEN_EXP_SEC: parseInt(process.env.REFRESH_TOKEN_EXP_SEC || "604800", 10),  // 7 days

  // ============================================
  // Rate Limiting Configuration
  // ============================================
  RATE_LIMIT_WINDOW_MS: parseInt(process.env.RATE_LIMIT_WINDOW_MS || "900000", 10),  // 15 minutes
  RATE_LIMIT_MAX_REQUESTS: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || "100", 10), // per IP/user

  // ============================================
  // Downstream Microservices URLs
  // ============================================
  SERVICES: {
    USER_SERVICE_URL: process.env.USER_SERVICE_URL || "http://localhost:4001",
    SEARCH_SERVICE_URL: process.env.SEARCH_SERVICE_URL || "http://localhost:4002",
    ADMIN_SERVICE_URL: process.env.ADMIN_SERVICE_URL || "http://localhost:4003",
    NOTIFICATION_SERVICE_URL: process.env.NOTIFICATION_SERVICE_URL || "http://localhost:4004",
    BOOKING_SERVICE_URL: process.env.BOOKING_SERVICE_URL || "http://localhost:4005",
    PAYMENT_SERVICE_URL: process.env.PAYMENT_SERVICE_URL || "http://localhost:4006",
    INVENTORY_SERVICE_URL: process.env.INVENTORY_SERVICE_URL || "http://localhost:4007",
  },

  // ============================================
  // Circuit Breaker Configuration
  // ============================================
  SERVICE_TIMEOUT_MS: parseInt(process.env.SERVICE_TIMEOUT_MS || "60000", 10),              // 60 seconds
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),  // Open after 5 failures
  CIRCUIT_BREAKER_TIMEOUT: parseInt(process.env.CIRCUIT_BREAKER_TIMEOUT || "60000", 10),  // Wait 60 seconds
};

// ============================================
// Validate Required Configuration
// ============================================
// These variables MUST be provided, application will fail to start without them
const requiredConfig: (keyof Config)[] = [
  "JWT_ACCESS_SECRET",
  "JWT_REFRESH_SECRET",
];

// Check each required config key
requiredConfig.forEach((key) => {
  if (!config[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
});

export { config };
