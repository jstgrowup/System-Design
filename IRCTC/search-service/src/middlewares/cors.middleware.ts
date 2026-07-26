// ============================================
// CORS (Cross-Origin Resource Sharing) Middleware
// ============================================
// Handles requests from different origins (domains) and allows/blocks them
// based on configuration. Prevents unauthorized cross-domain requests.

import cors, { CorsOptions } from "cors";
import { config } from "../config";

// Parse ALLOWED_ORIGINS from .env (comma-separated)
// Example: "http://localhost:3000,http://localhost:3001"
const allowedOrigins: string[] = config.ALLOWED_ORIGINS
  ? config.ALLOWED_ORIGINS.split(",").map((o) => o.trim())
  : [];

// CORS Configuration options
const corsOptions: CorsOptions = {
  // Check if incoming request origin is in whitelist
  origin: (origin, callback) => {
    // Allow requests without origin (like mobile apps, curl, etc.)
    if (!origin) return callback(null, true);

    // Allow only whitelisted origins (security measure)
    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  },

  // Allow credentials (cookies, auth headers) to be sent
  credentials: true,

  // HTTP methods allowed from browser origins
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],

  // Headers that browser can send
  allowedHeaders: ["Content-Type", "Authorization"],
};

// Export configured CORS middleware
export const corsMiddleware = cors(corsOptions);
