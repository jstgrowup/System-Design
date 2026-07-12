import cors, { CorsOptions } from "cors";
import { config } from "../config";

const allowedOrigins: string[] = config.ALLOWED_ORIGINS
  ? config.ALLOWED_ORIGINS.split(",").map((o) => o.trim())
  : [];

const corsOptions: CorsOptions = {
  origin: (origin, callback) => {
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
};

export const corsMiddleware = cors(corsOptions);
