import cors, { CorsOptions } from "cors";
import { RequestHandler } from "express";
import { config } from "../config";

const corsOptions: CorsOptions = {
  // Splits a comma-separated string of origins from your config into an array
  origin: config.ALLOWED_ORIGINS.split(","),

  credentials: true,

  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],

  allowedHeaders: [
    "Origin",
    "X-Requested-With",
    "Content-Type",
    "Accept",
    "Authorization",
  ],
};

export const corsMiddleware: RequestHandler = cors(corsOptions);
