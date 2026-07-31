import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import bookingRoutes from "./routes/booking.routes";
import prisma from "./config/prisma";
import logger from "./config/logger";
import { RedisClient } from "./config/redis";

const app = express();

app.use(helmet({
  crossOriginOpenerPolicy: false,
  crossOriginEmbedderPolicy: false,
}));
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello from booking-service");
});

// Health check — reports both Postgres and Redis reachability, since a
// booking can't be created without either (Redis holds the distributed
// seat locks; Postgres holds the saga/booking state).
app.get("/health", async (req, res) => {
  let dbHealthy = false;
  try {
    await prisma.$queryRaw`SELECT 1`;
    dbHealthy = true;
  } catch (err) {
    logger.error("Health check: DB unreachable", {
      error: (err as Error).message,
    });
  }

  const redisHealthy = RedisClient.isReady();
  const healthy = dbHealthy && redisHealthy;

  res.status(healthy ? 200 : 503).json({
    success: healthy,
    message: healthy ? "Booking Service is healthy" : "Booking Service is degraded",
    redis: redisHealthy,
    database: dbHealthy,
    timestamp: new Date().toISOString(),
  });
});

app.use(bookingRoutes);

// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
