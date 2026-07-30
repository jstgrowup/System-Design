import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import inventoryRoutes from "./routes/inventory.routes";
import prisma from "./config/prisma";
import logger from "./config/logger";

const app = express();

// Order matters: security headers and CORS first, then request logging,
// then body/cookie parsing, before any route handlers run.
app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello from inventory-service");
});

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

  res.status(dbHealthy ? 200 : 503).json({
    success: dbHealthy,
    message: dbHealthy
      ? "Inventory Service is healthy"
      : "Inventory Service is degraded",
    database: dbHealthy,
    timestamp: new Date().toISOString(),
  });
});

app.use(inventoryRoutes);

// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
