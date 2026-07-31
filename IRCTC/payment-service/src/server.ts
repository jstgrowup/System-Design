import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import paymentRoutes from "./routes/payment.routes";
import webhookRoutes from "./routes/webhook.routes";
import prisma from "./config/prisma";
import logger from "./config/logger";

const app = express();

app.use(corsMiddleware);
app.use(helmet({
  crossOriginOpenerPolicy: false,
  crossOriginEmbedderPolicy: false,
}));
app.use(reqLogger);

// Webhook routes MUST be registered before express.json() — they need the
// raw request body for Razorpay's signature verification, and express.json()
// would otherwise consume and parse the stream first.
app.use(webhookRoutes);

// JSON parsing for every other route
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

app.get("/", (req, res) => {
  res.send("Hello from payment-service");
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
    message: dbHealthy ? "Payment Service is healthy" : "Payment Service is degraded",
    database: dbHealthy,
    timestamp: new Date().toISOString(),
  });
});

app.use(paymentRoutes);

// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
