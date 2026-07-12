import "dotenv/config";
import express, { Request, Response, NextFunction } from "express";
import helmet from "helmet";
import cookieParser from "cookie-parser";
import morgan from "morgan";
import { config } from "./config";
import logger from "./config/logger";
import { notFound } from "./middlewares/not-found.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import { corsMiddleware } from "./middlewares/cors.middleware";
import { gatewayRouter } from "./routes/index";
import { errorMiddleware } from "./middlewares/error.middleware";

const app = express();

app.use(corsMiddleware);

app.use(
  helmet({
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);

app.use(reqLogger);

// Skip JSON parsing for Razorpay webhook — signature verification needs raw bytes
app.use((req: Request, res: Response, next: NextFunction) => {
  if (req.path === "/api/payments/webhooks/razorpay") {
    return express.raw({ type: "application/json", limit: "10mb" })(
      req,
      res,
      next,
    );
  }
  express.json({ limit: "10mb" })(req, res, next);
});

app.use(express.urlencoded({ extended: true, limit: "10mb" }));
app.use(cookieParser());

if (config.NODE_ENV === "development") {
  app.use(morgan("dev"));
}

app.get("/health", (req: Request, res: Response) => {
  res.status(200).json({
    success: true,
    message: "API Gateway is running",
    timestamp: new Date().toISOString(),
    environment: config.NODE_ENV,
  });
});

app.use("/api", gatewayRouter);
app.use(notFound);
app.use(errorMiddleware);

const gracefulShutdown = (): void => {
  logger.info("Received shutdown signal, closing server gracefully...");

  server.close(() => {
    logger.info("Server closed");
    process.exit(0);
  });

  setTimeout(() => {
    logger.error("Forced shutdown after timeout");
    process.exit(1);
  }, 30000);
};

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

const server = app.listen(config.PORT, () => {
  logger.info(
    `🚀 API Gateway running on port ${config.PORT} in ${config.NODE_ENV} mode`,
  );
});

process.on("unhandledRejection", (err: Error) => {
  logger.error("Unhandled Rejection:", err);
  server.close(() => process.exit(1));
});

export default app;
