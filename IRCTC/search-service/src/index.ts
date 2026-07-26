import "dotenv/config";
import path from "path";
import express, { Request, Response } from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { config } from "./config";
import logger from "./config/logger";
import { initIndices, recreateIndices } from "./config/elasticsearch";

import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";

import searchRoutes from "./routes/search.route";
import searchConsumer from "./kafka/search.service";
import { disconnectAll } from "./config/kafka";

const app = express();

app.use(corsMiddleware);
app.use(
  helmet({
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:"],
        connectSrc: ["'self'"],
      },
    },
  }),
);
app.use(reqLogger);
app.use(express.json());
app.use(cookieParser());

// Serve frontend static files
app.use(express.static(path.join(__dirname, "..", "public")));

// Mount search routes at root (gateway strips first path segment)
app.use(searchRoutes);

app.get("/health", (req: Request, res: Response) =>
  res.json({ status: "ok", service: config.SERVICE_NAME }),
);
app.use(errorHandler);

const startServer = async (): Promise<void> => {
  if (process.env.ES_RECREATE_INDICES === "true") {
    await recreateIndices();
  } else {
    await initIndices();
  }
  await searchConsumer.start();

  const server = app.listen(config.PORT, () => {
    logger.info(
      `${config.SERVICE_NAME} running on http://localhost:${config.PORT}`,
    );
  });

  const shutdown = async (): Promise<void> => {
    logger.info("Shutting down...");
    server.close(async () => {
      await disconnectAll();
      process.exit(0);
    });
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
};

startServer();
