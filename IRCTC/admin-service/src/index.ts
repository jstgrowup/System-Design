import dotenv from "dotenv";
dotenv.config();

import app from "./server";
import { config } from "./config";
import logger from "./config/logger";
import { disconnectProducer } from "./config/kafka";

const startServer = async (): Promise<void> => {
  try {
    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    const shutdown = async (): Promise<void> => {
      logger.info("Shutting down gracefully...");

      server.close(async () => {
        await disconnectProducer();
        logger.info("Server closed");
        process.exit(0);
      });
    };

    process.on("SIGTERM", () => void shutdown());
    process.on("SIGINT", () => void shutdown());
  } catch (error) {
    logger.error("Failed to start server", { error: (error as Error).message });
    process.exit(1);
  }
};

void startServer();
