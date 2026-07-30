import dotenv from "dotenv";
dotenv.config();

import app from "./server";
import { config } from "./config";
import logger from "./config/logger";
import { disconnectAll } from "./config/kafka";
import inventoryConsumer from "./kafka/consumer/inventory.consumer";
import { startLockExpiryJob, stopLockExpiryJob } from "./utils/lockExpiry";

const startServer = async (): Promise<void> => {
  try {
    await inventoryConsumer.start();
    startLockExpiryJob();

    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    const shutdown = async (): Promise<void> => {
      logger.info("Shutting down gracefully...");
      stopLockExpiryJob();

      server.close(async () => {
        await disconnectAll();
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
