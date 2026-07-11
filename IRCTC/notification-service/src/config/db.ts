import mongoose from "mongoose";
import logger from "./logger";
import emailConsumer from "../kafka/email-consumer";
/**
 * Entry point for the Notification Service.
 * Validates required env vars, then starts the Kafka email consumer.
 */
export async function startNotificationService(): Promise<void> {
  try {
    logger.info("Starting Notification Service...");

    const requiredEnvVars = ["RESEND_API_KEY", "MAIL_SEND", "KAFKA_BROKER"];
    const missing = requiredEnvVars.filter((varName) => !process.env[varName]);

    if (missing.length > 0) {
      throw new Error(
        `Missing required environment variables: ${missing.join(", ")}`,
      );
    }

    await emailConsumer.start();

    logger.info("✅ Notification Service started successfully");
    logger.info("Service is ready to process notifications");
  } catch (error) {
    console.log(error);

    const err = error as Error;
    logger.error("Failed to start Notification Service", {
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  }
}
// Catch promise rejections that weren't handled anywhere else in the app
process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection", { reason, promise });
});

// Catch synchronous errors that escaped all try/catch blocks — exit immediately
// since the process state is no longer guaranteed to be safe to continue running
process.on("uncaughtException", (error: Error) => {
  logger.error("Uncaught Exception", {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});
