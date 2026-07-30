import logger from "../config/logger";

interface RetryableError {
  code?: string;
  message?: string;
}

/**
 * Retries a Prisma transaction on serialization/lock-timeout/deadlock errors —
 * the kind of transient conflict expected when concurrent requests race for
 * the same seats via `FOR UPDATE NOWAIT`. Non-retryable errors (e.g. a thrown
 * AppError like ConflictError for "seat already booked") pass straight
 * through on the first attempt.
 */
export async function retryTransaction<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const err = error as RetryableError;
      const isRetryable =
        err.code === "P2034" ||
        !!err.message?.includes("could not serialize") ||
        !!err.message?.includes("could not obtain lock") ||
        !!err.message?.includes("deadlock detected");

      if (isRetryable && attempt < maxRetries) {
        const delay = 50 * attempt;
        logger.warn(
          `Transaction attempt ${attempt} failed (retryable), retrying in ${delay}ms...`,
        );
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  // Unreachable: the loop above either returns or throws on every iteration,
  // but TypeScript can't prove that without an explicit exhaustive throw.
  throw new Error("retryTransaction: exhausted retries without a result");
}
