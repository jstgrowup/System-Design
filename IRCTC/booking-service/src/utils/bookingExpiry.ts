import prisma from "../config/prisma";
import logger from "../config/logger";
import { config } from "../config";
import { redis } from "../config/redis";
import { forceReleaseSeatLocks } from "./distributedLock";
import { compensateAll } from "../services/saga.service";
import { userClient } from "../services/userClient";
import bookingProducer from "../kafka/producer/booking.producer";
import type { UserNotificationInfo } from "../types";

const fetchUserForNotification = async (
  userId: string,
): Promise<UserNotificationInfo> => {
  try {
    const user = await userClient.getUserById(userId);
    return user ? { email: user.email, firstName: user.firstName } : {};
  } catch (err) {
    logger.warn("Failed to enrich expiry event with user details", {
      userId,
      error: (err as Error).message,
    });
    return {};
  }
};

let expiryInterval: ReturnType<typeof setInterval> | null = null;

// Redis-based leader election key. Only one instance holds this lock at a time.
const LEADER_KEY = "booking:expiry-job:leader";
const LEADER_TTL_SECONDS = 25; // shorter than interval so it re-acquires each cycle

/**
 * Try to become the leader for this expiry cycle using Redis SET NX EX.
 * Returns true if this instance acquired the lock.
 */
async function tryAcquireLeadership(): Promise<boolean> {
  try {
    const result = await redis.set(
      LEADER_KEY,
      process.pid.toString(),
      "EX",
      LEADER_TTL_SECONDS,
      "NX",
    );
    return result === "OK";
  } catch (err) {
    // If Redis is down, skip this cycle rather than having all instances run
    logger.error("Failed to acquire expiry job leadership", {
      error: (err as Error).message,
    });
    return false;
  }
}

/**
 * Background job that expires stale bookings.
 * Runs every BOOKING_EXPIRY_CHECK_INTERVAL_MS (default 30s).
 *
 * Uses Redis leader election so only ONE instance across all replicas
 * runs the cleanup each cycle, avoiding redundant DB scans.
 *
 * Finds bookings in PENDING / SEATS_HELD / PAYMENT_PENDING
 * where lockExpiresAt < now, compensates all saga steps,
 * and marks them as EXPIRED.
 */
async function cleanExpiredBookings(): Promise<void> {
  // Leader election: only one instance runs per cycle
  const isLeader = await tryAcquireLeadership();
  if (!isLeader) {
    logger.debug("Skipping expiry job — another instance is the leader");
    return;
  }

  try {
    const expiredBookings = await prisma.booking.findMany({
      where: {
        status: { in: ["PENDING", "SEATS_HELD", "PAYMENT_PENDING"] },
        lockExpiresAt: { lt: new Date() },
      },
      include: { seats: true },
    });

    if (expiredBookings.length === 0) return;

    logger.info(`Found ${expiredBookings.length} expired booking(s) to clean up`);

    for (const booking of expiredBookings) {
      try {
        const seatIds = booking.seats.map((s) => s.seatId).sort();

        // Atomically claim this booking via optimistic lock (CAS).
        // If payment webhook or user cancel already changed the version, skip it.
        const claimed = await prisma.booking.updateMany({
          where: {
            id: booking.id,
            version: booking.version,
            status: { in: ["PENDING", "SEATS_HELD", "PAYMENT_PENDING"] },
          },
          data: {
            status: "EXPIRED",
            failureReason: "booking_timeout",
            version: { increment: 1 },
          },
        });

        if (claimed.count === 0) {
          logger.info(
            `Booking ${booking.id} already handled by another process, skipping expiry`,
          );
          continue;
        }

        // Compensate all completed saga steps
        await compensateAll(booking, seatIds);

        // Release Redis locks (segment-aware)
        await forceReleaseSeatLocks(
          booking.scheduleId,
          seatIds,
          booking.fromSeq,
          booking.toSeq,
        );

        // Publish BOOKING_FAILED
        try {
          const userInfo = await fetchUserForNotification(booking.userId);
          await bookingProducer.publishBookingFailed({
            bookingId: booking.id,
            userId: booking.userId,
            email: userInfo.email,
            firstName: userInfo.firstName,
            scheduleId: booking.scheduleId,
            reason: "booking_timeout",
          });
        } catch (err) {
          logger.error("Failed to publish BOOKING_FAILED for expired booking", {
            bookingId: booking.id,
            error: (err as Error).message,
          });
        }

        logger.info(`Expired booking ${booking.id} cleaned up`, {
          previousStatus: booking.status,
        });
      } catch (error) {
        logger.error(`Failed to clean up expired booking ${booking.id}`, {
          error: (error as Error).message,
        });
      }
    }
  } catch (error) {
    logger.error("Error in booking expiry job", {
      error: (error as Error).message,
    });
  }
}

export function startBookingExpiryJob(): void {
  // Run immediately once
  void cleanExpiredBookings();

  // Then run on interval
  expiryInterval = setInterval(
    () => void cleanExpiredBookings(),
    config.BOOKING_EXPIRY_CHECK_INTERVAL_MS,
  );
  logger.info(
    `Booking expiry job started (interval: ${config.BOOKING_EXPIRY_CHECK_INTERVAL_MS}ms)`,
  );
}

export function stopBookingExpiryJob(): void {
  if (expiryInterval) {
    clearInterval(expiryInterval);
    expiryInterval = null;
    logger.info("Booking expiry job stopped");
  }
}
