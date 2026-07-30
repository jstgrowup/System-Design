import prisma from "../config/prisma";
import logger from "../config/logger";
import { config } from "../config";
import { inventoryService } from "../services/inventory.service";
import type { TransactionClient } from "../types";

let intervalHandle: ReturnType<typeof setInterval> | null = null;

// PostgreSQL advisory lock ID for leader election. Only one running instance
// of this service holds this lock at a time (try-lock, non-blocking) — so
// scaling to multiple replicas doesn't run the expiry sweep redundantly.
const ADVISORY_LOCK_ID = 800001;

interface AdvisoryLockRow {
  acquired: boolean;
}

/**
 * Try to become the leader for this expiry cycle using pg_try_advisory_lock.
 * Returns true if this instance acquired the lock. The lock is session-level
 * and released explicitly after the job finishes.
 */
async function tryAcquireLeadership(): Promise<boolean> {
  try {
    const result = await prisma.$queryRaw<
      AdvisoryLockRow[]
    >`SELECT pg_try_advisory_lock(${ADVISORY_LOCK_ID}) AS acquired`;
    return result[0]?.acquired === true;
  } catch (err) {
    logger.error("Failed to acquire lock expiry leadership", {
      error: (err as Error).message,
    });
    return false;
  }
}

async function releaseLeadership(): Promise<void> {
  try {
    await prisma.$queryRaw`SELECT pg_advisory_unlock(${ADVISORY_LOCK_ID})`;
  } catch (err) {
    logger.error("Failed to release lock expiry leadership", {
      error: (err as Error).message,
    });
  }
}

interface ExpiredSegmentLockRow {
  id: string;
  scheduleId: string;
  seatId: string;
}

interface ExpiredSeatRow {
  id: string;
  scheduleId: string;
  seatNumber: number;
}

async function cleanExpiredLocks(): Promise<void> {
  const isLeader = await tryAcquireLeadership();
  if (!isLeader) {
    logger.debug("Skipping lock expiry job — another instance is the leader");
    return;
  }

  try {
    // Clean expired segment locks first.
    try {
      const expiredSegmentLocks = await prisma.seatSegmentLock.findMany({
        where: { status: "LOCKED", lockExpiresAt: { lt: new Date() } },
        select: { id: true, scheduleId: true, seatId: true },
      });

      if (expiredSegmentLocks.length > 0) {
        logger.info(
          `Found ${expiredSegmentLocks.length} expired segment lock(s) to clean up`,
        );

        const segmentIds = expiredSegmentLocks.map(
          (l: ExpiredSegmentLockRow) => l.id,
        );
        await prisma.$executeRaw`
          DELETE FROM seat_segment_locks WHERE id = ANY(${segmentIds}::text[])
        `;

        // Group by scheduleId -> Set<seatId> for recomputing SeatInventory
        const affectedScheduleSeats = new Map<string, Set<string>>();
        for (const lock of expiredSegmentLocks) {
          if (!affectedScheduleSeats.has(lock.scheduleId)) {
            affectedScheduleSeats.set(lock.scheduleId, new Set());
          }
          affectedScheduleSeats.get(lock.scheduleId)!.add(lock.seatId);
        }

        for (const [scheduleId, seatIdSet] of affectedScheduleSeats) {
          // Recompute each seat's summary status from remaining segment
          // locks — correctly handles every transition (LOCKED->AVAILABLE,
          // LOCKED->BOOKED, etc.) instead of assuming expiry means AVAILABLE.
          await prisma.$transaction(async (tx: TransactionClient) => {
            await inventoryService.recomputeSegmentSeatStatuses(
              tx,
              scheduleId,
              [...seatIdSet],
            );
          });
          await inventoryService.recountAndPublish(scheduleId);
        }

        logger.info(
          `Cleaned ${expiredSegmentLocks.length} expired segment lock(s)`,
        );
      }
    } catch (segErr) {
      logger.error("Segment lock expiry cleanup failed", {
        error: (segErr as Error).message,
      });
    }

    // Find all expired locked seats (original full-journey lock expiry)
    const expiredSeats = await prisma.seatInventory.findMany({
      where: { status: "LOCKED", lockExpiresAt: { lt: new Date() } },
      select: { id: true, scheduleId: true, seatNumber: true },
    });

    if (expiredSeats.length === 0) return;

    logger.info(`Found ${expiredSeats.length} expired seat lock(s) to clean up`);

    const bySchedule = new Map<string, ExpiredSeatRow[]>();
    for (const seat of expiredSeats) {
      if (!bySchedule.has(seat.scheduleId)) bySchedule.set(seat.scheduleId, []);
      bySchedule.get(seat.scheduleId)!.push(seat);
    }

    for (const [scheduleId, seats] of bySchedule) {
      try {
        const seatPkIds = seats.map((s) => s.id);

        await prisma.$executeRaw`
          UPDATE seat_inventories
          SET status = 'AVAILABLE', "lockedBy" = NULL,
              "lockedAt" = NULL, "lockExpiresAt" = NULL,
              version = version + 1, "updatedAt" = NOW()
          WHERE id = ANY(${seatPkIds}::text[])
          AND status = 'LOCKED'
        `;

        // Recount from actual seat rows to prevent counter drift
        await inventoryService.recountAndPublish(scheduleId);

        logger.info(
          `Released ${seats.length} expired lock(s) for schedule ${scheduleId}`,
        );
      } catch (err) {
        logger.error(`Failed to clean expired locks for schedule ${scheduleId}`, {
          error: (err as Error).message,
        });
      }
    }
  } catch (error) {
    logger.error("Lock expiry cleanup failed", {
      error: (error as Error).message,
    });
  } finally {
    await releaseLeadership();
  }
}

export function startLockExpiryJob(): void {
  // Run once immediately, then on the configured interval.
  void cleanExpiredLocks();

  intervalHandle = setInterval(
    () => void cleanExpiredLocks(),
    config.LOCK_EXPIRY_INTERVAL_MS,
  );
  logger.info(
    `Lock expiry job started (interval: ${config.LOCK_EXPIRY_INTERVAL_MS}ms)`,
  );
}

export function stopLockExpiryJob(): void {
  if (intervalHandle) {
    clearInterval(intervalHandle);
    intervalHandle = null;
    logger.info("Lock expiry job stopped");
  }
}
