import { Request, Response } from "express";
import { inventoryService } from "../services/inventory.service";
import {
  zLockSeats,
  zUnlockSeats,
  zConfirmSeats,
  zCancelBooking,
  zSeatFilters,
} from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import asyncHandler from "../utils/asyncHandler";

/**
 * GET /schedules/:scheduleId/availability
 *
 * Public (no auth) — used by search-service's results page to show seat
 * counts without exposing per-seat detail.
 */
const getScheduleAvailability = asyncHandler(
  async (req: Request<{ scheduleId: string }>, res: Response) => {
    const { scheduleId } = req.params;

    const data = await inventoryService.getAvailability(scheduleId);

    res.status(200).json({ success: true, data });
  },
);

/**
 * GET /schedules/:scheduleId/seats
 *
 * Reachable by an end user (via the gateway) or by booking-service (via the
 * internal service key) — see routes/inventory.routes.ts's userOrInternal.
 * Optional fromSeq/toSeq query params narrow seat status to a specific
 * journey segment instead of the whole route.
 */
const getScheduleSeats = asyncHandler(async (req: Request<{ scheduleId: string }>, res: Response) => {
  const { scheduleId } = req.params;

  const result = zSeatFilters.safeParse(req.query);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const data = await inventoryService.getSeats(scheduleId, result.data);

  res.status(200).json({ success: true, data });
});

/**
 * POST /seats/lock — internal only, called by booking-service while it
 * holds seats during the create-booking saga.
 */
const lockSeats = asyncHandler(async (req: Request, res: Response) => {
  const result = zLockSeats.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { scheduleId, seatIds, userId, ttlSeconds, fromSeq, toSeq } =
    result.data;

  const lockResult = await inventoryService.lockSeats(
    scheduleId,
    seatIds,
    userId,
    ttlSeconds ?? 0,
    fromSeq,
    toSeq,
  );

  res.status(200).json({
    success: true,
    message: `${lockResult.lockedSeats.length} seat(s) locked successfully`,
    data: {
      scheduleId: lockResult.scheduleId,
      lockedSeats: lockResult.lockedSeats,
      lockExpiresAt: lockResult.lockExpiresAt,
    },
  });
});

/** POST /seats/unlock — internal only, releases seats the saga is compensating. */
const unlockSeats = asyncHandler(async (req: Request, res: Response) => {
  const result = zUnlockSeats.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { scheduleId, seatIds, userId, fromSeq, toSeq } = result.data;

  const unlockResult = await inventoryService.unlockSeats(
    scheduleId,
    seatIds,
    userId,
    fromSeq,
    toSeq,
  );

  res.status(200).json({
    success: true,
    message: `${unlockResult.unlockedSeats.length} seat(s) unlocked successfully`,
    data: {
      scheduleId: unlockResult.scheduleId,
      unlockedSeats: unlockResult.unlockedSeats,
    },
  });
});

/** POST /seats/confirm — internal only, called once payment succeeds. */
const confirmSeats = asyncHandler(async (req: Request, res: Response) => {
  const result = zConfirmSeats.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { scheduleId, seatIds, bookingId, userId, fromSeq, toSeq } =
    result.data;

  const confirmResult = await inventoryService.confirmSeats(
    scheduleId,
    seatIds,
    userId,
    bookingId,
    fromSeq,
    toSeq,
  );

  res.status(200).json({
    success: true,
    message: `${confirmResult.confirmedSeats.length} seat(s) confirmed`,
    data: {
      scheduleId: confirmResult.scheduleId,
      bookingId: confirmResult.bookingId,
      confirmedSeats: confirmResult.confirmedSeats,
    },
  });
});

/** POST /seats/cancel-booking — internal only, releases a confirmed booking's seats. */
const cancelBooking = asyncHandler(async (req: Request, res: Response) => {
  const result = zCancelBooking.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { scheduleId, bookingId } = result.data;

  const cancelResult = await inventoryService.cancelBooking(
    scheduleId,
    bookingId,
    result.data.userId,
  );

  res.status(200).json({
    success: true,
    message: `Booking cancelled, ${cancelResult.releasedSeats.length} seat(s) released`,
    data: {
      scheduleId: cancelResult.scheduleId,
      bookingId: cancelResult.bookingId,
      releasedSeats: cancelResult.releasedSeats,
    },
  });
});

export const inventoryController = {
  getScheduleAvailability,
  getScheduleSeats,
  lockSeats,
  unlockSeats,
  confirmSeats,
  cancelBooking,
};
