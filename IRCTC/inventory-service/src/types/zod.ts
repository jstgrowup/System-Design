import { z } from "zod";

const zSeatIds = z
  .array(z.string({ error: "Each seatId must be a string" }))
  .min(1, "seatIds must be a non-empty array");

export const zLockSeats = z.object({
  scheduleId: z.string({ error: "scheduleId is required" }),
  seatIds: zSeatIds,
  userId: z.string({ error: "userId is required" }),
  ttlSeconds: z.number().positive().optional(),
  fromSeq: z.number().int().positive().optional(),
  toSeq: z.number().int().positive().optional(),
});
export type LockSeatsBodyType = z.infer<typeof zLockSeats>;

export const zUnlockSeats = z.object({
  scheduleId: z.string({ error: "scheduleId is required" }),
  seatIds: zSeatIds,
  userId: z.string({ error: "userId is required" }),
  fromSeq: z.number().int().positive().optional(),
  toSeq: z.number().int().positive().optional(),
});
export type UnlockSeatsBodyType = z.infer<typeof zUnlockSeats>;

export const zConfirmSeats = z.object({
  scheduleId: z.string({ error: "scheduleId is required" }),
  seatIds: zSeatIds,
  bookingId: z.string({ error: "bookingId is required" }),
  userId: z.string({ error: "userId is required" }),
  fromSeq: z.number().int().positive().optional(),
  toSeq: z.number().int().positive().optional(),
});
export type ConfirmSeatsBodyType = z.infer<typeof zConfirmSeats>;

export const zCancelBooking = z.object({
  scheduleId: z.string({ error: "scheduleId is required" }),
  bookingId: z.string({ error: "bookingId is required" }),
  userId: z.string({ error: "userId is required" }),
});
export type CancelBookingBodyType = z.infer<typeof zCancelBooking>;

export const zSeatFilters = z.object({
  status: z.enum(["AVAILABLE", "LOCKED", "BOOKED", "CANCELLED"]).optional(),
  seatType: z.string().optional(),
  fromSeq: z.coerce.number().int().positive().optional(),
  toSeq: z.coerce.number().int().positive().optional(),
});
export type SeatFiltersQueryType = z.infer<typeof zSeatFilters>;
