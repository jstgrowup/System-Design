import type { Prisma } from "../generated/prisma/client";

/** The transaction client passed into every `prisma.$transaction(async (tx) => ...)` callback. */
export type TransactionClient = Prisma.TransactionClient;

export type SeatStatus = "AVAILABLE" | "LOCKED" | "BOOKED" | "CANCELLED";

/** A single seat as denormalized into admin-service's SCHEDULE_CREATED event. */
export interface ScheduleCreatedSeatData {
  seatId: string;
  seatNumber: number;
  seatType: string;
  price: number;
}

/** A single route stop as denormalized into admin-service's SCHEDULE_CREATED event. */
export interface ScheduleCreatedRouteStopData {
  stationId: string;
  stationName: string;
  stationCode: string;
  sequenceNumber: number;
}

/**
 * Payload of admin.schedule-created, as published by admin-service's
 * scheduleService.createSchedule (fully denormalized: train + seats + route).
 */
export interface ScheduleCreatedEventData {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  departureDate: string;
  seats: ScheduleCreatedSeatData[];
  route?: ScheduleCreatedRouteStopData[];
}

/**
 * Payload of admin.schedule-cancelled. admin-service's producer wraps the
 * schedule row in an envelope ({ eventType, data, timestamp }), but this is
 * modeled as a union since cancelScheduleInventory accepts either shape.
 */
export type ScheduleCancelledEventData =
  | { data: { scheduleId?: string; id?: string } }
  | { scheduleId?: string; id?: string };

export interface AvailabilityCounts {
  available: number;
  locked: number;
  booked: number;
}

export interface ScheduleAvailability {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  departureDate: Date;
  status: string;
  totalSeats: number;
  available: number;
  locked: number;
  booked: number;
}

export interface SeatFilters {
  status?: SeatStatus;
  seatType?: string;
  fromSeq?: number | string;
  toSeq?: number | string;
}

export interface SeatSummaryWithSegment {
  seatId: string;
  seatNumber: number;
  seatType: string;
  price: number;
  status: SeatStatus;
  lockedBy: string | null;
  lockExpiresAt: Date | null;
  bookingId: string | null;
  segmentStatus?: "AVAILABLE" | "UNAVAILABLE";
}

export interface SeatsResult {
  scheduleId: string;
  totalSeats: number;
  seats: SeatSummaryWithSegment[];
}

export interface SeatInventoryRow {
  id: string;
  seatId: string;
  seatNumber: number;
  status: SeatStatus;
  lockedBy: string | null;
}

export interface SeatIdRow {
  seatId: string;
}

export interface SeatStatusRow {
  status: SeatStatus;
}

export interface CountsRow {
  available: number;
  locked: number;
  booked: number;
}

export interface LockedSeatSummary {
  seatId: string;
  seatNumber: number;
  lockExpiresAt: Date;
}

export interface LockSeatsResult {
  scheduleId: string;
  trainId: string;
  lockedSeats: LockedSeatSummary[];
  lockExpiresAt: Date;
  counts: AvailabilityCounts;
}

export interface UnlockSeatsResult {
  scheduleId: string;
  trainId: string;
  unlockedSeats: string[];
  counts: AvailabilityCounts;
}

export interface ConfirmedSeatSummary {
  seatId: string;
  seatNumber: number;
  status: "BOOKED";
}

export interface ConfirmSeatsResult {
  scheduleId: string;
  trainId: string;
  bookingId: string;
  confirmedSeats: ConfirmedSeatSummary[];
  counts: AvailabilityCounts;
}

export interface CancelBookingResult {
  scheduleId: string;
  trainId: string;
  bookingId: string;
  releasedSeats: string[];
  counts: AvailabilityCounts;
}

export interface SegmentStatusChanges {
  nowAvailable: number;
  nowOccupied: number;
  lockedToBooked: number;
  bookedToLocked: number;
}
