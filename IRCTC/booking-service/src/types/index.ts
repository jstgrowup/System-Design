import type { Prisma } from "../generated/prisma/client";

/** The transaction client passed into every `prisma.$transaction(async (tx) => ...)` callback. */
export type TransactionClient = Prisma.TransactionClient;

// ─── inventory-service response shapes (as booking-service consumes them) ──

export interface InventoryAvailability {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  departureDate: string;
  status: string;
  totalSeats: number;
  available: number;
  locked: number;
  booked: number;
}

export interface InventorySeatSummary {
  seatId: string;
  seatNumber: number;
  seatType: string;
  price: number;
  status: "AVAILABLE" | "LOCKED" | "BOOKED" | "CANCELLED";
  segmentStatus?: "AVAILABLE" | "UNAVAILABLE";
}

export interface InventorySeatsResult {
  scheduleId: string;
  totalSeats: number;
  seats: InventorySeatSummary[];
}

export interface InventorySeatFilters {
  status?: string;
  seatType?: string;
  fromSeq?: number;
  toSeq?: number;
}

export interface InventoryLockedSeat {
  seatId: string;
  seatNumber: number;
  lockExpiresAt: string;
}

export interface InventoryLockResult {
  scheduleId: string;
  trainId: string;
  lockedSeats: InventoryLockedSeat[];
  lockExpiresAt: string;
}

export interface InventoryUnlockResult {
  scheduleId: string;
  trainId: string;
  unlockedSeats: string[];
}

export interface InventoryConfirmResult {
  scheduleId: string;
  trainId: string;
  bookingId: string;
  confirmedSeats: { seatId: string; seatNumber: number; status: "BOOKED" }[];
}

export interface InventoryCancelResult {
  scheduleId: string;
  trainId: string;
  bookingId: string;
  releasedSeats: string[];
}

// ─── payment-service response shapes (as booking-service consumes them) ────

export interface PaymentOrder {
  paymentOrderId: string;
  gatewayOrderId: string;
  amount: number;
  currency: string;
  keyId: string;
}

export interface PaymentVerifyResult {
  status: string;
}

export interface PaymentRefundResult {
  refundId: string;
  status: string;
}

// ─── user-service / admin-service internal lookups ──────────────────────────

export interface InternalUser {
  id: string;
  email: string;
  firstName: string;
}

export interface InternalStation {
  id: string;
  name: string;
  code: string;
}

export interface UserNotificationInfo {
  email?: string;
  firstName?: string;
}

// ─── Kafka event payloads consumed by booking.consumer ─────────────────────

export interface PaymentSuccessEventData {
  paymentOrderId: string;
  gatewayPaymentId: string;
  amount: number;
}

export interface PaymentFailedEventData {
  paymentOrderId: string;
  reason?: string;
}

/**
 * admin-service's producer wraps the schedule row in an envelope
 * ({ eventType, data, timestamp }) — modeled as a union since the handler
 * accepts either shape (matches inventory-service's own consumer for the
 * same event).
 */
export type ScheduleCancelledEventData =
  | { data: { scheduleId?: string; id?: string } }
  | { scheduleId?: string; id?: string };

// ─── Booking read-model DTOs (what the HTTP surface returns) ────────────────

export interface BookingSeatSummary {
  seatId: string;
  seatNumber: number;
  seatType: string;
  price: number;
}

export interface PassengerSummary {
  name: string;
  age: number;
  gender: string;
}

export interface CreateBookingResult {
  bookingId: string;
  status: string;
  totalAmount: number;
  lockExpiresAt: Date | null;
  seats: BookingSeatSummary[];
  passengers: PassengerSummary[];
  paymentOrder: PaymentOrder;
}

export interface BookingDetail {
  id: string;
  status: string;
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  departureDate: Date;
  totalAmount: number;
  seatCount: number;
  fromStationId: string | null;
  toStationId: string | null;
  fromSeq: number | null;
  toSeq: number | null;
  paymentOrderId: string | null;
  lockExpiresAt: Date | null;
  failureReason: string | null;
  seats: BookingSeatSummary[];
  passengers: (PassengerSummary & { id: string; seatId: string | null })[];
  createdAt: Date;
  updatedAt: Date;
}

export interface UserBookingListItem {
  id: string;
  status: string;
  scheduleId: string;
  trainNumber: string;
  trainName: string;
  departureDate: Date;
  totalAmount: number;
  seatCount: number;
  fromStationId: string | null;
  toStationId: string | null;
  fromSeq: number | null;
  toSeq: number | null;
  seats: BookingSeatSummary[];
  passengers: PassengerSummary[];
  createdAt: Date;
}

export interface UserBookingsResult {
  bookings: UserBookingListItem[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

export interface CancelBookingOutcome {
  bookingId: string;
  status: "CANCELLED";
  refundInitiated: boolean;
}

export interface VerifyPaymentResult {
  bookingId: string;
  paymentStatus: string;
}
