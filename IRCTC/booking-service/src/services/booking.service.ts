import prisma from "../config/prisma";
import logger from "../config/logger";
import { config } from "../config";
import { inventoryClient } from "./inventoryClient";
import { paymentClient } from "./paymentClient";
import { userClient } from "./userClient";
import { stationClient } from "./stationClient";
import {
  acquireSeatLocks,
  releaseSeatLocks,
  forceReleaseSeatLocks,
} from "../utils/distributedLock";
import * as saga from "./saga.service";
import bookingProducer from "../kafka/producer/booking.producer";
import {
  BadRequestError,
  NotFoundError,
  ConflictError,
  StaleStateError,
} from "../utils/error";
import type { Booking } from "../generated/prisma/client";
import type {
  CreateBookingResult,
  BookingDetail,
  UserBookingsResult,
  UserBookingListItem,
  CancelBookingOutcome,
  VerifyPaymentResult,
  UserNotificationInfo,
} from "../types";
import type { PassengerBodyType } from "../types/zod";

// ─── Optimistic Lock Helper (CAS — Compare-And-Swap) ────────────────────────
// Atomically updates booking status ONLY IF the version hasn't changed since
// read. Throws StaleStateError if another process got there first.

const casUpdateBooking = async (
  bookingId: string,
  expectedVersion: number,
  data: Record<string, unknown>,
): Promise<void> => {
  const result = await prisma.booking.updateMany({
    where: { id: bookingId, version: expectedVersion },
    data: { ...data, version: { increment: 1 } },
  });

  if (result.count === 0) {
    throw new StaleStateError(
      `Booking ${bookingId} was modified by another process (expected version ${expectedVersion})`,
    );
  }
};

// ─── Notification Enrichment Helpers ─────────────────────────────────────────
// Looks up user (and optionally stations) so booking events can carry
// email/firstName directly. Failures here must never break the booking
// workflow — log and return an empty/null result instead.

const fetchUserForNotification = async (
  userId: string,
): Promise<UserNotificationInfo> => {
  try {
    const user = await userClient.getUserById(userId);
    return user ? { email: user.email, firstName: user.firstName } : {};
  } catch (err) {
    logger.warn("Failed to enrich booking event with user details", {
      userId,
      error: (err as Error).message,
    });
    return {};
  }
};

const fetchStationName = async (
  stationId: string | null,
): Promise<string | null> => {
  if (!stationId) return null;
  try {
    const station = await stationClient.getStationById(stationId);
    return station ? station.name : null;
  } catch (err) {
    logger.warn("Failed to enrich booking event with station name", {
      stationId,
      error: (err as Error).message,
    });
    return null;
  }
};

// ─── Idempotency Helper ──────────────────────────────────────────────────────

// The `as unknown as X` casts below are a deliberate external-boundary
// conversion: IdempotencyRecord.response is a Prisma Json column, and
// CreateBookingResult is a plain interface with no index signature, so
// neither direction satisfies Prisma's Json typing structurally on its own.
const checkIdempotency = async (
  key: string,
): Promise<CreateBookingResult | null> => {
  const existing = await prisma.idempotencyRecord.findUnique({
    where: { eventKey: key },
  });
  if (existing) {
    logger.info(`Idempotent request: ${key}`);
    return existing.response as unknown as CreateBookingResult;
  }
  return null;
};

const saveIdempotency = async (
  key: string,
  response: CreateBookingResult,
): Promise<void> => {
  await prisma.idempotencyRecord.create({
    data: { eventKey: key, response: response as unknown as object },
  });
};

// ─── Create Booking ──────────────────────────────────────────────────────────

const createBooking = async (
  userId: string,
  scheduleId: string,
  seatIds: string[],
  passengers: PassengerBodyType[],
  idempotencyKey: string,
  fromStationId?: string,
  toStationId?: string,
  fromSeq?: number,
  toSeq?: number,
): Promise<CreateBookingResult> => {
  // 1. Check idempotency (input shape itself is already Zod-validated by the controller)
  const cached = await checkIdempotency(`booking:${idempotencyKey}`);
  if (cached) return cached;

  // 2. Fetch schedule availability and seat details from inventory
  const availability = await inventoryClient.getAvailability(scheduleId);
  if (availability.status !== "ACTIVE") {
    throw new BadRequestError("Schedule is not active");
  }

  // Prevent booking trains that have already departed
  if (new Date(availability.departureDate) < new Date()) {
    throw new BadRequestError("Cannot book a train that has already departed");
  }

  // Segment booking: pass fromSeq/toSeq to get segment-aware seat availability
  const seatData = await inventoryClient.getSeats(scheduleId, {
    fromSeq: fromSeq,
    toSeq: toSeq,
  });
  const seatMap = new Map(seatData.seats.map((s) => [s.seatId, s]));

  // Verify all requested seats exist and are available
  const bookingSeats: {
    seatId: string;
    seatNumber: number;
    seatType: string;
    price: number;
  }[] = [];
  let totalAmount = 0;
  for (const seatId of seatIds) {
    const seat = seatMap.get(seatId);
    if (!seat) {
      throw new NotFoundError(`Seat ${seatId} not found in schedule`);
    }
    // Segment booking: use segmentStatus when available for segment-aware validation
    const isAvailable =
      fromSeq && toSeq && seat.segmentStatus !== undefined
        ? seat.segmentStatus === "AVAILABLE"
        : seat.status === "AVAILABLE";
    if (!isAvailable) {
      throw new ConflictError(
        `Seat #${seat.seatNumber} is not available for this segment`,
        "SEATS_UNAVAILABLE",
      );
    }
    bookingSeats.push(seat);
    totalAmount += seat.price;
  }

  // 3. Sort seatIds (deadlock prevention for distributed locks)
  const sortedSeatIds = [...seatIds].sort();

  // 4. Acquire Redis distributed locks (segment-aware keys for segment bookings)
  const { acquired, lockValue } = await acquireSeatLocks(
    scheduleId,
    sortedSeatIds,
    `pre-${Date.now()}`, // temporary ID before the booking row exists
    config.BOOKING_TTL_SECONDS,
    fromSeq,
    toSeq,
  );

  if (!acquired) {
    throw new ConflictError(
      "One or more seats are being booked by another user. Please try again.",
      "SEATS_LOCKED",
    );
  }

  let booking: Booking | undefined;
  try {
    // 5. Create booking record in DB
    const lockExpiresAt = new Date(
      Date.now() + config.BOOKING_TTL_SECONDS * 1000,
    );

    booking = await prisma.booking.create({
      data: {
        userId,
        scheduleId,
        trainId: availability.trainId,
        trainNumber: availability.trainNumber,
        trainName: availability.trainName,
        departureDate: new Date(availability.departureDate),
        status: "PENDING",
        totalAmount,
        seatCount: seatIds.length,
        fromStationId: fromStationId || null,
        toStationId: toStationId || null,
        fromSeq: fromSeq || null,
        toSeq: toSeq || null,
        idempotencyKey,
        lockExpiresAt,
        seats: {
          create: bookingSeats.map((seat) => ({
            seatId: seat.seatId,
            seatNumber: seat.seatNumber,
            seatType: seat.seatType,
            price: seat.price,
          })),
        },
        passengers: {
          create: passengers.map((p, index) => ({
            name: p.name,
            age: p.age,
            gender: p.gender,
            seatId: seatIds[index] || null, // use original order to match user's intended seat assignment
          })),
        },
      },
      include: { seats: true, passengers: true },
    });

    // 6. Execute saga Step 1: Hold seats in inventory
    await saga.executeHoldSeats(
      booking,
      sortedSeatIds,
      config.LOCK_TTL_SECONDS,
      fromSeq,
      toSeq,
    );

    // 7. Execute saga Step 2: Create payment order
    const paymentOrder = await saga.executeCreatePayment(booking);

    // Refresh booking after updates
    const refreshed = await prisma.booking.findUniqueOrThrow({
      where: { id: booking.id },
      include: { seats: true, passengers: true },
    });

    // 8. Save idempotency
    const response: CreateBookingResult = {
      bookingId: refreshed.id,
      status: refreshed.status,
      totalAmount: refreshed.totalAmount,
      lockExpiresAt: refreshed.lockExpiresAt,
      seats: refreshed.seats.map((s) => ({
        seatId: s.seatId,
        seatNumber: s.seatNumber,
        seatType: s.seatType,
        price: s.price,
      })),
      passengers: refreshed.passengers.map((p) => ({
        name: p.name,
        age: p.age,
        gender: p.gender,
      })),
      paymentOrder: {
        paymentOrderId: paymentOrder.paymentOrderId,
        gatewayOrderId: paymentOrder.gatewayOrderId,
        amount: paymentOrder.amount,
        currency: paymentOrder.currency,
        keyId: paymentOrder.keyId,
      },
    };

    await saveIdempotency(`booking:${idempotencyKey}`, response);

    return response;
  } catch (error) {
    // Compensate on failure
    logger.error(`Booking creation failed for user ${userId}`, {
      error: (error as Error).message,
    });

    if (booking) {
      await saga.compensateAll(booking, sortedSeatIds);
      await prisma.booking.update({
        where: { id: booking.id },
        data: {
          status: "FAILED",
          failureReason: (error as Error).message,
        },
      });
    }

    // Release Redis locks (segment-aware)
    await releaseSeatLocks(
      scheduleId,
      sortedSeatIds,
      lockValue,
      fromSeq,
      toSeq,
    );

    throw error;
  }
};

// ─── Handle Payment Success (Kafka consumer) ─────────────────────────────────

const handlePaymentSuccess = async (
  paymentOrderId: string,
  gatewayPaymentId: string,
  amount: number,
): Promise<void> => {
  const booking = await prisma.booking.findUnique({
    where: { paymentOrderId },
    include: { seats: true, passengers: true },
  });

  if (!booking) {
    logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
    return;
  }

  // Idempotent: already confirmed
  if (booking.status === "CONFIRMED") {
    logger.info(`Booking ${booking.id} already confirmed`);
    return;
  }

  if (booking.status !== "PAYMENT_PENDING") {
    logger.warn(
      `Booking ${booking.id} in unexpected status: ${booking.status}`,
    );
    return;
  }

  const seatIds = booking.seats.map((s) => s.seatId).sort();

  try {
    // Atomically claim this booking — if expiry job or cancel already changed it, bail out
    await casUpdateBooking(booking.id, booking.version, {
      status: "CONFIRMING",
    });

    // Execute saga Step 3: Confirm seats in inventory
    await saga.executeConfirmSeats(
      booking,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    );

    // Final status update (version was already incremented by CAS above)
    await prisma.booking.updateMany({
      where: { id: booking.id, status: "CONFIRMING" },
      data: { status: "CONFIRMED", version: { increment: 1 } },
    });

    // Release Redis locks (segment-aware)
    await forceReleaseSeatLocks(
      booking.scheduleId,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    );

    // Publish BOOKING_CONFIRMED (retried by producer — log but don't fail the booking)
    try {
      const [userInfo, fromStationName, toStationName] = await Promise.all([
        fetchUserForNotification(booking.userId),
        fetchStationName(booking.fromStationId),
        fetchStationName(booking.toStationId),
      ]);

      await bookingProducer.publishBookingConfirmed({
        bookingId: booking.id,
        userId: booking.userId,
        email: userInfo.email,
        firstName: userInfo.firstName,
        scheduleId: booking.scheduleId,
        trainNumber: booking.trainNumber,
        trainName: booking.trainName,
        fromStationName,
        toStationName,
        departureDate: booking.departureDate,
        seats: booking.seats.map((s) => ({
          seatNumber: s.seatNumber,
          seatType: s.seatType,
          price: s.price,
        })),
        passengers: booking.passengers.map((p) => ({
          name: p.name,
          age: p.age,
          gender: p.gender,
        })),
        totalAmount: booking.totalAmount,
      });
    } catch (err) {
      logger.error(
        "CRITICAL: Failed to publish BOOKING_CONFIRMED after retries — notification/search may be stale",
        { bookingId: booking.id, error: (err as Error).message },
      );
    }

    logger.info(`Booking ${booking.id} confirmed successfully`);
  } catch (error) {
    // If StaleStateError, another process already handled this booking — do nothing
    if (error instanceof StaleStateError) {
      logger.info(
        `Booking ${booking.id} already handled by another process, skipping`,
      );
      return;
    }

    logger.error(`Failed to confirm booking ${booking.id}`, {
      error: (error as Error).message,
    });

    // Compensate: refund payment and release seats
    await saga.compensateAll(booking, seatIds);

    await prisma.booking.updateMany({
      where: {
        id: booking.id,
        status: { in: ["PAYMENT_PENDING", "CONFIRMING"] },
      },
      data: {
        status: "FAILED",
        failureReason: `confirm_failed: ${(error as Error).message}`,
        version: { increment: 1 },
      },
    });

    await forceReleaseSeatLocks(
      booking.scheduleId,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    );

    try {
      const userInfo = await fetchUserForNotification(booking.userId);
      await bookingProducer.publishBookingFailed({
        bookingId: booking.id,
        userId: booking.userId,
        email: userInfo.email,
        firstName: userInfo.firstName,
        scheduleId: booking.scheduleId,
        reason: "confirm_seats_failed",
      });
    } catch (err) {
      logger.error("Failed to publish BOOKING_FAILED after retries", {
        bookingId: booking.id,
        error: (err as Error).message,
      });
    }
  }
};

// ─── Handle Payment Failure (Kafka consumer) ─────────────────────────────────

const handlePaymentFailure = async (
  paymentOrderId: string,
  reason?: string,
): Promise<void> => {
  const booking = await prisma.booking.findUnique({
    where: { paymentOrderId },
    include: { seats: true },
  });

  if (!booking) {
    logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
    return;
  }

  // Idempotent
  if (["FAILED", "CANCELLED", "EXPIRED"].includes(booking.status)) {
    logger.info(
      `Booking ${booking.id} already in terminal state: ${booking.status}`,
    );
    return;
  }

  if (booking.status !== "PAYMENT_PENDING") {
    logger.warn(
      `Booking ${booking.id} in unexpected status: ${booking.status}`,
    );
    return;
  }

  const seatIds = booking.seats.map((s) => s.seatId).sort();

  // Atomically claim this booking before compensating
  try {
    await casUpdateBooking(booking.id, booking.version, {
      status: "FAILED",
      failureReason: reason || "payment_failed",
    });
  } catch (error) {
    if (error instanceof StaleStateError) {
      logger.info(
        `Booking ${booking.id} already handled by another process, skipping`,
      );
      return;
    }
    throw error;
  }

  // Compensate: release held seats
  await saga.compensateHoldSeats(booking, seatIds);

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
      reason: reason || "payment_failed",
    });
  } catch (err) {
    logger.error("Failed to publish BOOKING_FAILED after retries", {
      bookingId: booking.id,
      error: (err as Error).message,
    });
  }

  logger.info(`Booking ${booking.id} failed: ${reason}`);
};

// ─── Cancel Booking ──────────────────────────────────────────────────────────

const cancelBooking = async (
  bookingId: string,
  userId: string,
): Promise<CancelBookingOutcome> => {
  const booking = await prisma.booking.findUnique({
    where: { id: bookingId },
    include: { seats: true },
  });

  if (!booking || booking.userId !== userId) {
    throw new NotFoundError("Booking not found");
  }

  if (
    ["CANCELLED", "CANCELLING", "FAILED", "EXPIRED", "CONFIRMING"].includes(
      booking.status,
    )
  ) {
    throw new ConflictError(`Booking is already ${booking.status}`);
  }

  const seatIds = booking.seats.map((s) => s.seatId).sort();
  let refundInitiated = false;

  // Atomically claim this booking — prevents race with payment webhook or expiry job
  try {
    await casUpdateBooking(booking.id, booking.version, {
      status: "CANCELLING",
      failureReason: "user_cancelled",
    });
  } catch (error) {
    if (error instanceof StaleStateError) {
      // Re-read to give user accurate error
      const fresh = await prisma.booking.findUnique({
        where: { id: bookingId },
      });
      throw new ConflictError(
        `Booking status changed to ${fresh?.status || "unknown"} while cancelling. Please refresh.`,
      );
    }
    throw error;
  }

  if (booking.status === "CONFIRMED") {
    // Cancel confirmed booking: release seats + refund
    try {
      await inventoryClient.cancelBooking(
        booking.scheduleId,
        booking.id,
        booking.userId,
      );
    } catch (error) {
      logger.error(
        `Failed to release seats in inventory for booking ${booking.id}`,
        {
          error: (error as Error).message,
        },
      );
      // Roll back from CANCELLING to CONFIRMED so the user can retry
      await prisma.booking.updateMany({
        where: { id: booking.id, status: "CANCELLING" },
        data: {
          status: "CONFIRMED",
          failureReason: null,
          version: { increment: 1 },
        },
      });
      throw error;
    }

    if (booking.paymentOrderId) {
      try {
        const idempotencyKey = `${booking.id}-cancel-refund`;
        await paymentClient.initiateRefund(
          booking.paymentOrderId,
          booking.totalAmount,
          "user_cancelled",
          idempotencyKey,
        );
        refundInitiated = true;
      } catch (error) {
        logger.error(`Failed to initiate refund for booking ${booking.id}`, {
          error: (error as Error).message,
        });
      }
    }
  } else if (["PAYMENT_PENDING", "SEATS_HELD"].includes(booking.status)) {
    // Release held seats
    try {
      await inventoryClient.releaseSeats(
        booking.scheduleId,
        seatIds,
        booking.userId,
        booking.fromSeq,
        booking.toSeq,
      );
    } catch (error) {
      logger.error("Failed to release seats during cancel", {
        error: (error as Error).message,
      });
    }
  }

  // Final status (CANCELLING → CANCELLED)
  await prisma.booking.updateMany({
    where: { id: booking.id, status: "CANCELLING" },
    data: {
      status: "CANCELLED",
      version: { increment: 1 },
    },
  });

  // Release Redis locks (segment-aware)
  await forceReleaseSeatLocks(
    booking.scheduleId,
    seatIds,
    booking.fromSeq,
    booking.toSeq,
  );

  // Publish BOOKING_CANCELLED
  try {
    const userInfo = await fetchUserForNotification(booking.userId);
    await bookingProducer.publishBookingCancelled({
      bookingId: booking.id,
      userId: booking.userId,
      email: userInfo.email,
      firstName: userInfo.firstName,
      scheduleId: booking.scheduleId,
      reason: "user_cancelled",
      refundAmount: refundInitiated ? booking.totalAmount : 0,
    });
  } catch (err) {
    logger.error("Failed to publish BOOKING_CANCELLED after retries", {
      bookingId: booking.id,
      error: (err as Error).message,
    });
  }

  logger.info(`Booking ${booking.id} cancelled by user ${userId}`);

  return {
    bookingId: booking.id,
    status: "CANCELLED",
    refundInitiated,
  };
};

// ─── Get Booking ─────────────────────────────────────────────────────────────

const getBooking = async (
  bookingId: string,
  userId: string,
): Promise<BookingDetail> => {
  const booking = await prisma.booking.findUnique({
    where: { id: bookingId },
    include: {
      seats: { orderBy: { seatNumber: "asc" } },
      passengers: true,
    },
  });

  if (!booking || booking.userId !== userId) {
    throw new NotFoundError("Booking not found");
  }

  return {
    id: booking.id,
    status: booking.status,
    scheduleId: booking.scheduleId,
    trainId: booking.trainId,
    trainNumber: booking.trainNumber,
    trainName: booking.trainName,
    departureDate: booking.departureDate,
    totalAmount: booking.totalAmount,
    seatCount: booking.seatCount,
    fromStationId: booking.fromStationId,
    toStationId: booking.toStationId,
    fromSeq: booking.fromSeq,
    toSeq: booking.toSeq,
    paymentOrderId: booking.paymentOrderId,
    lockExpiresAt: booking.lockExpiresAt,
    failureReason: booking.failureReason,
    seats: booking.seats.map((s) => ({
      seatId: s.seatId,
      seatNumber: s.seatNumber,
      seatType: s.seatType,
      price: s.price,
    })),
    passengers: booking.passengers.map((p) => ({
      id: p.id,
      name: p.name,
      age: p.age,
      gender: p.gender,
      seatId: p.seatId,
    })),
    createdAt: booking.createdAt,
    updatedAt: booking.updatedAt,
  };
};

// ─── Get User Bookings ───────────────────────────────────────────────────────

interface GetUserBookingsOptions {
  status?: string;
  page?: number;
  limit?: number;
}

const getUserBookings = async (
  userId: string,
  { status, page = 1, limit = 10 }: GetUserBookingsOptions = {},
): Promise<UserBookingsResult> => {
  const skip = (page - 1) * limit;
  const where = {
    userId,
    ...(status ? { status: status.toUpperCase() as Booking["status"] } : {}),
  };

  const [bookings, total] = await Promise.all([
    prisma.booking.findMany({
      where,
      include: {
        seats: { orderBy: { seatNumber: "asc" } },
        passengers: true,
      },
      orderBy: { createdAt: "desc" },
      skip,
      take: limit,
    }),
    prisma.booking.count({ where }),
  ]);

  const items: UserBookingListItem[] = bookings.map((b) => ({
    id: b.id,
    status: b.status,
    scheduleId: b.scheduleId,
    trainNumber: b.trainNumber,
    trainName: b.trainName,
    departureDate: b.departureDate,
    totalAmount: b.totalAmount,
    seatCount: b.seatCount,
    fromStationId: b.fromStationId,
    toStationId: b.toStationId,
    fromSeq: b.fromSeq,
    toSeq: b.toSeq,
    seats: b.seats.map((s) => ({
      seatId: s.seatId,
      seatNumber: s.seatNumber,
      seatType: s.seatType,
      price: s.price,
    })),
    passengers: b.passengers.map((p) => ({
      name: p.name,
      age: p.age,
      gender: p.gender,
    })),
    createdAt: b.createdAt,
  }));

  return {
    bookings: items,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
    },
  };
};

// ─── Verify Payment (client-side verification after Razorpay checkout) ───────

const verifyPayment = async (
  bookingId: string,
  userId: string,
  razorpayPaymentId: string,
  razorpaySignature: string,
): Promise<VerifyPaymentResult> => {
  const booking = await prisma.booking.findUnique({
    where: { id: bookingId },
  });

  if (!booking || booking.userId !== userId) {
    throw new NotFoundError("Booking not found");
  }

  if (!booking.paymentOrderId) {
    throw new BadRequestError("Booking has no payment order");
  }

  if (booking.status === "CONFIRMED") {
    return { bookingId: booking.id, paymentStatus: "CONFIRMED" };
  }

  if (booking.status !== "PAYMENT_PENDING") {
    throw new ConflictError(
      `Booking is in ${booking.status} status, cannot verify payment`,
    );
  }

  // Call payment service to verify and capture
  const result = await paymentClient.verifyPayment(
    booking.paymentOrderId,
    razorpayPaymentId,
    razorpaySignature,
  );

  logger.info(`Payment verified for booking ${bookingId}`, { result });

  return {
    bookingId: booking.id,
    paymentStatus: result.status,
  };
};

// ─── Handle Schedule Cancelled (Kafka consumer) ─────────────────────────────
// When a schedule is cancelled, all active bookings on that schedule must be
// failed/cancelled so users aren't left with stranded tickets.

const handleScheduleCancelled = async (
  scheduleId: string | undefined,
): Promise<void> => {
  if (!scheduleId) {
    logger.warn("handleScheduleCancelled called without scheduleId");
    return;
  }

  const activeBookings = await prisma.booking.findMany({
    where: {
      scheduleId,
      status: { in: ["PENDING", "SEATS_HELD", "PAYMENT_PENDING", "CONFIRMED"] },
    },
    include: { seats: true },
  });

  if (activeBookings.length === 0) {
    logger.info(`No active bookings to cancel for schedule ${scheduleId}`);
    return;
  }

  logger.info(
    `Cancelling ${activeBookings.length} active booking(s) due to schedule cancellation`,
    { scheduleId },
  );

  for (const booking of activeBookings) {
    try {
      // CAS: claim ownership of this booking transition
      const claimed = await prisma.booking.updateMany({
        where: {
          id: booking.id,
          version: booking.version,
          status: {
            in: ["PENDING", "SEATS_HELD", "PAYMENT_PENDING", "CONFIRMED"],
          },
        },
        data: {
          status: "CANCELLED",
          failureReason: "schedule_cancelled",
          version: { increment: 1 },
        },
      });

      if (claimed.count === 0) {
        logger.info(
          `Booking ${booking.id} already handled, skipping schedule-cancel`,
        );
        continue;
      }

      const seatIds = booking.seats.map((s) => s.seatId).sort();

      // Release Redis locks if any are still held
      await forceReleaseSeatLocks(
        booking.scheduleId,
        seatIds,
        booking.fromSeq,
        booking.toSeq,
      );

      // Initiate refund for confirmed bookings that had payment
      if (booking.status === "CONFIRMED" && booking.paymentOrderId) {
        try {
          const idempotencyKey = `${booking.id}-schedule-cancel-refund`;
          await paymentClient.initiateRefund(
            booking.paymentOrderId,
            booking.totalAmount,
            "schedule_cancelled",
            idempotencyKey,
          );
        } catch (refundErr) {
          logger.error(
            `Failed to initiate refund for booking ${booking.id} during schedule cancellation`,
            { error: (refundErr as Error).message },
          );
        }
      }

      // Publish BOOKING_CANCELLED event
      try {
        const userInfo = await fetchUserForNotification(booking.userId);
        await bookingProducer.publishBookingCancelled({
          bookingId: booking.id,
          userId: booking.userId,
          email: userInfo.email,
          firstName: userInfo.firstName,
          scheduleId: booking.scheduleId,
          reason: "schedule_cancelled",
          refundAmount:
            booking.status === "CONFIRMED" ? booking.totalAmount : 0,
        });
      } catch (err) {
        logger.error(
          "Failed to publish BOOKING_CANCELLED for schedule cancellation",
          {
            bookingId: booking.id,
            error: (err as Error).message,
          },
        );
      }

      logger.info(
        `Booking ${booking.id} cancelled due to schedule cancellation`,
      );
    } catch (error) {
      logger.error(
        `Failed to cancel booking ${booking.id} during schedule cancellation`,
        {
          error: (error as Error).message,
        },
      );
    }
  }
};

export const bookingService = {
  createBooking,
  handlePaymentSuccess,
  handlePaymentFailure,
  handleScheduleCancelled,
  cancelBooking,
  getBooking,
  getUserBookings,
  verifyPayment,
};
