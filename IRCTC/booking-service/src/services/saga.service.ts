import prisma from "../config/prisma";
import logger from "../config/logger";
import { inventoryClient } from "./inventoryClient";
import { paymentClient } from "./paymentClient";
import type { Booking, Prisma } from "../generated/prisma/client";
import type { InventoryLockResult, InventoryConfirmResult, PaymentOrder } from "../types";

/**
 * SagaLog.response is a Prisma Json column — these downstream response
 * shapes are plain interfaces with no index signature, so they don't
 * structurally satisfy Prisma's InputJsonValue on their own. This is a
 * deliberate external-boundary cast (writing an already-validated,
 * JSON-serializable object into a Json column), not a general escape hatch.
 */
function toJson(value: unknown): Prisma.InputJsonValue {
  return value as Prisma.InputJsonValue;
}

/**
 * Saga orchestrator for booking lifecycle.
 * Each step is logged to SagaLog for auditability and crash recovery.
 *
 * Forward flow: HOLD_SEATS -> CREATE_PAYMENT -> CONFIRM_SEATS -> COMPLETE
 * Compensation: reverse order of completed steps
 */

function errorMessage(error: unknown): string {
  const withResponse = error as { response?: { data?: { message?: string } }; message?: string };
  return withResponse.response?.data?.message || withResponse.message || String(error);
}

// ─── Forward Steps ───────────────────────────────────────────────────────────

export async function executeHoldSeats(
  booking: Booking,
  seatIds: string[],
  ttlSeconds: number,
  fromSeq?: number | null,
  toSeq?: number | null,
): Promise<InventoryLockResult> {
  const sagaLog = await prisma.sagaLog.create({
    data: {
      bookingId: booking.id,
      step: "HOLD_SEATS",
      status: "PENDING",
      request: {
        scheduleId: booking.scheduleId,
        seatIds,
        userId: booking.userId,
        ttlSeconds,
        fromSeq,
        toSeq,
      },
    },
  });

  try {
    const result = await inventoryClient.holdSeats(
      booking.scheduleId,
      seatIds,
      booking.userId,
      ttlSeconds,
      fromSeq ?? undefined,
      toSeq ?? undefined,
    );

    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "COMPLETED", response: toJson(result) },
    });

    await prisma.booking.update({
      where: { id: booking.id },
      data: { status: "SEATS_HELD" },
    });

    logger.info(`Saga HOLD_SEATS completed for booking ${booking.id}`);
    return result;
  } catch (error) {
    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "FAILED", error: errorMessage(error) },
    });
    throw error;
  }
}

export async function executeCreatePayment(booking: Booking): Promise<PaymentOrder> {
  const idempotencyKey = `${booking.id}-payment`;

  const sagaLog = await prisma.sagaLog.create({
    data: {
      bookingId: booking.id,
      step: "CREATE_PAYMENT",
      status: "PENDING",
      request: { bookingId: booking.id, amount: booking.totalAmount, userId: booking.userId },
    },
  });

  try {
    const result = await paymentClient.createPaymentOrder(
      booking.id,
      booking.totalAmount,
      booking.userId,
      idempotencyKey,
    );

    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "COMPLETED", response: toJson(result) },
    });

    await prisma.booking.update({
      where: { id: booking.id },
      data: {
        status: "PAYMENT_PENDING",
        paymentOrderId: result.paymentOrderId,
      },
    });

    logger.info(`Saga CREATE_PAYMENT completed for booking ${booking.id}`);
    return result;
  } catch (error) {
    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "FAILED", error: errorMessage(error) },
    });
    throw error;
  }
}

export async function executeConfirmSeats(
  booking: Booking,
  seatIds: string[],
  fromSeq?: number | null,
  toSeq?: number | null,
): Promise<InventoryConfirmResult> {
  const sagaLog = await prisma.sagaLog.create({
    data: {
      bookingId: booking.id,
      step: "CONFIRM_SEATS",
      status: "PENDING",
      request: {
        scheduleId: booking.scheduleId,
        seatIds,
        userId: booking.userId,
        bookingId: booking.id,
        fromSeq,
        toSeq,
      },
    },
  });

  try {
    const result = await inventoryClient.confirmSeats(
      booking.scheduleId,
      seatIds,
      booking.userId,
      booking.id,
      fromSeq,
      toSeq,
    );

    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "COMPLETED", response: toJson(result) },
    });

    logger.info(`Saga CONFIRM_SEATS completed for booking ${booking.id}`);
    return result;
  } catch (error) {
    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "FAILED", error: errorMessage(error) },
    });
    throw error;
  }
}

// ─── Compensation Steps ──────────────────────────────────────────────────────

export async function compensateHoldSeats(
  booking: Booking,
  seatIds: string[],
): Promise<void> {
  logger.info(`Compensating HOLD_SEATS for booking ${booking.id}`);
  try {
    await inventoryClient.releaseSeats(
      booking.scheduleId,
      seatIds,
      booking.userId,
      booking.fromSeq,
      booking.toSeq,
    );

    await prisma.sagaLog.updateMany({
      where: { bookingId: booking.id, step: "HOLD_SEATS", status: "COMPLETED" },
      data: { status: "COMPENSATED" },
    });
  } catch (error) {
    logger.error(`Failed to compensate HOLD_SEATS for booking ${booking.id}`, {
      error: errorMessage(error),
    });
    // Inventory lock expiry will eventually clean this up
  }
}

export async function compensateCreatePayment(booking: Booking): Promise<void> {
  if (!booking.paymentOrderId) return;

  logger.info(`Compensating CREATE_PAYMENT for booking ${booking.id}`);
  try {
    const idempotencyKey = `${booking.id}-refund-compensation`;
    await paymentClient.initiateRefund(
      booking.paymentOrderId,
      booking.totalAmount,
      "booking_compensation",
      idempotencyKey,
    );

    await prisma.sagaLog.updateMany({
      where: { bookingId: booking.id, step: "CREATE_PAYMENT", status: "COMPLETED" },
      data: { status: "COMPENSATED" },
    });
  } catch (error) {
    logger.error(`Failed to compensate CREATE_PAYMENT for booking ${booking.id}`, {
      error: errorMessage(error),
    });
  }
}

export async function compensateConfirmSeats(booking: Booking): Promise<void> {
  logger.info(`Compensating CONFIRM_SEATS for booking ${booking.id}`);
  try {
    await inventoryClient.cancelBooking(booking.scheduleId, booking.id, booking.userId);

    await prisma.sagaLog.updateMany({
      where: { bookingId: booking.id, step: "CONFIRM_SEATS", status: "COMPLETED" },
      data: { status: "COMPENSATED" },
    });
  } catch (error) {
    logger.error(`Failed to compensate CONFIRM_SEATS for booking ${booking.id}`, {
      error: errorMessage(error),
    });
  }
}

/**
 * Compensate all completed saga steps in reverse order.
 * Used when a booking needs to be rolled back (failure, timeout, cancellation).
 */
export async function compensateAll(booking: Booking, seatIds: string[]): Promise<void> {
  const completedSteps = await prisma.sagaLog.findMany({
    where: { bookingId: booking.id, status: "COMPLETED" },
    orderBy: { createdAt: "desc" },
  });

  for (const step of completedSteps) {
    switch (step.step) {
      case "CONFIRM_SEATS":
        await compensateConfirmSeats(booking);
        break;
      case "CREATE_PAYMENT":
        await compensateCreatePayment(booking);
        break;
      case "HOLD_SEATS":
        await compensateHoldSeats(booking, seatIds);
        break;
    }
  }
}
