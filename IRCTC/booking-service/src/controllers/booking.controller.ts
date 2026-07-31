import { Request, Response } from "express";
import asyncHandler from "../utils/asyncHandler";
import { bookingService } from "../services/booking.service";
import { zCreateBooking, zVerifyPayment, zGetUserBookings } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

/**
 * POST /bookings
 *
 * Kicks off the booking saga: validates the body against zCreateBooking,
 * then hands off to bookingService.createBooking, which holds seats,
 * creates a payment order, and returns the payment details the client needs
 * to complete checkout.
 */
const createBooking = asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.id;

  const result = zCreateBooking.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const {
    scheduleId,
    seatIds,
    passengers,
    idempotencyKey,
    fromStationId,
    toStationId,
    fromSeq,
    toSeq,
  } = result.data;

  const booking = await bookingService.createBooking(
    userId,
    scheduleId,
    seatIds,
    passengers,
    idempotencyKey,
    fromStationId,
    toStationId,
    fromSeq,
    toSeq,
  );

  res.status(201).json({ success: true, data: booking });
});

/**
 * GET /bookings/:bookingId
 */
const getBooking = asyncHandler(
  async (req: Request<{ bookingId: string }>, res: Response) => {
    const userId = req.user!.id;
    const { bookingId } = req.params;

    const result = await bookingService.getBooking(bookingId, userId);

    res.status(200).json({ success: true, data: result });
  },
);

/**
 * GET /bookings?status=&page=&limit=
 */
const getUserBookings = asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.id;

  const result = zGetUserBookings.safeParse(req.query);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { status, page, limit } = result.data;

  const bookings = await bookingService.getUserBookings(userId, {
    status,
    page,
    limit,
  });

  res.status(200).json({ success: true, data: bookings });
});

/**
 * POST /bookings/:bookingId/verify-payment
 */
const verifyPayment = asyncHandler(
  async (req: Request<{ bookingId: string }>, res: Response) => {
    const userId = req.user!.id;
    const { bookingId } = req.params;

    const result = zVerifyPayment.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, { message: formatZodError(result.error) });
    }

    const { razorpayPaymentId, razorpaySignature } = result.data;

    const outcome = await bookingService.verifyPayment(
      bookingId,
      userId,
      razorpayPaymentId,
      razorpaySignature,
    );

    res.status(200).json({ success: true, data: outcome });
  },
);

/**
 * POST /bookings/:bookingId/cancel
 */
const cancelBooking = asyncHandler(
  async (req: Request<{ bookingId: string }>, res: Response) => {
    const userId = req.user!.id;
    const { bookingId } = req.params;

    const result = await bookingService.cancelBooking(bookingId, userId);

    res.status(200).json({
      success: true,
      message: "Booking cancelled successfully",
      data: result,
    });
  },
);

export const bookingController = {
  createBooking,
  getBooking,
  getUserBookings,
  verifyPayment,
  cancelBooking,
};
