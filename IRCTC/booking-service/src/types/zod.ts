import { z } from "zod";

export const zPassenger = z.object({
  name: z
    .string({ error: "Passenger name is required" })
    .min(2, "Passenger name must be at least 2 characters")
    .max(40, "Passenger name cannot exceed 40 characters")
    .trim(),
  age: z
    .number({ error: "Passenger age is required" })
    .int("Age must be a whole number")
    .positive("Age must be greater than 0")
    .max(120, "Age must be realistic"),
  gender: z.enum(["MALE", "FEMALE", "OTHER"], { error: "Invalid gender" }),
});
export type PassengerBodyType = z.infer<typeof zPassenger>;

export const zCreateBooking = z
  .object({
    scheduleId: z.uuid("Schedule ID must be a valid UUID"),
    seatIds: z
      .array(z.string().min(1))
      .min(1, "At least one seat must be selected"),
    passengers: z
      .array(zPassenger)
      .min(1, "At least one passenger is required"),
    idempotencyKey: z
      .string({ error: "idempotencyKey is required" })
      .min(1, "idempotencyKey is required"),
    // Segment booking: boarding/alighting station + their sequence numbers
    // on the route. Either all four are provided (a partial-journey
    // booking) or none are (a full-journey booking).
    fromStationId: z.uuid().optional(),
    toStationId: z.uuid().optional(),
    fromSeq: z.number().int().positive().optional(),
    toSeq: z.number().int().positive().optional(),
  })
  .refine((data) => data.seatIds.length === data.passengers.length, {
    message: "Number of seats must match number of passengers",
    path: ["seatIds"],
  })
  .refine(
    (data) =>
      !(data.fromSeq !== undefined && data.toSeq !== undefined) ||
      data.fromSeq < data.toSeq,
    {
      message: "fromStation must come before toStation in route",
      path: ["fromSeq"],
    },
  );
export type CreateBookingBodyType = z.infer<typeof zCreateBooking>;

export const zVerifyPayment = z.object({
  razorpayPaymentId: z
    .string({ error: "razorpayPaymentId is required" })
    .min(1, "razorpayPaymentId is required"),
  razorpaySignature: z
    .string({ error: "razorpaySignature is required" })
    .min(1, "razorpaySignature is required"),
});
export type VerifyPaymentBodyType = z.infer<typeof zVerifyPayment>;

export const zGetUserBookings = z.object({
  status: z
    .enum([
      "PENDING",
      "SEATS_HELD",
      "PAYMENT_PENDING",
      "CONFIRMING",
      "CONFIRMED",
      "CANCELLING",
      "FAILED",
      "CANCELLED",
      "EXPIRED",
    ])
    .optional(),
  page: z.coerce.number().int().positive().optional().default(1),
  limit: z.coerce.number().int().positive().max(100).optional().default(10),
});
export type GetUserBookingsQueryType = z.infer<typeof zGetUserBookings>;
