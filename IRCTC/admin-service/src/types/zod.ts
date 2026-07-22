import { z } from "zod";

export const zStation = z.object({
  name: z
    .string({ error: "Station name is required" })
    .min(4, "Station name must be at least 4 characters")
    .max(40, "Station name cannot exceed 40 characters")
    .trim(),
  code: z
    .string({ error: "Station code is required" })
    .min(2, "Station code must be at least 2 characters")
    .max(10, "Station code cannot exceed 10 characters")
    .trim()
    .toUpperCase(),
  city: z
    .string({ error: "City is required" })
    .min(2, "City must be at least 2 characters")
    .max(40, "City cannot exceed 40 characters")
    .trim(),
  state: z
    .string()
    .max(40, "State cannot exceed 40 characters")
    .trim()
    .optional(),
});
export type StationBodyType = z.infer<typeof zStation>;
export const zSeat = z.object({
  seatNumber: z
    .number({ error: "Seat number is required" })
    .int("Seat number must be a whole number")
    .positive("Seat number must be greater than 0"),
  seatType: z.enum(["LOWER", "MIDDLE", "UPPER", "SIDE_LOWER", "SIDE_UPPER"], {
    error: "Invalid seat type",
  }),
  price: z
    .number({ error: "Price is required" })
    .positive("Price must be greater than 0"),
});
export type SeatBodyType = z.infer<typeof zSeat>;

export const zTrain = z.object({
  trainNumber: z
    .string({ error: "Train number is required" })
    .min(1, "Train number must be at least 1 character")
    .max(10, "Train number cannot exceed 10 characters")
    .trim(),
  trainName: z
    .string({ error: "Train name is required" })
    .min(4, "Train name must be at least 4 characters")
    .max(40, "Train name cannot exceed 40 characters")
    .trim(),
  coachName: z
    .string()
    .max(20, "Coach name cannot exceed 20 characters")
    .trim()
    .optional(),
  seats: z.array(zSeat).min(1, "At least one seat is required"),
});
export type TrainBodyType = z.infer<typeof zTrain>;
