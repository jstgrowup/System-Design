import { z } from "zod";

export const zSearchTrains = z.object({
  from: z
    .string({ error: "Origin station is required" })
    .trim()
    .min(1, "Origin station is required")
    .max(50, "Origin station cannot exceed 50 characters"),
  to: z
    .string({ error: "Destination station is required" })
    .trim()
    .min(1, "Destination station is required")
    .max(50, "Destination station cannot exceed 50 characters"),
  date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/, "Date must be in YYYY-MM-DD format")
    .optional(),
});
export type SearchTrainsQuery = z.infer<typeof zSearchTrains>;
