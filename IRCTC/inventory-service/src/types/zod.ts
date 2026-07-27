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
