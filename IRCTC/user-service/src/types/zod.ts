import { z } from "zod";
export const zSignUp = z.object({
  firstName: z
    .string({ error: "First name is required" })
    .min(4, "First name must be at least 4 characters")
    .max(40, "First name cannot exceed 40 characters")
    .trim(),
  lastName: z.string().max(40).trim().optional(),
  email: z.email("Invalid email format").trim().toLowerCase(),
  password: z
    .string({ error: "Password is required" })
    .min(8, "Password must be at least 8 characters")
    .regex(/[A-Z]/, "Password must contain at least one uppercase letter")
    .regex(/[a-z]/, "Password must contain at least one lowercase letter")
    .regex(/[0-9]/, "Password must contain at least one number"),
});
export type SignUpBodyType = z.infer<typeof zSignUp>;
