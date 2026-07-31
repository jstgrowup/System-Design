import { ZodError } from "zod/v4";

export const formatZodError = (error: ZodError): string => {
  const issue = error.issues[0];
  // No template literal here — `${issue?.message}` would coerce a missing
  // message into the truthy string "undefined", silently defeating this
  // fallback.
  return issue?.message || "Validation failed";
};
