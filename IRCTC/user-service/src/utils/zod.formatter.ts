import { ZodError } from "zod/v4";

export const formatZodError = (error: ZodError): string => {
  const issue = error.issues[0];
  return `${issue?.message}` || "Validation failed";
};
