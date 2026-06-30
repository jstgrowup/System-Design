import { ZodError } from "zod/v4";

export const formatZodError = (error: ZodError): Record<string, string> => {
  const errors: Record<string, string> = {};
  for (const issue of error.issues) {
    const path = issue.path.join(".") || "root";
    if (!errors[path]) {
      errors[path] = issue.message;
    }
  }
  return errors;
};
