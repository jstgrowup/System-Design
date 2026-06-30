import { ErrorResponse } from "../utils/api-response";

const sendOTP = async ({
  firstName,
  lastName,
  email,
  password,
}: {
  firstName: string;
  lastName: string;
  email: string;
  password: string;
}) => {
  const existingUser = {};
  if (existingUser) {
  }
  const otpsessionId = "okau";
  return { otpsessionId };
};

export const authservice = {
  sendOTP,
};
