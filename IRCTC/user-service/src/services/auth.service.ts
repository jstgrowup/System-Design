import { ErrorResponse } from "../utils/api-response";
import prisma from "../config/prisma";
import { ConflictError } from "../utils/error";
import bcrypt from "bcrypt";
import emailService from "../utils/email";
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
  const existingUser = await prisma.user.findUnique({ where: { email } });
  if (existingUser) {
    throw new ConflictError("User Already exists");
  }
  const hashedPassword = await bcrypt.hash(password, 12);
  const meta = { firstName, lastName, email, password };
  const { otp, otpSessionId } = await generateAndStoreOtp(meta);
  await emailService.sendOtpEmail(email, otp, 5);
  return otpSessionId;
};

export const authservice = {
  sendOTP,
};
