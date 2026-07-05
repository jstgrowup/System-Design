import { ErrorResponse } from "../utils/api-response";
import prisma from "../config/prisma";
import { BadRequestError, ConflictError } from "../utils/error";
import bcrypt from "bcrypt";
import emailService from "../utils/email";
import { generateAndStoreOtp, verifyOtpViaUnHashing } from "../utils/otp";
const sendOtp = async ({
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
  const meta = { firstName, lastName, email, password: hashedPassword };
  const { otp, otpSessionId } = await generateAndStoreOtp(meta);
  await emailService.sendOtpEmail(email, otp, 5);
  return otpSessionId;
};
const verifyOtp = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  const meta = await verifyOtpViaUnHashing({ otp, otpSessionId });
  if (!meta) {
    throw new BadRequestError("Invalid or expired OTP", "OTP_INVALID");
  }
  const user = await prisma.user.create({
    data: {
      firstName: meta.firstName,
      lastName: meta.lastName,
      email: meta.email,
      password: meta.hashedPassword,
      emailVerified: true,
    },
  });

  return user;
};
export const authservice = {
  sendOtp,
  verifyOtp,
};
