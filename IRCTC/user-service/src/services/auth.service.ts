import { ErrorResponse } from "../utils/api-response";
import prisma from "../config/prisma";
import { BadRequestError, ConflictError, ForbiddenError } from "../utils/error";
import bcrypt from "bcrypt";
import emailService from "../utils/email";
import jwt from "jsonwebtoken";
import { generateAndStoreOtp, verifyOtpViaUnHashing } from "../utils/otp";
import {
  generateAccessToken,
  generateRefreshToken,
  verifyRefreshToken,
} from "../utils/auth";
import { redis } from "../config/redis";
import { config } from "../config";
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
const login = async ({
  email,
  password,
  deviceId,
}: {
  email: string;
  password: string;
  deviceId: string;
}) => {
  const existingUser = await prisma.user.findUnique({ where: { email } });
  if (!existingUser || !existingUser.password) {
    throw new BadRequestError("Email not found");
  }
  const doesPasswordMatch = await bcrypt.compare(
    password,
    existingUser.password,
  );
  if (!doesPasswordMatch) {
    throw new BadRequestError("Incorrect Password");
  }
  const accessToken = generateAccessToken(existingUser.id);
  const refreshToken = generateRefreshToken(existingUser.id);
  const data = jwt.decode(refreshToken) as any;
  await redis.set(
    `refresh:${existingUser.id}:${deviceId}`,
    data.jti,
    "EX",
    config.REFRESH_TOKEN_EXP_SEC,
  );
  const { password: _password, ...safeUser } = existingUser;
  await redis.set(
    `user:${existingUser.id}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );
  return { accessToken, refreshToken, loggedInUser: existingUser };
};
const rotateRefreshToken = async ({
  refreshToken,
  deviceId,
}: {
  refreshToken: string;
  deviceId: string;
}) => {
  const payload = verifyRefreshToken(refreshToken);
  const { id: userId, jti } = payload;
  const storedJti = await redis.get(`refresh:${userId}:${deviceId}`);
  if (!storedJti) {
    throw new ForbiddenError("Session Expired", "Login AGain");
  }
  if (storedJti !== jti) {
    await redis.del(`refresh:${userId}:${deviceId}`);
    throw new ForbiddenError("Refresh token reused", "LOGIN AGAIn");
  }
  const newAccessToken = generateAccessToken(payload.id);
  const newRefreshToken = generateRefreshToken(payload.id);
  const response = jwt.decode(newRefreshToken) as any;
  await redis.set(
    `refresh:${payload.id}:${deviceId}`,
    response.jti,
    "EX",
    config.REFRESH_TOKEN_EXP_SEC,
  );
  return { newAccessToken, newRefreshToken };
};
export const authservice = {
  sendOtp,
  verifyOtp,
  login,
  rotateRefreshToken,
};
