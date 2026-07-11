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
import notificationProducer from "../kafka/producer/notification-producer";
import logger from "../config/logger";

/**
 * Handles the first step of registration.
 * Checks for duplicate email, hashes the password,
 * generates an OTP, stores it in Redis, and sends it via email.
 */
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
  // Prevent duplicate registrations
  const existingUser = await prisma.user.findUnique({ where: { email } });
  if (existingUser) {
    throw new ConflictError("User already exists");
  }

  // Hash password before storing in Redis (never store plaintext)
  const hashedPassword = await bcrypt.hash(password, 12);

  // Bundle user meta with hashed password — stored in Redis against the OTP session
  const meta = { firstName, lastName, email, password: hashedPassword };

  // Generate OTP + session ID, store HMAC of OTP in Redis
  const { otp, otpSessionId } = await generateAndStoreOtp(meta);

  // Send OTP to user's email with a 5-minute TTL message
  await notificationProducer.sendOtpEmail({
    email,
    otp,
    ttlMinutes: config.OTP_TTL / 60,
  });
  logger.info(`OTP email queused for : ${email}`);
  await emailService.sendOtpEmail(email, otp, 5);

  // Return session ID to be set as a cookie in the controller
  return otpSessionId;
};

/**
 * Handles the second step of registration.
 * Verifies the OTP via HMAC comparison and creates the user in the DB.
 */
const verifyOtp = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  // Retrieve and verify the OTP from Redis using HMAC comparison
  const meta = await verifyOtpViaUnHashing({ otp, otpSessionId });
  if (!meta) {
    throw new BadRequestError("Invalid or expired OTP", "OTP_INVALID");
  }

  // Create the verified user in the database
  const user = await prisma.user.create({
    data: {
      firstName: meta.firstName,
      lastName: meta.lastName,
      email: meta.email,
      password: meta.hashedPassword, // already bcrypt hashed from sendOtp
      emailVerified: true,
    },
  });

  return user;
};

/**
 * Handles user login.
 * Validates credentials, issues access + refresh tokens,
 * stores the refresh token JTI in Redis scoped to the device,
 * and caches the safe user object.
 */
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

  // Compare submitted password against the stored bcrypt hash
  const doesPasswordMatch = await bcrypt.compare(
    password,
    existingUser.password,
  );
  if (!doesPasswordMatch) {
    throw new BadRequestError("Incorrect password");
  }

  const accessToken = generateAccessToken(existingUser.id);
  const refreshToken = generateRefreshToken(existingUser.id);

  // Decode the refresh token to extract the JTI (unique token ID)
  // JTI is used to detect refresh token reuse attacks
  const data = jwt.decode(refreshToken) as { jti: string };

  // Store JTI in Redis keyed by userId + deviceId
  // This allows one active session per device per user
  await redis.set(
    `refresh:${existingUser.id}:${deviceId}`,
    data.jti,
    "EX",
    config.REFRESH_TOKEN_EXP_SEC,
  );

  // Cache the user object (without password) for fast lookups
  const { password: _password, ...safeUser } = existingUser;
  await redis.set(
    `user:${existingUser.id}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );

  return { accessToken, refreshToken, loggedInUser: safeUser };
};

/**
 * Rotates the refresh token using a JTI check to prevent reuse attacks.
 * If the JTI doesn't match what's stored in Redis, the session is invalidated
 * (token reuse detected — possible theft).
 */
const rotateRefreshToken = async ({
  refreshToken,
  deviceId,
}: {
  refreshToken: string;
  deviceId: string;
}) => {
  // Verify the JWT signature — throws if expired or tampered
  const payload = verifyRefreshToken(refreshToken);
  const { id: userId, jti } = payload;

  // Look up the stored JTI for this user + device
  const storedJti = await redis.get(`refresh:${userId}:${deviceId}`);
  if (!storedJti) {
    throw new ForbiddenError("Session expired", "LOGIN_AGAIN");
  }

  // If JTIs don't match, the token was already rotated — possible reuse attack
  // Invalidate the session entirely as a security measure
  if (storedJti !== jti) {
    await redis.del(`refresh:${userId}:${deviceId}`);
    throw new ForbiddenError("Refresh token reused", "LOGIN_AGAIN");
  }

  // Issue new tokens
  const newAccessToken = generateAccessToken(payload.id);
  const newRefreshToken = generateRefreshToken(payload.id);

  // Decode and store the new JTI, replacing the old one
  const response = jwt.decode(newRefreshToken) as { jti: string };
  await redis.set(
    `refresh:${payload.id}:${deviceId}`,
    response.jti,
    "EX",
    config.REFRESH_TOKEN_EXP_SEC,
  );

  return { newAccessToken, newRefreshToken };
};

export const authservice = { sendOtp, verifyOtp, login, rotateRefreshToken };
