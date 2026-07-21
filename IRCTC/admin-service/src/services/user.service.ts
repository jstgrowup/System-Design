import prisma from "../config/prisma";
import { NotFoundError } from "../utils/error";
import { redis } from "../config/redis";
import { config } from "../config";
import logger from "../config/logger";

/**
 * Handles the first step of registration.
 * Checks for duplicate email, hashes the password,
 * generates an OTP, stores it in Redis, and sends it via email.
 */
const getUserProfile = async (userId: string) => {
  const storedUser = await redis.get(`user:${userId}`);
  if (storedUser) {
    return JSON.parse(storedUser);
  }
  // Prevent duplicate registrations
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  const { password: _password, ...safeUser } = existingUser;
  logger.info("Stored user profile in redis for the future");
  await redis.set(
    `user:${userId}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );
  return existingUser;
};

export const userService = {
  getUserProfile,
};
