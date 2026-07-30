import prisma from "../config/prisma";
import { NotFoundError } from "../utils/error";
import { redis } from "../config/redis";
import { config } from "../config";
import logger from "../config/logger";

/**
 * Reads a user's profile, cache-first. On a cache hit, returns the
 * already-scrubbed cached copy. On a miss, reads from Postgres, strips the
 * password hash, caches the scrubbed copy, and returns that same scrubbed
 * copy — not the raw row that was just fetched.
 */
const getUserProfile = async (userId: string) => {
  const storedUser = await redis.get(`user:${userId}`);
  if (storedUser) {
    return JSON.parse(storedUser);
  }
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
  return safeUser;
};

/**
 * Updates the caller's own editable profile fields (name only — email and
 * password go through their own dedicated, verification-gated flows, not
 * this endpoint) and refreshes the cached copy.
 */
const updateProfile = async (
  userId: string,
  updates: { firstName?: string; lastName?: string },
) => {
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  const updatedUser = await prisma.user.update({
    where: { id: userId },
    data: updates,
  });

  const { password: _password, ...safeUser } = updatedUser;
  await redis.set(
    `user:${userId}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );
  return safeUser;
};

/**
 * Deletes the caller's own account and clears their cached profile.
 * Doesn't revoke outstanding refresh-token sessions on other devices — there's
 * no registry of a user's active device sessions to enumerate and clear them
 * from here (each `refresh:<userId>:<deviceId>` key is keyed by a device
 * fingerprint the server never stores a list of).
 */
const deleteProfile = async (userId: string): Promise<void> => {
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  await prisma.user.delete({ where: { id: userId } });
  await redis.del(`user:${userId}`);
};

export const userService = {
  getUserProfile,
  updateProfile,
  deleteProfile,
};
