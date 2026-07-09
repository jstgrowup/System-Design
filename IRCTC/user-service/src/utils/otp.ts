import { config } from "../config";
import { redis } from "../config/redis";
import { TooManyRequestsError } from "./error";
import otpGenerator from "otp-generator";
import crypto from "node:crypto";

const HMAC_SECRET = config.OTP_HMAC_SECRET;

// Max OTP requests allowed per email per hour
const RATE_MAX = Number(config.OTP_RATE_MAX_PER_HOUR || "10");

/**
 * Creates an HMAC-SHA256 signature of the email + OTP combination.
 * Used to securely store and verify OTPs without saving them in plaintext.
 */
const hmacFor = ({ email, otp }: { email: string; otp: string }) => {
  return crypto
    .createHmac("sha256", HMAC_SECRET)
    .update(email + ":" + otp)
    .digest("hex");
};

/**
 * Generates a 6-digit numeric OTP, rate-limits by email,
 * stores the HMAC of the OTP + user meta in Redis, and returns
 * both the plaintext OTP (to send via email) and the session ID (for the cookie).
 */
export const generateAndStoreOtp = async (meta: {
  firstName: string;
  lastName: string;
  email: string;
  password: string; // already bcrypt hashed at this point
}) => {
  // Check how many OTPs this email has requested in the last hour
  const rateKey = `otp:rate:${meta.email}`;
  const sentCount = parseInt((await redis.get(rateKey)) || "0", 10);
  if (sentCount >= RATE_MAX) {
    throw new TooManyRequestsError(
      "Too many OTP requests. Try again later",
      "OTP_RATE_LIMIT",
    );
  }

  // Generate a 6-digit numeric OTP
  const otp = otpGenerator.generate(6, {
    upperCaseAlphabets: false,
    lowerCaseAlphabets: false,
    specialChars: false,
  });

  // Unique session ID tied to this OTP attempt — set as a cookie
  const otpSessionId = crypto.randomUUID();

  // HMAC the OTP so we never store the plaintext OTP in Redis
  const hashed = hmacFor({ email: meta.email, otp });

  // Store the hashed OTP + user meta in Redis with a TTL
  await redis.set(
    `otp:session:${otpSessionId}`,
    JSON.stringify({ hashedOtp: hashed, meta }),
    "EX",
    config.OTP_TTL,
  );

  // Increment the rate limit counter and set/refresh the 1-hour window
  await redis.incr(rateKey);
  await redis.expire(rateKey, 3600);

  return { otp, otpSessionId };
};

/**
 * Verifies a submitted OTP against the HMAC stored in Redis.
 * Tracks failed attempts and blocks after exceeding the max.
 * Deletes the session from Redis on success to prevent reuse.
 *
 * NOTE: There is a typo bug here — `hasedOtp` should be `hashedOtp`.
 * This will cause verification to always fail. Fix: change `hasedOtp` → `hashedOtp`.
 */
export const verifyOtpViaUnHashing = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  // Retrieve the stored session data from Redis
  const rawData = await redis.get(`otp:session:${otpSessionId}`);
  if (!rawData) return null; // session expired or never existed

  // ⚠️ BUG: `hasedOtp` is a typo — should be `hashedOtp` to match what was stored
  const { hashedOtp: storedOtp, meta } = JSON.parse(rawData);

  // Track failed verification attempts to prevent brute force
  const attemptsKey = `otp:attempt:${meta.email}`;
  const attemptsCount = parseInt((await redis.get(attemptsKey)) || "0", 10);
  if (attemptsCount >= config.OTP_MAX_VERIFY_ATTEMPTS) {
    throw new TooManyRequestsError("Too many attempts to verify OTP");
  }

  // HMAC the submitted OTP using the same secret + email
  const hashedOtp = hmacFor({ email: meta.email, otp });

  // Use timing-safe comparison to prevent timing attacks
  if (
    crypto.timingSafeEqual(
      Buffer.from(hashedOtp, "hex"),
      Buffer.from(storedOtp, "hex"),
    )
  ) {
    // OTP is valid — clean up all related Redis keys
    await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
    await redis.del(`otp:rate:${meta.email}`);
    return meta;
  } else {
    // OTP is wrong — increment attempt counter with TTL
    await redis.incr(attemptsKey);
    await redis.expire(attemptsKey, config.OTP_TTL);
    return null;
  }
};
