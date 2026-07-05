import { config } from "../config";
import { redis } from "../config/redis";
import { TooManyRequestsError } from "./error";
import otpGenerator from "otp-generator";
import crypto from "node:crypto";
const HMAC_SECRET = config.OTP_HMAC_SECRET;
const RATE_MAX = Number(config.OTP_RATE_MAX_PER_HOUR || "10");
const hmacFor = ({ email, otp }: { email: string; otp: string }) => {
  return crypto
    .createHmac("sha256", HMAC_SECRET)
    .update(email + ":" + otp)
    .digest("hex");
};
export const generateAndStoreOtp = async (meta: {
  firstName: string;
  lastName: string;
  email: string;
  password: string;
}) => {
  const rateKey = `otp:rate:${meta.email}`;
  const sentCount = parseInt((await redis.get(rateKey)) || "0", 10);
  if (sentCount >= RATE_MAX) {
    throw new TooManyRequestsError(
      "Too many OTP requests. Try again later",
      "OTP_RATE_LIMIT",
    );
  }
  const otp = otpGenerator.generate(6, {
    upperCaseAlphabets: false,
    lowerCaseAlphabets: false,
    specialChars: false,
  });
  const otpSessionId = crypto.randomUUID();
  const hashed = hmacFor({ email: meta.email, otp });
  await redis.set(
    `otp:session:${otpSessionId}`,
    JSON.stringify({ hashedOtp: hashed, meta }),
    "EX",
    config.OTP_TTL,
  );
  await redis.incr(rateKey);
  await redis.expire(rateKey, 3600);
  return { otp, otpSessionId };
};
export const verifyOtpViaUnHashing = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  const rawData = await redis.get(`otp:session:${otpSessionId}`);
  if (!rawData) return null;
  const { hasedOtp: storedOtp, meta } = JSON.parse(rawData);
  const attemptsKey = `otp:attempt:${meta.email}`;
  const attemptsCount = parseInt((await redis.get(attemptsKey)) || "0", 10);
  if (attemptsCount >= config.OTP_MAX_VERIFY_ATTEMPTS) {
    throw new TooManyRequestsError("Too Many attepts to verify OTP");
  }
  const hashedOtp = hmacFor({ email: meta.email, otp });
  if (
    crypto.timingSafeEqual(
      Buffer.from(hashedOtp, "hex"),
      Buffer.from(storedOtp, "hex"),
    )
  ) {
    await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
    await redis.del(`otp:rate:${meta.email}`);
    return meta;
  } else {
    await redis.incr(attemptsKey);
    await redis.expire(attemptsKey, config.OTP_TTL);
    return null;
  }
};
