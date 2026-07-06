import crypto from "crypto";
import { Request } from "express";

/**
 * Generates a short device fingerprint from request headers.
 * Combines user-agent, IP address, and accept headers into a
 * SHA-256 hash, returning the first 16 characters as a device ID.
 */
const getDeviceFingerprint = (req: Request): string => {
  const userAgent = req.headers["user-agent"] ?? "";
  const ip = req.ip ?? "";
  const accept = req.headers["accept"] ?? "";

  // Combine identifiers into a single raw string
  const raw = `${userAgent}|${ip}|${accept}`;

  // Hash the raw string and slice to a short 16-char device ID
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
};

export default getDeviceFingerprint;
