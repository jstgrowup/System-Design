import crypto from "crypto";
import jwt from "jsonwebtoken";
import { config } from "../config";
import type { StringValue } from "ms";

interface JwtPayload {
  id: string;
}

interface RefreshTokenPayload extends JwtPayload {
  jti: string; // unique token ID used for reuse detection
}

/** Creates a SHA-256 hash of a token — used for safe storage/comparison */
export const hashToken = (refreshToken: string): string => {
  return crypto.createHash("sha256").update(refreshToken).digest("hex");
};

/** Signs a short-lived access token (15m) with just the user ID */
export const generateAccessToken = (userId: string): string => {
  const payload: JwtPayload = { id: userId };
  return jwt.sign(payload, config.JWT_ACCESS_SECRET, {
    expiresIn: config.ACCESS_TOKEN_EXP as StringValue,
  });
};

/** Signs a long-lived refresh token (7d) with user ID + unique JTI */
export const generateRefreshToken = (userId: string): string => {
  const payload: RefreshTokenPayload = {
    id: userId,
    jti: crypto.randomUUID(), // unique per token issuance
  };
  return jwt.sign(payload, config.JWT_REFRESH_SECRET, {
    expiresIn: config.REFRESH_TOKEN_EXP as StringValue,
  });
};

/** Verifies an access token — throws if expired or tampered */
export const verifyAccessToken = (accessToken: string): JwtPayload => {
  return jwt.verify(accessToken, config.JWT_ACCESS_SECRET) as JwtPayload;
};

/** Verifies a refresh token — throws if expired or tampered */
export const verifyRefreshToken = (
  refreshToken: string,
): RefreshTokenPayload => {
  return jwt.verify(
    refreshToken,
    config.JWT_REFRESH_SECRET,
  ) as RefreshTokenPayload;
};
