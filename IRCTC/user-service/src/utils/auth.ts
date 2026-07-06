import crypto from "crypto";
import jwt from "jsonwebtoken";
import { config } from "../config";
import type { StringValue } from "ms";
interface JwtPayload {
  id: string;
}

interface RefreshTokenPayload extends JwtPayload {
  jti: string;
}

export const hashToken = (refreshToken: string): string => {
  return crypto.createHash("sha256").update(refreshToken).digest("hex");
};

export const generateAccessToken = (userId: string): string => {
  const payload: JwtPayload = { id: userId };
  return jwt.sign(payload, config.JWT_ACCESS_SECRET, {
    expiresIn: config.ACCESS_TOKEN_EXP as StringValue,
  });
};

export const generateRefreshToken = (userId: string): string => {
  const payload: RefreshTokenPayload = {
    id: userId,
    jti: crypto.randomUUID(),
  };
  return jwt.sign(payload, config.JWT_REFRESH_SECRET, {
    expiresIn: config.REFRESH_TOKEN_EXP as StringValue,
  });
};

export const verifyAccessToken = (accessToken: string): JwtPayload => {
  return jwt.verify(accessToken, config.JWT_ACCESS_SECRET) as JwtPayload;
};

export const verifyRefreshToken = (
  refreshToken: string,
): RefreshTokenPayload => {
  return jwt.verify(
    refreshToken,
    config.JWT_REFRESH_SECRET,
  ) as RefreshTokenPayload;
};
