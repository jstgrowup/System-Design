import { Request, Response, NextFunction } from "express";
import { authservice } from "../services/auth.service";
import { zLogin, zSendOtp, zVerifyOtp } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { config } from "../config";
import asyncHandler from "../utils/asyncHandler";
import { BadRequestError, UnauthorizedError } from "../utils/error";
import getDeviceFingerprint from "../utils/device-fingerprint";

/**
 * POST /send-otp
 * Validates the registration payload, triggers OTP generation + email,
 * and stores the OTP session ID in an httpOnly cookie.
 */
const createStation = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zSendOtp schema
    const result = zSendOtp.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { firstName, lastName, email, password } = result.data;

    // Delegate to service: checks for existing user, hashes password,
    // generates OTP, stores it in Redis, and sends the email
    const otpSessionId = await authservice.sendOtp({
      firstName,
      lastName: lastName ?? "",
      email,
      password,
    });

    // Store the OTP session ID in a secure httpOnly cookie
    // so the client can reference it during /verify-otp without exposing it
    res
      .cookie("otp_session", otpSessionId, {
        httpOnly: true, // not accessible via JS
        secure: true, // HTTPS only
        sameSite: "strict",
        maxAge: config.OTP_TTL * 1000, // convert seconds to ms
      })
      .status(200)
      .json({ success: true, message: "OTP sent successfully" });
  },
);

export const authController = { sendOtp };
