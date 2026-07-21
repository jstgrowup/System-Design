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
const sendOtp = asyncHandler(
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

/**
 * POST /verify-otp
 * Reads the OTP session from the cookie, verifies the submitted OTP,
 * and creates the user account if valid.
 */
const verifyOtp = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate the OTP format (6 digits)
    const result = zVerifyOtp.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { otp } = result.data;

    // Retrieve the OTP session ID set during /send-otp
    const otpSessionId = req.cookies.otp_session;
    if (!otpSessionId) {
      throw new BadRequestError("OTP session is missing");
    }

    // Verify the OTP against the HMAC stored in Redis
    // and create the user in the database if valid
    const user = await authservice.verifyOtp({ otp, otpSessionId });

    return res.status(201).json({ message: "Account is created", data: user });
  },
);

/**
 * POST /login
 * Validates credentials, generates access + refresh tokens,
 * stores the refresh token JTI in Redis, and sets both as cookies.
 */
const login = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate email + password
    const result = zLogin.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    // Generate a device fingerprint from user-agent + IP + accept headers
    // Used to scope the refresh token to a specific device
    const deviceId = getDeviceFingerprint(req);

    const { accessToken, refreshToken, loggedInUser } = await authservice.login(
      { email: result.data.email, password: result.data.password, deviceId },
    );

    // Set access token cookie (short-lived: 15 minutes)
    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: config.ACCESS_TOKEN_EXP_SEC * 1000,
    });

    // Set refresh token cookie (long-lived: 7 days)
    return res
      .cookie("refreshToken", refreshToken, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        maxAge: config.REFRESH_TOKEN_EXP_SEC * 1000,
      })
      .status(200)
      .json({
        success: true,
        message: "Logged in successfully",
        data: loggedInUser,
      });
  },
);

/**
 * POST /refresh
 * Verifies the existing refresh token, checks its JTI against Redis
 * to detect reuse attacks, then rotates both tokens.
 */
const rotateRefreshToken = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const refreshToken = req.cookies.refreshToken;
    if (!refreshToken) {
      throw new UnauthorizedError("Refresh token is missing", "LOGIN_AGAIN");
    }

    // Fingerprint must match the one used at login to tie the session to the device
    const deviceId = getDeviceFingerprint(req);

    const { newAccessToken, newRefreshToken } =
      await authservice.rotateRefreshToken({ refreshToken, deviceId });

    // Reissue both cookies with fresh tokens
    res.cookie("accessToken", newAccessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: config.ACCESS_TOKEN_EXP_SEC * 1000,
    });

    return res
      .cookie("refreshToken", newRefreshToken, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        maxAge: config.REFRESH_TOKEN_EXP_SEC * 1000,
      })
      .status(200)
      .json({
        success: true,
        message: "Access and refresh tokens reissued",
      });
  },
);

export const authController = { sendOtp, verifyOtp, login, rotateRefreshToken };
