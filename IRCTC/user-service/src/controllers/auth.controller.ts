import { Request, Response, NextFunction } from "express";
import { authservice } from "../services/auth.service";
import { zLogin, zSendOtp, zVerifyOtp } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { config } from "../config";
import asyncHandler from "../utils/asyncHandler";
import { BadRequestError, UnauthorizedError } from "../utils/error";
import getDeviceFingerprint from "../utils/device-fingerprint";
const sendOtp = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zSendOtp.safeParse(req.body);

    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }
    const { firstName, lastName, email, password } = result.data;
    const otpSessionId = await authservice.sendOtp({
      firstName,
      lastName: lastName ?? "",
      email,
      password,
    });
    res
      .cookie("otp_session", otpSessionId, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        maxAge: config.OTP_TTL * 1000,
      })
      .status(200)
      .json({ success: true, message: "OTP sent successfully" });
  },
);
const verifyOtp = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zVerifyOtp.safeParse(req.body);

    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }
    const { otp } = result.data;
    const otpSessionId = req.cookies.otp_session;
    if (!otpSessionId) {
      throw new BadRequestError("OTPsession is missing ");
    }
    const user = await authservice.verifyOtp({ otp, otpSessionId });
    return res.status(201).json({ message: "Account is created", data: user });
  },
);
const login = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    console.log("req.body:", req.body);
    const result = zLogin.safeParse(req.body);

    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }
    const deviceId = getDeviceFingerprint(req);
    const { accessToken, refreshToken, loggedInUser } = await authservice.login(
      { email: result.data.email, password: result.data.password, deviceId },
    );
    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: config.ACCESS_TOKEN_EXP_SEC * 1000,
    });
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
        message: "Logged In successfully",
        data: loggedInUser,
      });
  },
);
const rotateRefreshToken = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    console.log("req.body:", req.body);
    const refreshToken = req.cookies.refreshToken;
    if (!refreshToken) {
      throw new UnauthorizedError("Refresh token is missing", "LoGIN AGAIN");
    }
    const deviceId = getDeviceFingerprint(req);
    const { newAccessToken, newRefreshToken } =
      await authservice.rotateRefreshToken({ refreshToken, deviceId });
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
        message: "Access and Refresh token reissued",
      });
  },
);
export const authController = { sendOtp, verifyOtp, login, rotateRefreshToken };
