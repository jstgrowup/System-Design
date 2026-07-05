import { Request, Response, NextFunction } from "express";
import { authservice } from "../services/auth.service";
import { zSendOtp, zVerifyOtp } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { config } from "../config";
import asyncHandler from "../utils/asyncHandler";
import { BadRequestError } from "../utils/error";
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
    return res.status(201).json({ message: "Account is created" });
  },
);
export const authController = { sendOtp, verifyOtp };
