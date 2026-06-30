import { Request, Response, NextFunction } from "express";
import { authservice } from "../services/auth.service";
import { zSignUp } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { config } from "../config";
import asyncHandler from "../utils/asyncHandler";
const sendOtp = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zSignUp.safeParse(req.body);

    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: "Validation failed",
        errors: formatZodError(result.error),
      });
    }
    const { firstName, lastName, email, password } = result.data;
    const otpSessionId = await authservice.sendOTP({
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
export const authController = { sendOtp };
