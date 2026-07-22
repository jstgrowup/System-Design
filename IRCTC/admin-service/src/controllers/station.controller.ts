import { Request, Response, NextFunction } from "express";
import { stationService } from "../services/station.service";
import { zStation } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { config } from "../config";
import asyncHandler from "../utils/asyncHandler";

/**
 * POST /send-otp
 * Validates the registration payload, triggers OTP generation + email,
 * and stores the OTP session ID in an httpOnly cookie.
 */
const createStation = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zSendOtp schema
    const result = zStation.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { name, code, city, state } = result.data;

    const station = stationService.createStation({
      code: code.toUpperCase(),
      name,
      city,
      state,
    });

    // Store the OTP session ID in a secure httpOnly cookie
    // so the client can reference it during /verify-otp without exposing it
    res.status(200).json({ success: true, message: "OTP sent successfully" });
  },
);

export const stationController = { createStation };
