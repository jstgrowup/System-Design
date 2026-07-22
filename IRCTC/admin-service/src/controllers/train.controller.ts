import { Request, Response, NextFunction } from "express";
import { stationService } from "../services/station.service";
import { zStation, zTrain } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

import asyncHandler from "../utils/asyncHandler";
import { BadRequestError } from "../utils/error";

/**
 * POST /send-otp
 * Validates the registration payload, triggers OTP generation + email,
 * and stores the OTP session ID in an httpOnly cookie.
 */
const createTrain = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zSendOtp schema
    const result = zTrain.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainName, trainNumber, coachName, totalSeats } = result.data;
    if (totalSeats === 0) {
      throw new BadRequestError("Atleast one seat must be defined");
    }

    const train = stationService.createTrain({
      trainName,
      trainNumber,
      coachName,
      totalSeats,
    });

    // Store the OTP session ID in a secure httpOnly cookie
    // so the client can reference it during /verify-otp without exposing it
    res
      .status(200)
      .json({ success: true, message: "Train created successfully" });
  },
);

export const Controller = { createTrain };
