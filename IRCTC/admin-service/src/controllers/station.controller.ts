import { Request, Response, NextFunction } from "express";
import { stationService } from "../services/station.service";
import { zStation } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

import asyncHandler from "../utils/asyncHandler";

/**
 * POST /stations/station
 *
 * Creates a new station. Expects a JSON body matching `zStation`:
 * { name, code, city, state? }.
 *
 * Flow:
 *  1. Validate the body with zStation (this also trims strings and
 *     uppercases `code`) — on failure, respond 400 with the first Zod
 *     issue message.
 *  2. Hand the parsed fields to stationService.createStation, which checks
 *     for a duplicate station code, inserts the row, and publishes a
 *     STATION_CREATED Kafka event.
 *  3. Respond 200.
 *
 * Errors the service throws (e.g. ConflictError on a duplicate code) are
 * normally caught by asyncHandler and forwarded to errorHandler — see the
 * note below on why that doesn't reliably happen here.
 */
const createStation = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zStation schema
    const result = zStation.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { name, code, city, state } = result.data;

    // Note: not awaited. zStation's `code` field already applies
    // `.toUpperCase()`, so this second `.toUpperCase()` is a no-op on an
    // already-uppercased value. More importantly, since the returned
    // promise is neither awaited nor returned, the response below fires
    // before the DB write / Kafka publish settle, and a rejection here
    // (e.g. ConflictError on a duplicate code) becomes an unhandled
    // promise rejection instead of reaching errorHandler.
    const station = stationService.createStation({
      code: code.toUpperCase(),
      name,
      city,
      state,
    });

    // Message text is a holdover from a different (OTP-based) flow.
    res.status(200).json({ success: true, message: "OTP sent successfully" });
  },
);

export const stationController = { createStation };
