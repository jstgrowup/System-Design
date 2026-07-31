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

    // zStation's `code` field already applies `.toUpperCase()` via zod, so
    // this call passes it straight through rather than re-uppercasing.
    const station = await stationService.createStation({
      code,
      name,
      city,
      state,
    });

    res.status(200).json({
      success: true,
      message: "Station created successfully",
      data: station,
    });
  },
);

/**
 * GET /stations/station/internal/:stationId
 *
 * Internal-only (behind internalAuth's shared-secret header) — resolves a
 * station by id for another backend service. booking-service uses this to
 * attach a station's name to a booking-confirmed email.
 */
const getStationByIdInternal = asyncHandler(
  async (req: Request<{ stationId: string }>, res: Response) => {
    const { stationId } = req.params;
    const station = await stationService.getStationById(stationId);
    res.status(200).json({ success: true, data: station });
  },
);

export const stationController = { createStation, getStationByIdInternal };
