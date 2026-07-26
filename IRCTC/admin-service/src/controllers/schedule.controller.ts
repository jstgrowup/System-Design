import { Request, Response, NextFunction } from "express";
import { zRoute, zSchedule, zTrain } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

import asyncHandler from "../utils/asyncHandler";
import { BadRequestError } from "../utils/error";
import { trainService } from "../services/train.service";
import { scheduleService } from "../services/schedule.service";

/**
 * POST /trains/train
 *
 * Creates a new train together with its full seat map. Expects a JSON
 * body matching `zTrain`: { trainNumber, trainName, coachName?, seats[] },
 * where each seat is { seatNumber, seatType, price } (see zSeat).
 *
 * Flow:
 *  1. Validate the body with zTrain — on failure, respond 400 with the
 *     first Zod issue message.
 *  2. Defensive re-check that at least one seat was supplied (zTrain's own
 *     `.min(1, ...)` on `seats` already guarantees this, so in practice
 *     this branch is unreachable).
 *  3. Await trainService.createTrain, which checks for a duplicate train
 *     number, rejects duplicate seat numbers within the payload, creates
 *     the train + seats in one transaction, and publishes a
 *     TRAIN_CREATED Kafka event (publish failures there are logged, not
 *     thrown).
 *  4. Respond 200.
 */
const createSchedule = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zTrain schema
    const result = zSchedule.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainId, departureDate } = result.data;
    await scheduleService.createSchedule({ trainId, departureDate });
    return res
      .status(200)
      .json({ success: true, message: "Train created successfully" });
  },
);

export const scheduleController = { createSchedule };
