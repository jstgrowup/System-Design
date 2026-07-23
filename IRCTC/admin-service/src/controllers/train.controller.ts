import { Request, Response, NextFunction } from "express";
import { zTrain } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

import asyncHandler from "../utils/asyncHandler";
import { BadRequestError } from "../utils/error";
import { trainService } from "../services/train.service";

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
const createTrain = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zTrain schema
    const result = zTrain.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainName, trainNumber, coachName, seats } = result.data;
    // Redundant with zTrain's own `.min(1, ...)` on `seats`, kept as a defensive check
    if (seats.length === 0) {
      throw new BadRequestError("Atleast one seat must be defined");
    }

    await trainService.createTrain({
      trainName,
      trainNumber,
      coachName,
      seats,
    });

    res
      .status(200)
      .json({ success: true, message: "Train created successfully" });
  },
);
const createRoute = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zTrain schema
    const result = zTrain.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainName, trainNumber, coachName, seats } = result.data;
    // Redundant with zTrain's own `.min(1, ...)` on `seats`, kept as a defensive check
    if (seats.length === 0) {
      throw new BadRequestError("Atleast one seat must be defined");
    }

    await trainService.createTrain({
      trainName,
      trainNumber,
      coachName,
      seats,
    });

    res
      .status(200)
      .json({ success: true, message: "Train created successfully" });
  },
);

export const trainController = { createTrain };
