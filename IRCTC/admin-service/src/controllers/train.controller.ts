import { Request, Response, NextFunction } from "express";
import { zRoute, zTrain } from "../types/zod";
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
/**
 * POST /trains/route
 *
 * Defines the stop-by-stop route for an existing train. Expects a JSON
 * body matching `zRoute`: { trainId, stations[] }, where each station is
 * { stationId, sequenceNumber, arrivalTime?, departureTime?,
 * distanceFromOrigin? } (see zRouteStation). A train can only have one
 * route (`Route.trainId` is unique in the schema).
 *
 * Flow (see trainService.createRoute for the actual checks):
 *  1. Validate the body with zRoute — on failure, respond 400.
 *  2. Defensive re-check that at least one station was supplied (zRoute's
 *     own `.min(2, ...)` on `stations` already guarantees at least two, so
 *     this branch is unreachable; the message below still says "2
 *     stations" even though the check itself is `=== 0`).
 *  3. Await trainService.createRoute, which validates the train and
 *     station ids exist and that sequence numbers are contiguous from 1,
 *     then creates the route + its stations in one transaction.
 *  4. Respond 200.
 */
const createRoute = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zRoute schema
    const result = zRoute.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { stations, trainId } = result.data;
    // Redundant with zRoute's own `.min(2, ...)` on `stations`, kept as a defensive check
    if (stations.length === 0) {
      throw new BadRequestError("A route must have at least 2 stations");
    }

    await trainService.createRoute({
      stations,
      trainId,
    });

    res
      .status(200)
      .json({ success: true, message: "Route created successfully" });
  },
);
/**
 * POST /trains/route/:id
 *
 * Fetches a single train by id, including its seats (ordered by
 * seatNumber) and its route (ordered by sequenceNumber, each stop
 * including the full station record).
 *
 * Two bugs make this endpoint unusable as currently routed
 * (see `train.routes.ts`):
 *  - it's mounted as `POST`, not `GET`, despite being a pure read;
 *  - the route param is named `:id`, but this handler reads
 *    `req.params.trainId` — always `undefined` — so every request to
 *    this path 400s with "Train Id is missing" before `trainService
 *    .getTrainById` is ever called.
 */
const getTrainById = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const { trainId } = req.params;
    if (!trainId) {
      throw new BadRequestError("Train Id is missing");
    }
    const train = await trainService.getTrainById(trainId as string);
    return res.status(200).json({
      success: true,
      data: train,
    });
  },
);
export const trainController = { getTrainById, createTrain, createRoute };
