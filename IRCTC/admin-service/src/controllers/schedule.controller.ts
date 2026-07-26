import { Request, Response, NextFunction } from "express";
import { zSchedule } from "../types/zod";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import asyncHandler from "../utils/asyncHandler";
import { scheduleService } from "../services/schedule.service";

/**
 * POST /schedule (defined in schedule.route.ts — see that file for why
 * this endpoint can't actually be reached today)
 *
 * Creates a schedule (a specific departureDate run) for an existing train
 * that already has a route defined. Expects a JSON body matching
 * `zSchedule`: { trainId, departureDate, status? }.
 *
 * Flow:
 *  1. Validate the body with zSchedule — on failure, respond 400 with the
 *     first Zod issue message.
 *  2. Await scheduleService.createSchedule, which checks the train exists
 *     and has a route, rejects a duplicate (trainId, departureDate) pair,
 *     creates the schedule row, and publishes a denormalized
 *     SCHEDULE_CREATED Kafka event (train + seats + route inlined) for
 *     inventory-service and search-service.
 *  3. Respond 200. The success message below ("Train created
 *     successfully") is a copy-paste leftover from train.controller.ts —
 *     it describes the wrong resource.
 */
const createSchedule = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zSchedule schema
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
