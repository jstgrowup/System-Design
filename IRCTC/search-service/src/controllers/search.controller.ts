import { Request, Response, NextFunction } from "express";
import { zSearchTrains } from "../types/zod";
import asyncHandler from "../utils/asyncHandler";
import searchService from "../services/search.service";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

/**
 * GET /trains?from=Delhi&to=Mumbai&date=2025-07-15
 *
 * Searches for trains running between two stations, optionally filtered to
 * a specific departure date. Station names/codes are fuzzy-resolved (see
 * searchService.resolveStation).
 */
const searchTrains = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zSearchTrains.safeParse(req.query);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { from, to, date } = result.data;
    const response = await searchService.searchTrains({ from, to, date });

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /autocomplete?q=del
 */
const autoComplete = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const { q } = req.query;

    const response = await searchService.autocompleteStation(q as string);

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /debug/stations — lists every indexed station document, for
 * inspecting the Elasticsearch index directly rather than through the
 * autocomplete/fuzzy-search paths.
 */
const debugStations = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const response = await searchService.getAllStations();

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /debug/trains — lists every indexed train document.
 */
const debugTrains = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const response = await searchService.getAllTrains();

    return res.status(200).json({ success: true, data: response });
  },
);

export const searchController = {
  searchTrains,
  autoComplete,
  debugStations,
  debugTrains,
};
