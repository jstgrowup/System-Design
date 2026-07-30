import { Request, Response } from "express";
import asyncHandler from "../utils/asyncHandler";
import { BadRequestError } from "../utils/error";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";
import { zUpdateProfile } from "../types/zod";

import { userService } from "../services/user.service";

/**
 * POST /user/profile
 * Reachable only behind getUserContext, which sets req.user or rejects the
 * request first — the `if (!userId) throw` below is a defensive fallback,
 * not the primary guard.
 */
const getProfile = asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user?.id;
  if (!userId) {
    throw new BadRequestError("user Id is missing ");
  }
  const user = await userService.getUserProfile(userId);
  res.status(200).json({ data: user, success: true });
});

/**
 * PUT /user/profile
 * Updates the caller's own firstName/lastName. Email and password are
 * intentionally not editable here — those need their own verification-gated
 * flows, not a plain profile update.
 */
const updateProfile = asyncHandler(
  async (req: Request, res: Response) => {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    const result = zUpdateProfile.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const updatedUser = await userService.updateProfile(userId, result.data);
    res.status(200).json({ data: updatedUser, success: true });
  },
);

/**
 * DELETE /user/profile
 * Deletes the caller's own account.
 */
const deleteProfile = asyncHandler(
  async (req: Request, res: Response) => {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    await userService.deleteProfile(userId);
    res
      .status(200)
      .json({ success: true, message: "Account deleted successfully" });
  },
);

/**
 * GET /user/internal/:userId
 * Internal only (behind internalAuth's shared-secret check, not a JWT) — for
 * other services (e.g. booking-service resolving the user on a booking) to
 * read a user's profile without going through the gateway's auth flow.
 */
const getUserByIdInternal = asyncHandler(
  async (req: Request<{ userId: string }>, res: Response) => {
    const { userId } = req.params;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    const user = await userService.getUserProfile(userId);
    res.status(200).json({ data: user, success: true });
  },
);

export const userController = {
  getProfile,
  updateProfile,
  deleteProfile,
  getUserByIdInternal,
};
