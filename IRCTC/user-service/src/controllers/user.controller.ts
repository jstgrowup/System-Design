import { Response } from "express";
import asyncHandler from "../utils/asyncHandler";
import { BadRequestError, UnauthorizedError } from "../utils/error";

import { userService } from "../services/user.service";
import { AuthenticatedRequest } from "../../../shared/types";
const getProfile = asyncHandler(async (req: any, res: Response) => {
  const userId = req.user?.id;
  if (!userId) {
    throw new BadRequestError("user Id is missing ");
  }
  const user = await userService.getUserProfile(userId);
  res.status(200).json({ data: user, success: true });
});

const updateProfile = asyncHandler(async (req, res) => {
  // TODO TASK FOR YOU
});

const deleteProfile = asyncHandler(async (req, res) => {
  // TODO TASK FOR YOU
});
export const userController = {
  getProfile,
  updateProfile,
  deleteProfile,
};
