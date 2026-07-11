import { Request, Response, NextFunction } from "express";

export const askAi = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    res.json({});
  } catch (error) {
    next(error);
  }
};
