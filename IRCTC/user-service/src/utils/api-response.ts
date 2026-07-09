import { Response } from "express";
export type ApiResponseType = {
  message: string;
  data?: Record<string, unknown>;
};

export const SuccessResponse = (
  res: Response,
  statusCode: number,
  params: ApiResponseType,
) => {
  return res.status(statusCode).json({
    success: true,
    message: params.message,
    data: params?.data ?? {},
  });
};

export const ErrorResponse = (
  res: Response,
  statusCode: number,
  params: ApiResponseType,
) => {
  return res.status(statusCode).json({
    success: false,
    message: params.message,
  });
};
