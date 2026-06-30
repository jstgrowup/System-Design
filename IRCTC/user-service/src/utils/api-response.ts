import { Response } from "express";
export interface ApiResponseType {
  success?: boolean;
  message?: string;
  data?: any;
  errors?: any;
}

export const SuccessResponse = (
  res: Response,
  statusCode: number,
  params: ApiResponseType,
) => {
  return res.status(statusCode).json({
    success: true,
    message: params.message,
    data: params?.data ?? {},
    errors: params?.errors ?? {},
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
    data: params?.data ?? {},
    errors: params?.errors ?? {},
  });
};
