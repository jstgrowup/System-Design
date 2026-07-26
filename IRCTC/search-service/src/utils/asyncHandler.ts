import { Request, Response, NextFunction } from "express";

/**
 * Wraps an async route/controller function and forwards errors to next().
 * Usage: router.get('/', asyncHandler(myAsyncController));
 */
export default function asyncHandler<
  Req extends Request = Request,
  Res extends Response = Response,
>(fn: (req: Req, res: Res, next: NextFunction) => Promise<any> | any) {
  return (req: Req, res: Res, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}
