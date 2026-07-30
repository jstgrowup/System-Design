// Augments Express's Request type with the `user` field that
// getUserContext/internalAuth attach after reading the gateway's
// x-user-id header (or accepting an internal-service call).
declare global {
  namespace Express {
    interface Request {
      user?: {
        id: string;
      };
    }
  }
}

export {};
