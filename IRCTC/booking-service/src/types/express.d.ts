// Augments Express's Request type with the `user` field that
// getUserContext attaches after reading the gateway's x-user-id header.
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
