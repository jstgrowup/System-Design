import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { config } from "./config";
import logger from "./config/logger";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import authRoutes from "./routes/auth.route";
import userRoutes from "./routes/user.route";
const app = express();

app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());
// Mounted at plain /auth (no version prefix) to match every other service's
// convention in this repo — the gateway's proxy strips exactly one path
// segment and forwards the rest verbatim, so a versioned prefix here can
// never be reproduced by that generic rewrite rule. This used to be
// /api/v1/auth, which is why every login attempt through the gateway 404'd.
app.use("/auth", authRoutes);
app.use("/user", userRoutes);
app.get("/", (req, res) => {
  res.send("Hello from user-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    message: "ok",
  });
});
app.use(errorHandler);

export default app;
