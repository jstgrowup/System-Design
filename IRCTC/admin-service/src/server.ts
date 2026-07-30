import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import stationRoutes from "./routes/station.route";
import trainRoutes from "./routes/train.routes";
import scheduleRoutes from "./routes/schedule.route";

const app = express();

// Order matters: security headers and CORS first, then request logging,
// then body/cookie parsing, before any route handlers run.
app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());
app.use("/stations", stationRoutes);
app.use("/trains", trainRoutes);
app.use("/schedules", scheduleRoutes);

app.get("/", (req, res) => {
  res.send("Hello from admin-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    success: true,
    message: "Admin Service is healthy",
    timestamp: new Date().toISOString(),
  });
});

// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
