import express from "express";
import askAiRoute from "./routes";
import { errorHandler } from "./middlewares/errorHandler";

const app = express();

app.use(express.json());

// Routes
app.use("/api/ask", askAiRoute);

app.use(errorHandler);
export default app;
