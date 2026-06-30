import app from "./server";
import { config } from "./config";
import connectDB from "./config/db";
import dotenv from "dotenv";

dotenv.config();
connectDB().then(() => {
  return app.listen(config.PORT, () => {
    console.log(`Server running on port ${config.PORT}`);
  });
});
