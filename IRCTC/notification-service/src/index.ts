import app from "./server";
import config from "./config/config";
import connectDB from "./config/db";
import dotenv from "dotenv";

dotenv.config();
connectDB().then(() => {
  return app.listen(config.port, () => {
    console.log(`Server running on port ${config.port}`);
  });
});
