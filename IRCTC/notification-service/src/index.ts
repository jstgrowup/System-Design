import app from "./server";
import { startNotificationService } from "./config/db";
import dotenv from "dotenv";
import { config } from "./config/config";

dotenv.config();
startNotificationService().then(() => {
  return app.listen(config.PORT, () => {
    console.log(`Server running on port ${config.PORT}`);
  });
});
