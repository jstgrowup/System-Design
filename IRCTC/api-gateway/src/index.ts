import app from "./server";
import dotenv from "dotenv";
import { config } from "./config";

dotenv.config();
app.listen(config.PORT, () => {
  console.log(`Server running on port ${config.PORT}`);
});
