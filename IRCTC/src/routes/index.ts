import { Router } from "express";
import { askAi } from "../controllers";

const router = Router();

router.post("/", askAi);

export default router;
