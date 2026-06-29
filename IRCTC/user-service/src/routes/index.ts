import { Router } from "express";
import { askAi } from "../controllers/auth.controller";

const router = Router();

router.post("/", askAi);

export default router;
