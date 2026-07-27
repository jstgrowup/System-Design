import prisma from "../config/prisma";
import { BadRequestError, ConflictError } from "../utils/error";
import { ScheduleBodyType } from "../types/zod";
import adminProducer from "../kafka/producer/admin.producer";

const initializeInventory = async () => {
  return "";
};

export const inventoryService = { initializeInventory };
