import prisma from "../config/prisma";
import { ConflictError } from "../utils/error";
import logger from "../config/logger";
import { StationBodyType, TrainBodyType } from "../types/zod";
import adminProducer from "../kafka/producer/admin.producer";

/**
 * Handles the first step of registration.
 * Checks for duplicate email, hashes the password,
 * generates an OTP, stores it in Redis, and sends it via email.
 */
const createStation = async ({ code, name, city, state }: StationBodyType) => {
  // Prevent duplicate registrations
  const existingUser = await prisma.station.findUnique({ where: { code } });
  if (existingUser) {
    throw new ConflictError("Station already exists");
  }
  const createdStation = await prisma.station.create({
    data: {
      code,
      name,
      city,
      state,
    },
  });
  logger.info("Station Created", { id: createdStation.id });
  // Return session ID to be set as a cookie in the controller
  await adminProducer.publishStationCreated(createdStation);
  return createdStation;
};
const createTrain = async ({
  trainName,
  trainNumber,
  coachName,
  totalSeats,
}: TrainBodyType) => {
  // Prevent duplicate registrations
  const existing = await prisma.train.findUnique({ where: { trainNumber } });
  if (existing) {
    throw new ConflictError("Train with this number already exists");
  }
  const seatNumbers = totalSeats.map;
  return;
};
export const stationService = { createStation, createTrain };
