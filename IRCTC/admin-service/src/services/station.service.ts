import prisma from "../config/prisma";
import { ConflictError } from "../utils/error";
import logger from "../config/logger";
import { StationBodyType, TrainBodyType } from "../types/zod";
import adminProducer from "../kafka/producer/admin.producer";

/**
 * Creates a new station.
 *
 * Steps:
 *  1. Look up an existing station by `code` (the unique identifier) and
 *     throw ConflictError if one is found, so callers get a clean 409
 *     instead of a raw Prisma unique-constraint error.
 *  2. Insert the station row.
 *  3. Publish a STATION_CREATED event on Kafka so other services (e.g.
 *     route/schedule management) can react to the new station.
 *
 * Returns the created station record as returned by Prisma.
 */
const createStation = async ({ code, name, city, state }: StationBodyType) => {
  // Station code is the unique identifier — reject duplicates before hitting the DB constraint
  const existingStation = await prisma.station.findUnique({ where: { code } });
  if (existingStation) {
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
  // Unlike trainService.createTrain, this publish isn't wrapped in a .catch —
  // a Kafka failure here throws and would normally turn an already-committed
  // station creation into a 500 response from the controller. In practice
  // it can't even do that today: station.controller.ts's createStation
  // never awaits this function, so the rejection instead becomes an
  // unhandled promise rejection.
  await adminProducer.publishStationCreated(createdStation);
  return createdStation;
};

export const stationService = { createStation };
