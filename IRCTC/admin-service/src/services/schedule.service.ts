import prisma from "../config/prisma";
import { BadRequestError, ConflictError, NotFoundError } from "../utils/error";
import logger from "../config/logger";
import { RouteBodyType, ScheduleBodyType, TrainBodyType } from "../types/zod";
import adminProducer from "../kafka/producer/admin.producer";

/**
 * Creates a new train along with its seat map.
 *
 * Steps:
 *  1. Look up an existing train by `trainNumber` (the unique identifier)
 *     and throw ConflictError if one is found, so callers get a clean 409
 *     instead of a raw Prisma unique-constraint error.
 *  2. Reject the payload if two seats share the same `seatNumber`.
 *  3. Create the train row and all seat rows in a single Prisma nested
 *     write (one transaction) — `totalSeats` is derived from the payload
 *     length rather than counted separately after insert.
 *  4. Publish a TRAIN_CREATED event on Kafka; unlike stationService,
 *     failures here are caught and logged rather than re-thrown, so a
 *     Kafka outage doesn't turn a successful train creation into a 500.
 *
 * Returns the created train with its seats, ordered by seatNumber.
 */
const createSchedule = async ({ trainId, departureDate }: ScheduleBodyType) => {
  // Train number is the unique identifier — reject duplicates before hitting the DB constraint
  const existingTrain = await prisma.train.findUnique({
    where: { id: trainId },
    include: {
      seats: true,
      route: {
        include: {
          routeStations: {
            include: { station: true },
          },
        },
      },
    },
  });
  if (!existingTrain) {
    throw new ConflictError("Train not found");
  }
  if (existingTrain.seats.length === 0) {
    throw new BadRequestError("Train not found");
  }
  if (!existingTrain.route) {
    throw new BadRequestError(
      "Train has no route defined. Create a route first ",
    );
  }
  const parsedDate = new Date(departureDate);
  if (isNaN(parsedDate.getTime())) {
    throw new BadRequestError("Invalid departure date format ");
  }
  const existingSchedule = await prisma.schedule.findUnique({
    where: { trainId_departureDate: { trainId, departureDate: parsedDate } },
  });
  if (existingSchedule) {
    throw new ConflictError(
      "Schedule already exists for this train on this date ",
    );
  }
  const schedule = await prisma.schedule.create({
    data: { trainId, departureDate: parsedDate },
  });
  const eventPayload = {
    scheduleId: schedule.id,
    trainId: existingTrain.id,
    trainNumber: existingTrain.trainNumber,
    trainName: existingTrain.trainName,
    coachName: existingTrain.coachName,
    totalSeats: existingTrain.totalSeats,
    departureDate: departureDate,
    status: schedule.status,
    seats: existingTrain.seats.map((s) => ({
      seatId: s.id,
      seatNumber: s.seatNumber,
      seatType: s.seatType,
      price: s.price,
    })),
    route: existingTrain.route.routeStations.map((rs) => ({
      stationId: rs.station.id,
      stationName: rs.station.name,
      stationCode: rs.station.code,
      city: rs.station.city,
      sequenceNumber: rs.sequenceNumber,
      arrivalTime: rs.arrivalTime,
      departureTime: rs.departureTime,
      distanceFromOrigin: rs.distanceFromOrigin,
    })),
  };

  // This event goes to both inventory-service and search-service via Kafka
  await adminProducer.publishScheduleCreated(eventPayload);
  return "";
};

export const scheduleService = { createSchedule };
