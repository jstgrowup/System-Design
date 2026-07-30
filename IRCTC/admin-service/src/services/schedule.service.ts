import prisma from "../config/prisma";
import { BadRequestError, ConflictError } from "../utils/error";
import { ScheduleBodyType } from "../types/zod";
import adminProducer from "../kafka/producer/admin.producer";

/**
 * Creates a schedule — a specific departureDate run of an existing train
 * that already has a route defined.
 *
 * Steps:
 *  1. Look up the train by id (with its seats and full route+stations
 *     included), 409/400 if it doesn't exist, has no seats, or has no
 *     route yet — a schedule can't be built without those.
 *  2. Parse and validate `departureDate`.
 *  3. Reject a duplicate schedule for the same (trainId, departureDate)
 *     pair (`Schedule` has a compound unique constraint on those two
 *     columns).
 *  4. Create the schedule row.
 *  5. Build a denormalized event payload — train info, the full seat map,
 *     and the full route with station details all inlined — so that
 *     inventory-service and search-service don't need to call back into
 *     admin-service just to react to a new schedule.
 *  6. Publish it as a SCHEDULE_CREATED Kafka event.
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
