import prisma from "../config/prisma";
import { BadRequestError, ConflictError } from "../utils/error";
import logger from "../config/logger";
import { TrainBodyType } from "../types/zod";
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
const createTrain = async ({
  trainName,
  trainNumber,
  coachName,
  seats,
}: TrainBodyType) => {
  // Train number is the unique identifier — reject duplicates before hitting the DB constraint
  const existing = await prisma.train.findUnique({ where: { trainNumber } });
  if (existing) {
    throw new ConflictError("Train with this number already exists");
  }
  // Guard against two seats in the same payload sharing a seat number
  const seatNumbers = seats.map((s) => s.seatNumber);
  if (new Set(seatNumbers).size !== seatNumbers.length) {
    throw new BadRequestError("Duplicate seat numbers found");
  }
  const train = await prisma.train.create({
    data: {
      trainNumber,
      trainName,
      // Defaults to "AC" when the client omits coachName (zTrain marks it optional)
      coachName: coachName || "AC",
      totalSeats: seats.length,
      // Nested write — train and all of its seats are created in one transaction
      seats: {
        create: seats.map((seat) => ({
          seatNumber: seat.seatNumber,
          seatType: seat.seatType,
          price: seat.price,
        })),
      },
    },
    include: { seats: { orderBy: { seatNumber: "asc" } } },
  });
  // Unlike stationService.createStation, a publish failure here is caught and
  // logged rather than thrown — the train is already committed, so a Kafka
  // outage shouldn't turn a successful creation into a 500.
  await adminProducer.publishTrainCreated(train).catch((err) => {
    logger.error("Failed to publish train created event", {
      error: err.message,
    });
  });

  return train;
};
export const trainService = { createTrain };
