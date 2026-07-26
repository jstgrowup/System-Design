import prisma from "../config/prisma";
import { BadRequestError, ConflictError, NotFoundError } from "../utils/error";
import logger from "../config/logger";
import { RouteBodyType, TrainBodyType } from "../types/zod";
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
/**
 * Defines the route (ordered list of stations) for an existing train.
 *
 * Steps:
 *  1. Look up the train by id, 404 if it doesn't exist.
 *  2. Look up whether a route already exists for this train (`Route.trainId`
 *     is unique — a train can have at most one route).
 *
 *     BUG: the check below is inverted. It reads `if (!existingRoute)` —
 *     i.e. "no route found yet" — and throws "Route already existis for
 *     this train" in exactly that case. The intended check was almost
 *     certainly `if (existingRoute) throw new ConflictError(...)`. As
 *     written, every train's *first* route creation fails with a
 *     misleading "already exists" error, and if a route already does
 *     exist, execution instead falls through to `prisma.route.create`
 *     below, which would then throw a raw Prisma unique-constraint error
 *     on `Route.trainId` rather than a clean `ConflictError`.
 *  3. Validate every `stationId` in the payload actually exists.
 *  4. Validate `sequenceNumber`s are contiguous starting at 1 (sorted
 *     copy, checked index-by-index — doesn't mutate the original order
 *     used for the actual insert below).
 *  5. Create the route and all its `RouteStation` rows in one nested
 *     Prisma write, defaulting `arrivalTime`/`departureTime` to null and
 *     `distanceFromOrigin` to 0 when omitted.
 *
 * A ROUTE_CREATED Kafka publish was evidently planned (see the commented-
 * out block below, right after the `prisma.route.create` call) but is not
 * currently wired up — `adminProducer.publishRouteCreated` exists and is
 * fully implemented, but nothing calls it.
 */
const createRoute = async ({ trainId, stations }: RouteBodyType) => {
  // Train must already exist — a route can't be attached to a train that isn't there
  const existingTrain = await prisma.train.findUnique({
    where: { id: trainId },
  });
  if (!existingTrain) {
    throw new NotFoundError("Train Not found");
  }
  // See the bug note above the function: this check is inverted.
  const existingRoute = await prisma.route.findUnique({ where: { trainId } });
  if (!existingRoute) {
    throw new NotFoundError("Route already existis for this train");
  }
  const stationIds = stations.map((station) => station.stationId);
  const existingStations = await prisma.station.findMany({
    where: { id: { in: stationIds } },
  });
  if (existingStations.length !== stationIds.length) {
    throw new BadRequestError("One or more station Ids are invalid");
  }
  const sorted = [...stations].sort(
    (a, b) => a.sequenceNumber - b.sequenceNumber,
  );
  for (let i = 0; i < sorted.length; i++) {
    if (sorted[i].sequenceNumber !== i + 1) {
      throw new BadRequestError(
        "Sequence Numbers must be continous starting free",
      );
    }
  }
  const route = await prisma.route.create({
    data: {
      trainId,
      routeStations: {
        create: stations.map((s) => ({
          stationId: s.stationId,
          sequenceNumber: s.sequenceNumber,
          arrivalTime: s.arrivalTime || null,
          departureTime: s.departureTime || null,
          distanceFromOrigin: s.distanceFromOrigin || 0,
        })),
      },
    },
    include: {
      routeStations: {
        include: { station: true },
        orderBy: { sequenceNumber: "asc" },
      },
    },
  });
  // Commented out — a ROUTE_CREATED publish was started but never finished.
  // adminProducer.publishRouteCreated (see admin.producer.ts) still expects
  // a plain Prisma `Route`, not a `{ ...route, train }` shape, so this
  // wouldn't type-check as-is even if uncommented.
  // const trainWithSeats = await prisma.train.findUnique({
  //   where: { id: trainId },
  //   include: { seats: { orderBy: { seatNumber: "asc" } } },
  // });

  // await adminProducer.publishRouteCreated({
  //   ...route,
  //   train: trainWithSeats,
  // });
  return route;
};

/**
 * Fetches a single train by id with its seats (ordered by seatNumber) and
 * its full route (ordered by sequenceNumber, each stop including the
 * related Station row). Throws NotFoundError if no train has that id.
 */
const getTrainById = async (id: string) => {
  const train = await prisma.train.findUnique({
    where: { id },
    include: {
      seats: { orderBy: { seatNumber: "asc" } },
      route: {
        include: {
          routeStations: {
            include: { station: true },
            orderBy: { sequenceNumber: "asc" },
          },
        },
      },
    },
  });
  if (!train) throw new NotFoundError("Train not found");
  return train;
};
export const trainService = { getTrainById, createTrain, createRoute };
