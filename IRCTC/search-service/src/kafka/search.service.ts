import { consumer, producer, connectProducer } from "../config/kafka";
import searchService from "../services/search.service";
import logger from "../config/logger";
import { KAFKA_TOPICS } from "../../../shared/constants/kafka-topics";
import { withDLQ } from "../../../shared/utils/dlqHanlder";

/**
 * Kafka event contracts for every topic this consumer subscribes to.
 * These describe the JSON payload as it actually arrives over the wire —
 * defined here (the consumer) since this is the boundary that owns "what
 * shape does a message on topic X have", mirroring how admin-service's
 * admin.producer.ts owns its own event interfaces on the publish side.
 */

/**
 * Published by admin-service's stationService.createStation on every
 * successful POST /stations/station — the only event in this list that
 * actually fires in the system as currently wired (see this service's docs).
 */
export interface StationCreatedEvent {
  eventType: "STATION_CREATED";
  data: {
    id: string;
    name: string;
    code: string;
    city: string;
    state?: string | null;
  };
  timestamp: string;
}

/**
 * Would be published when admin-service attaches a route to a train —
 * never actually fires today: admin-service's trainService.createRoute has
 * its publishRouteCreated(...) call commented out.
 */
export interface RouteCreatedEvent {
  train: {
    id: string;
    trainNumber: string;
    trainName: string;
    seats?: { seatType: string }[];
  };
  routeStations: {
    station: { id: string; name: string; code: string; city: string };
    sequenceNumber: number;
    arrivalTime: string | null;
    departureTime: string | null;
    distanceFromOrigin: number;
  }[];
}

/**
 * Published by admin-service's scheduleService.createSchedule — the publish
 * call itself is correct, but never fires in practice because the HTTP
 * route that would trigger it isn't mounted in admin-service's server.ts.
 */
export interface ScheduleCreatedEvent {
  scheduleId: string;
  trainId: string;
  departureDate: string;
  status: string;
  seats?: {
    seatId: string;
    seatNumber: number;
    seatType: string;
    price: number;
  }[];
}

/**
 * Would be published when a schedule is cancelled — nothing in admin-service
 * calls publishScheduleCancelled anywhere, so this never fires today either.
 */
export interface ScheduleCancelledEvent {
  eventType: "SCHEDULE_CANCELLED";
  data: {
    id: string;
    trainId: string;
    status: string;
  };
  timestamp: string;
}

/**
 * Expected to be published by inventory-service whenever a seat is booked,
 * released, or locked for checkout. Not verified against inventory-service's
 * own source as part of this pass.
 */
export interface SeatAvailabilityUpdatedEvent {
  scheduleId: string;
  trainId: string;
  available: number;
  locked: number;
  booked: number;
}

/**
 * Kafka consumer for this service — connects, subscribes to every topic
 * search-service cares about, and dispatches each message to the matching
 * indexing function in services/search.service.ts. Exported as a singleton
 * (below) so index.ts only ever starts one instance per process.
 */
class SearchConsumer {
  /**
   * Connects the consumer and the DLQ producer, subscribes to all five
   * topics from the beginning of each partition (so a fresh Elasticsearch
   * index gets backfilled from Kafka's retained history rather than only
   * seeing messages produced after this service started), then runs the
   * per-message dispatch loop until the process is shut down.
   */
  async start(): Promise<void> {
    await consumer.connect();
    await connectProducer(); // needed for DLQ publishing
    logger.info("Search consumer connected");

    await consumer.subscribe({
      topics: [
        KAFKA_TOPICS.STATION_CREATED,
        KAFKA_TOPICS.ROUTE_CREATED,
        KAFKA_TOPICS.SCHEDULE_CREATED,
        KAFKA_TOPICS.SCHEDULE_CANCELLED,
        KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
      ],
      fromBeginning: true,
    });

    await consumer.run({
      // parsedValue arrives as `unknown` from withDLQ (it's just
      // JSON.parse'd off the wire) — the `as` cast in each branch below is
      // a deliberate external-boundary assertion into that topic's known
      // event shape, not a blanket `any`.
      eachMessage: withDLQ<unknown>(
        producer,
        KAFKA_TOPICS.DLQ_SEARCH,
        logger,
        async ({ topic, partition, message, parsedValue }) => {
          logger.info(`Processing ${topic}`, {
            partition,
            offset: message.offset,
          });

          switch (topic) {
            case KAFKA_TOPICS.STATION_CREATED:
              await searchService.indexStation(
                parsedValue as StationCreatedEvent,
              );
              break;
            case KAFKA_TOPICS.ROUTE_CREATED:
              await searchService.indexTrainRoute(
                parsedValue as RouteCreatedEvent,
              );
              break;
            case KAFKA_TOPICS.SCHEDULE_CREATED:
              await searchService.indexSchedule(
                parsedValue as ScheduleCreatedEvent,
              );
              break;
            case KAFKA_TOPICS.SCHEDULE_CANCELLED:
              await searchService.cancelSchedule(
                parsedValue as ScheduleCancelledEvent,
              );
              break;
            case KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED:
              await searchService.updateSeatAvailability(
                parsedValue as SeatAvailabilityUpdatedEvent,
              );
              break;
            default:
              logger.warn(`Unknown topic: ${topic}`);
          }
        },
      ),
    });

    logger.info("Search consumer running...");
  }
}

export default new SearchConsumer();
