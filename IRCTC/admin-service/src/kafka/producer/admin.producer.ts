import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";
import {
  Station,
  Train,
  Route,
  Schedule,
  ScheduleStatus,
  SeatType,
} from "../../generated/prisma/client";

interface StationCreatedEvent {
  eventType: "STATION_CREATED";
  data: Station;
  timestamp: string;
}

interface ScheduleCancelledEvent {
  eventType: "SCHEDULE_CANCELLED";
  data: Schedule;
  timestamp: string;
}

/**
 * Denormalized schedule snapshot consumed by inventory-service and
 * search-service — carries the seat map and route so those services
 * don't need to call back into admin-service.
 */
export interface ScheduleCreatedPayload {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  coachName: string;
  totalSeats: number;
  departureDate: Date;
  status: ScheduleStatus;
  seats: {
    seatId: string;
    seatNumber: number;
    seatType: SeatType;
    price: number;
  }[];
  route: {
    stationId: string;
    stationName: string;
    stationCode: string;
    city: string;
    sequenceNumber: number;
    arrivalTime: string | null;
    departureTime: string | null;
    distanceFromOrigin: number;
  }[];
}

/**
 * Wraps the shared Kafka producer with domain-specific helpers for
 * admin-related events (station/train/route/schedule lifecycle).
 * Lazily connects the producer on first use rather than at import time.
 *
 * Only publishStationCreated (station.service.ts) and publishTrainCreated
 * (train.service.ts) are actually reached today. publishRouteCreated is
 * fully implemented but the call site in train.service.ts's createRoute is
 * commented out. publishScheduleCreated takes the denormalized
 * ScheduleCreatedPayload above (not a raw Prisma Schedule row) and is
 * called from schedule.service.ts — but that service is itself unreachable
 * via HTTP because schedule.route.ts is never mounted in server.ts.
 * publishScheduleCancelled has no caller anywhere in this codebase.
 */
class AdminProducer {
  private isInitialized: boolean;

  constructor() {
    this.isInitialized = false;
  }

  /**
   * Ensures the shared Kafka producer is connected before sending.
   * Safe to call multiple times — only connects once per process lifetime.
   */
  private async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  /**
   * Generic send helper — publishes a single message to the given topic.
   * Falls back to a timestamp-based key if none is provided, to avoid
   * all messages landing on the same partition when no natural key exists.
   */
  private async sendMessage<T>(
    topic: string,
    key: string | undefined,
    value: T,
  ) {
    try {
      await this.initialize();

      const message = {
        topic,
        messages: [
          {
            key: key || `${topic}-${Date.now()}`,
            value: JSON.stringify(value),
            timestamp: Date.now().toString(),
          },
        ],
      };

      const result = await producer.send(message);

      logger.info(`Message sent to kafka topic: ${topic}`, {
        key,
        partition: result[0].partition,
        offset: result[0].offset,
      });

      return result;
    } catch (error) {
      const err = error as Error;
      logger.error(`Failed to send message to kafka topic: ${topic}`, {
        error: err.message,
        stack: err.stack,
        key,
      });
      throw error;
    }
  }

  /**
   * Publishes a station-created event. Keyed by station id so all events
   * for the same station land on the same partition, preserving order.
   */
  async publishStationCreated(station: Station) {
    return this.sendMessage<StationCreatedEvent>(
      KAFKA_TOPICS.STATION_CREATED,
      `station-${station.id}`,
      {
        eventType: "STATION_CREATED",
        data: station,
        timestamp: new Date().toISOString(),
      },
    );
  }

  /**
   * Publishes a train-created event.
   */
  async publishTrainCreated(trainData: Train) {
    return this.sendMessage<Train>(
      KAFKA_TOPICS.TRAIN_CREATED,
      `train-${trainData.id}`,
      trainData,
    );
  }

  /**
   * Publishes a route-created event.
   */
  async publishRouteCreated(routeData: Route) {
    return this.sendMessage<Route>(
      KAFKA_TOPICS.ROUTE_CREATED,
      `route-${routeData.id}`,
      routeData,
    );
  }

  /**
   * Publishes a schedule-created event.
   */
  async publishScheduleCreated(scheduleData: ScheduleCreatedPayload) {
    return this.sendMessage<ScheduleCreatedPayload>(
      KAFKA_TOPICS.SCHEDULE_CREATED,
      `schedule-${scheduleData.scheduleId}`,
      scheduleData,
    );
  }

  /**
   * Publishes a schedule-cancelled event. Keyed by schedule id so all events
   * for the same schedule land on the same partition, preserving order.
   */
  async publishScheduleCancelled(schedule: Schedule) {
    return this.sendMessage<ScheduleCancelledEvent>(
      KAFKA_TOPICS.SCHEDULE_CANCELLED,
      `schedule-${schedule.id}`,
      {
        eventType: "SCHEDULE_CANCELLED",
        data: schedule,
        timestamp: new Date().toISOString(),
      },
    );
  }
}

export default new AdminProducer();
