import { producer, connectProducer } from "../../config/kafka";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";
import { Station, Train, Route, Schedule } from "../../generated/prisma/client";

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
 * Wraps the shared Kafka producer with domain-specific helpers for
 * admin-related events (station/train/route/schedule lifecycle).
 * Lazily connects the producer on first use rather than at import time.
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
  async publishScheduleCreated(scheduleData: Schedule) {
    return this.sendMessage<Schedule>(
      KAFKA_TOPICS.SCHEDULE_CREATED,
      `schedule-${scheduleData.id}`,
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
