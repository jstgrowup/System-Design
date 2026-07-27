import { consumer, producer, connectProducer } from "../../config/kafka";
import { inventoryService } from "../../services/inventory.service";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";
import { withDLQ } from "../../../../shared/utils/dlqHanlder";

/**
 * Kafka event contracts for every topic this consumer subscribes to.
 * These describe the JSON payload as it actually arrives over the wire —
 * defined here (the consumer) since this is the boundary that owns "what
 * shape does a message on topic X have", mirroring how admin-service's
 * admin.producer.ts owns its own event interfaces on the publish side.
 */

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
 * Kafka consumer for this service — connects, subscribes to the schedule
 * lifecycle topics this service cares about, and dispatches each message to
 * the matching function in services/inventory.service.ts. Exported as a
 * singleton (below) so index.ts only ever starts one instance per process.
 */
class InventoryConsumer {
  /**
   * Connects the consumer and the DLQ producer, subscribes to both topics
   * from the beginning of each partition (so a fresh inventory store gets
   * backfilled from Kafka's retained history rather than only seeing
   * messages produced after this service started), then runs the
   * per-message dispatch loop until the process is shut down.
   */
  async start(): Promise<void> {
    await consumer.connect();
    await connectProducer(); // needed for DLQ publishing
    logger.info("Inventory consumer connected");

    await consumer.subscribe({
      topics: [KAFKA_TOPICS.SCHEDULE_CREATED, KAFKA_TOPICS.SCHEDULE_CANCELLED],
      fromBeginning: true,
    });

    await consumer.run({
      // parsedValue arrives as `unknown` from withDLQ (it's just
      // JSON.parse'd off the wire) — the `as` cast in each branch below is
      // a deliberate external-boundary assertion into that topic's known
      // event shape, not a blanket `any`.
      eachMessage: withDLQ<unknown>(
        producer,
        KAFKA_TOPICS.DLQ_INVENTORY,
        logger,
        async ({ topic, partition, message, parsedValue }) => {
          logger.info(`Processing ${topic}`, {
            partition,
            offset: message.offset,
          });

          switch (topic) {
            case KAFKA_TOPICS.SCHEDULE_CREATED:
              await inventoryService.initializeInventory(
                parsedValue as ScheduleCreatedEvent,
              );
              break;
            case KAFKA_TOPICS.SCHEDULE_CANCELLED:
              await inventoryService.cancelScheduleInventory(
                parsedValue as ScheduleCancelledEvent,
              );
              break;
            default:
              logger.warn(`Unknown topic: ${topic}`);
          }
        },
      ),
    });

    logger.info("Inventory consumer running...");
  }
}

export default new InventoryConsumer();
