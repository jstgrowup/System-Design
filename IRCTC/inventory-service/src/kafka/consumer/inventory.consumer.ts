import { consumer, producer, connectProducer } from "../../config/kafka";
import { inventoryService } from "../../services/inventory.service";
import logger from "../../config/logger";
import { KAFKA_TOPICS } from "../../../../shared/constants/kafka-topics";
import { withDLQ } from "../../../../shared/utils/dlqHanlder";
import type {
  ScheduleCreatedEventData,
  ScheduleCancelledEventData,
} from "../../types";

class InventoryConsumer {
  async start(): Promise<void> {
    await consumer.connect();
    await connectProducer(); // needed for DLQ publishing
    logger.info("Inventory consumer connected");

    await consumer.subscribe({
      topics: [KAFKA_TOPICS.SCHEDULE_CREATED, KAFKA_TOPICS.SCHEDULE_CANCELLED],
      // Replays full topic history on a fresh deployment/consumer group so a
      // new instance rebuilds inventory state from scratch; safe only because
      // each handler checks IdempotencyRecord before applying an event.
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
                parsedValue as ScheduleCreatedEventData,
              );
              break;
            case KAFKA_TOPICS.SCHEDULE_CANCELLED:
              await inventoryService.cancelScheduleInventory(
                parsedValue as ScheduleCancelledEventData,
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
