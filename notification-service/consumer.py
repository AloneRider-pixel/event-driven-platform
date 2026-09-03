"""Notification Service Kafka Consumer - Listens to multiple topics."""
import asyncio
import logging
import sys

sys.path.insert(0, "/app/shared")
from kafka_utils import KafkaEventConsumer, KafkaEventProducer
from main import (
    handle_order_created, handle_order_cancelled,
    handle_payment_completed, handle_payment_failed,
    handle_inventory_insufficient, KAFKA_SERVERS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-consumer")


async def main():
    consumer = KafkaEventConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id="notification-service",
        topics=["order.events", "payment.events", "inventory.events"],
    )
    
    consumer.register_handler("order.created", handle_order_created)
    consumer.register_handler("order.cancelled", handle_order_cancelled)
    consumer.register_handler("payment.completed", handle_payment_completed)
    consumer.register_handler("payment.failed", handle_payment_failed)
    consumer.register_handler("inventory.insufficient", handle_inventory_insufficient)
    
    await consumer.start()
    dlq = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await dlq.start()
    
    logger.info("Notification consumer started, listening on order.events, payment.events, inventory.events...")
    await consumer.consume(dlq_producer=dlq)


if __name__ == "__main__":
    asyncio.run(main())
