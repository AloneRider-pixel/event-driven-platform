"""Inventory Service Kafka Consumer."""
import asyncio
import logging
import sys

sys.path.insert(0, "/app/shared")
from kafka_utils import KafkaEventConsumer, KafkaEventProducer
from main import reserve_inventory, release_inventory, KAFKA_SERVERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory-consumer")


async def handle_order_created(event: dict):
    payload = event.get("payload", {})
    await reserve_inventory(
        product_id=payload["product_id"],
        quantity=payload["quantity"],
        order_id=payload["order_id"],
        correlation_id=event.get("correlation_id"),
    )


async def handle_order_cancelled(event: dict):
    payload = event.get("payload", {})
    await release_inventory(
        order_id=payload["order_id"],
        correlation_id=event.get("correlation_id"),
    )


async def main():
    consumer = KafkaEventConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id="inventory-service",
        topics=["order.events"],
    )
    consumer.register_handler("order.created", handle_order_created)
    consumer.register_handler("order.cancelled", handle_order_cancelled)
    
    await consumer.start()
    dlq = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await dlq.start()
    
    logger.info("Inventory consumer started, listening on order.events...")
    await consumer.consume(dlq_producer=dlq)


if __name__ == "__main__":
    asyncio.run(main())
