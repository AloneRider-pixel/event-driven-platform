"""Payment Service Kafka Consumer - Listens for order.created events."""
import asyncio
import logging
import os
import sys

sys.path.insert(0, "/app/shared")
from kafka_utils import KafkaEventConsumer, KafkaEventProducer
from main import process_payment, kafka_producer, KAFKA_SERVERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payment-consumer")


async def handle_order_created(event: dict):
    """Handle order.created event."""
    payload = event.get("payload", {})
    logger.info(f"Processing order.created: order_id={payload.get('order_id')}")
    await process_payment(payload)


async def handle_order_cancelled(event: dict):
    """Handle order.cancelled - process refund."""
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    logger.info(f"Processing refund for cancelled order: {order_id}")
    # Refund logic would go here


async def main():
    consumer = KafkaEventConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id="payment-service",
        topics=["order.events"],
    )
    
    consumer.register_handler("order.created", handle_order_created)
    consumer.register_handler("order.cancelled", handle_order_cancelled)
    
    await consumer.start()
    
    # Create DLQ producer
    dlq_producer = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await dlq_producer.start()
    
    logger.info("Payment consumer started, listening on order.events...")
    await consumer.consume(dlq_producer=dlq_producer)


if __name__ == "__main__":
    asyncio.run(main())
