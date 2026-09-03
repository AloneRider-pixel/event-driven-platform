"""
Notification Service - Sends emails/SMS based on events.
Consumes from multiple topics, handles all notification types.
"""
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-service")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# In-memory store for notifications (in production, use Redis/DB)
notifications = []
kafka_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    import sys
    sys.path.insert(0, "/app/shared")
    from kafka_utils import KafkaEventProducer
    kafka_producer = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await kafka_producer.start()
    logger.info("Notification Service started ✓")
    yield
    await kafka_producer.stop()


app = FastAPI(title="Notification Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "notification-service", "version": "1.0.0"}


@app.get("/api/v1/notifications")
async def list_notifications(limit: int = 20):
    return {"notifications": notifications[-limit:], "total": len(notifications)}


async def send_notification(customer_id: str, notification_type: str, subject: str, body: str, order_id: str = None):
    """Send a notification and track delivery."""
    notification_id = f"NOTIF-{uuid.uuid4().hex[:8].upper()}"
    
    notification = {
        "notification_id": notification_id,
        "customer_id": customer_id,
        "type": notification_type,
        "subject": subject,
        "body": body,
        "order_id": order_id,
        "status": "sent",
        "sent_at": datetime.utcnow().isoformat(),
    }
    notifications.append(notification)
    
    logger.info(f"Notification sent: {notification_id} to {customer_id} ({notification_type})")
    
    # Publish notification sent event
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "notification.sent",
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "notification-service",
        "payload": {
            "notification_id": notification_id,
            "customer_id": customer_id,
            "notification_type": notification_type,
            "order_id": order_id,
        },
    }
    await kafka_producer.publish(topic="notification.events", event=event)
    
    return notification


async def handle_order_created(event: dict):
    payload = event.get("payload", {})
    await send_notification(
        customer_id=payload["customer_id"],
        notification_type="email",
        subject=f"Order {payload['order_id']} Confirmed",
        body=f"Your order has been received. Total: ${payload['total_amount']:.2f}",
        order_id=payload["order_id"],
    )


async def handle_order_cancelled(event: dict):
    payload = event.get("payload", {})
    await send_notification(
        customer_id=payload["customer_id"],
        notification_type="email",
        subject=f"Order {payload['order_id']} Cancelled",
        body=f"Your order has been cancelled. Refund of ${payload['refund_amount']:.2f} will be processed.",
        order_id=payload["order_id"],
    )


async def handle_payment_completed(event: dict):
    payload = event.get("payload", {})
    await send_notification(
        customer_id=payload["customer_id"],
        notification_type="email",
        subject="Payment Received",
        body=f"Payment of ${payload['amount']:.2f} received for order {payload['order_id']}.",
        order_id=payload["order_id"],
    )


async def handle_payment_failed(event: dict):
    payload = event.get("payload", {})
    await send_notification(
        customer_id=payload["customer_id"],
        notification_type="email",
        subject="Payment Failed",
        body=f"Payment for order {payload['order_id']} failed: {payload.get('failure_reason', 'Unknown error')}.",
        order_id=payload["order_id"],
    )


async def handle_inventory_insufficient(event: dict):
    payload = event.get("payload", {})
    # Would need customer lookup in production
    logger.warning(f"Insufficient inventory: product={payload['product_id']}, order={payload['order_id']}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
