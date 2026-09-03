"""
Payment Service - Handles payment processing.
Consumes OrderCreated events, processes payments, publishes results.
"""
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from sqlalchemy import Column, DateTime, Float, String, Text, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payment-service")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
DATABASE_URL = f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'app_user')}:{os.getenv('POSTGRES_PASSWORD', 'app_password')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB', 'ecommerce')}"
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8001")


class Base(DeclarativeBase):
    pass

class Payment(Base):
    __tablename__ = "payments"
    payment_id = Column(String, primary_key=True)
    order_id = Column(String, nullable=False, index=True)
    customer_id = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String, default="pending")
    transaction_id = Column(String, nullable=True)
    payment_method = Column(String, default="credit_card")
    failure_reason = Column(String, nullable=True)
    idempotency_key = Column(String, unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


engine = create_async_engine(DATABASE_URL, pool_size=10)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
kafka_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    import sys
    sys.path.insert(0, "/app/shared")
    from kafka_utils import KafkaEventProducer
    kafka_producer = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await kafka_producer.start()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Payment Service started ✓")
    yield
    await kafka_producer.stop()
    await engine.dispose()


app = FastAPI(title="Payment Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "payment-service", "version": "1.0.0"}


@app.get("/api/v1/payments/{order_id}")
async def get_payment(order_id: str):
    async with async_session() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        payment = result.scalar_one_or_none()
    if not payment:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Payment not found")
    return {
        "payment_id": payment.payment_id,
        "order_id": payment.order_id,
        "amount": payment.amount,
        "status": payment.status,
        "transaction_id": payment.transaction_id,
        "created_at": payment.created_at.isoformat() if payment.created_at else None,
    }


async def process_payment(order_data: dict):
    """Process payment for an order. Called by Kafka consumer."""
    order_id = order_data["order_id"]
    customer_id = order_data["customer_id"]
    amount = order_data["total_amount"]
    correlation_id = order_data.get("correlation_id", str(uuid.uuid4()))

    # Check idempotency
    async with async_session() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        existing = result.scalar_one_or_none()
        if existing and existing.status == "completed":
            logger.info(f"Payment already processed for order {order_id}")
            return

    # Simulate payment processing (90% success rate)
    import random
    payment_id = f"PAY-{uuid.uuid4().hex[:8].upper()}"
    success = random.random() > 0.1  # 90% success
    transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}" if success else None

    async with async_session() as session:
        payment = Payment(
            payment_id=payment_id,
            order_id=order_id,
            customer_id=customer_id,
            amount=amount,
            status="completed" if success else "failed",
            transaction_id=transaction_id,
            failure_reason=None if success else "Card declined",
        )
        session.add(payment)
        await session.commit()

    # Publish payment result event
    event_type = "payment.completed" if success else "payment.failed"
    payload = {
        "payment_id": payment_id,
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
    }
    if success:
        payload["transaction_id"] = transaction_id
        payload["payment_method"] = "credit_card"
    else:
        payload["failure_reason"] = "Card declined"
        payload["retry_eligible"] = True

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "payment-service",
        "correlation_id": correlation_id,
        "payload": payload,
    }

    await kafka_producer.publish(topic="payment.events", event=event, key=order_id)

    # Notify order service
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}/events",
                json=event,
                timeout=5.0,
            )
    except Exception as e:
        logger.warning(f"Failed to notify order service: {e}")

    logger.info(f"Payment {payment_id} for order {order_id}: {'completed' if success else 'failed'}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
