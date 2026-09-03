"""
Order Service - Core order management microservice.
Handles order CRUD, publishes events to Kafka, implements saga pattern.
"""
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Header, Query
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select, text, func
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order-service")

# ─── Configuration ───
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
DATABASE_URL = f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'app_user')}:{os.getenv('POSTGRES_PASSWORD', 'app_password')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB', 'ecommerce')}"
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# ─── Database ───
class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(String, primary_key=True)
    customer_id = Column(String, nullable=False, index=True)
    product_id = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    total_amount = Column(Float, nullable=False)
    status = Column(String, default="pending")
    idempotency_key = Column(String, unique=True, nullable=True)
    correlation_id = Column(String, nullable=True)
    payment_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata_json = Column("metadata", Text, default="{}")

engine = create_async_engine(DATABASE_URL, pool_size=10, max_overflow=20)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ─── Kafka ───
kafka_producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    import sys
    sys.path.insert(0, "/app/shared")
    from kafka_utils import KafkaEventProducer
    
    kafka_producer = KafkaEventProducer(bootstrap_servers=KAFKA_SERVERS)
    await kafka_producer.start()
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    logger.info("Order Service started ✓")
    yield
    await kafka_producer.stop()
    await engine.dispose()

app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)


# ─── Models ───
class CreateOrderRequest(BaseModel):
    customer_id: str
    product_id: str
    quantity: int = Field(gt=0)
    idempotency_key: Optional[str] = None

class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    total_amount: float
    status: str
    created_at: datetime
    updated_at: datetime


# ─── Health ───
@app.get("/health")
async def health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception:
        db_status = "unhealthy"
    return {"status": "healthy", "service": "order-service", "version": "1.0.0", "dependencies": {"postgres": db_status, "kafka": "connected" if kafka_producer else "disconnected"}}


# ─── Order Endpoints ───
@app.post("/api/v1/orders", status_code=201)
async def create_order(
    request: CreateOrderRequest,
    x_correlation_id: Optional[str] = Header(None),
):
    """Create a new order and publish OrderCreated event."""
    correlation_id = x_correlation_id or str(uuid.uuid4())
    
    # Idempotency check
    if request.idempotency_key:
        async with async_session() as session:
            result = await session.execute(
                select(Order).where(Order.idempotency_key == request.idempotency_key)
            )
            existing = result.scalar_one_or_none()
            if existing:
                return OrderResponse(
                    order_id=existing.order_id,
                    customer_id=existing.customer_id,
                    product_id=existing.product_id,
                    quantity=existing.quantity,
                    total_amount=existing.total_amount,
                    status=existing.status,
                    created_at=existing.created_at,
                    updated_at=existing.updated_at,
                )
    
    # Mock price lookup (in production, call inventory service)
    unit_price = 49.99
    total_amount = unit_price * request.quantity
    
    # Create order
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    
    async with async_session() as session:
        order = Order(
            order_id=order_id,
            customer_id=request.customer_id,
            product_id=request.product_id,
            quantity=request.quantity,
            total_amount=total_amount,
            status="pending",
            idempotency_key=request.idempotency_key,
            correlation_id=correlation_id,
        )
        session.add(order)
        await session.commit()
    
    logger.info(f"Order created: {order_id}, customer={request.customer_id}")
    
    # Publish OrderCreated event
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.created",
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "order-service",
        "correlation_id": correlation_id,
        "payload": {
            "order_id": order_id,
            "customer_id": request.customer_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "total_amount": total_amount,
        },
        "idempotency_key": request.idempotency_key,
    }
    
    await kafka_producer.publish(
        topic="order.events",
        event=event,
        key=order_id,
    )
    
    return OrderResponse(
        order_id=order_id,
        customer_id=request.customer_id,
        product_id=request.product_id,
        quantity=request.quantity,
        total_amount=total_amount,
        status="pending",
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


@app.get("/api/v1/orders/{order_id}")
async def get_order(order_id: str):
    """Get order by ID."""
    async with async_session() as session:
        result = await session.execute(select(Order).where(Order.order_id == order_id))
        order = result.scalar_one_or_none()
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return OrderResponse(
        order_id=order.order_id,
        customer_id=order.customer_id,
        product_id=order.product_id,
        quantity=order.quantity,
        total_amount=order.total_amount,
        status=order.status,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


@app.get("/api/v1/orders")
async def list_orders(
    customer_id: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List orders with pagination."""
    async with async_session() as session:
        query = select(Order)
        if customer_id:
            query = query.where(Order.customer_id == customer_id)
        query = query.order_by(Order.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
        
        result = await session.execute(query)
        orders = result.scalars().all()
        
        count_query = select(func.count(Order.order_id))
        if customer_id:
            count_query = count_query.where(Order.customer_id == customer_id)
        total = (await session.execute(count_query)).scalar()
    
    return {
        "items": [
            OrderResponse(
                order_id=o.order_id, customer_id=o.customer_id, product_id=o.product_id,
                quantity=o.quantity, total_amount=o.total_amount, status=o.status,
                created_at=o.created_at, updated_at=o.updated_at,
            ).model_dump()
            for o in orders
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.put("/api/v1/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    """Cancel an order and publish OrderCancelled event."""
    async with async_session() as session:
        result = await session.execute(select(Order).where(Order.order_id == order_id))
        order = result.scalar_one_or_none()
        
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        if order.status in ("cancelled", "delivered", "shipped"):
            raise HTTPException(status_code=400, detail=f"Cannot cancel order in '{order.status}' status")
        
        order.status = "cancelled"
        order.updated_at = datetime.utcnow()
        await session.commit()
    
    # Publish cancellation event
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.cancelled",
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "order-service",
        "correlation_id": order.correlation_id or str(uuid.uuid4()),
        "payload": {
            "order_id": order_id,
            "customer_id": order.customer_id,
            "reason": "customer_request",
            "refund_amount": order.total_amount,
        },
    }
    
    await kafka_producer.publish(topic="order.events", event=event, key=order_id)
    
    return {"order_id": order_id, "status": "cancelled", "message": "Order cancelled successfully"}


@app.post("/api/v1/orders/{order_id}/events")
async def handle_order_event(order_id: str, event: dict):
    """Handle internal events (payment completed, inventory reserved, etc.)."""
    event_type = event.get("event_type")
    
    async with async_session() as session:
        result = await session.execute(select(Order).where(Order.order_id == order_id))
        order = result.scalar_one_or_none()
        
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        if event_type == "payment.completed":
            order.status = "confirmed"
            order.payment_id = event.get("payload", {}).get("transaction_id")
        elif event_type == "payment.failed":
            order.status = "failed"
        elif event_type == "inventory.reserved":
            order.status = "processing"
        elif event_type == "inventory.insufficient":
            order.status = "failed"
        
        order.updated_at = datetime.utcnow()
        await session.commit()
    
    logger.info(f"Order {order_id} updated: status={order.status}, event={event_type}")
    return {"order_id": order_id, "status": order.status}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
