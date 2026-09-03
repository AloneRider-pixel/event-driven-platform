"""
Inventory Service - Manages product stock and reservations.
Consumes order.created events, reserves stock, publishes inventory events.
"""
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from sqlalchemy import Column, DateTime, Integer, String, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory-service")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
DATABASE_URL = f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'app_user')}:{os.getenv('POSTGRES_PASSWORD', 'app_password')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB', 'ecommerce')}"
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8001")


class Base(DeclarativeBase):
    pass

class InventoryItem(Base):
    __tablename__ = "inventory"
    product_id = Column(String, primary_key=True)
    product_name = Column(String, nullable=False)
    quantity_available = Column(Integer, default=100)
    quantity_reserved = Column(Integer, default=0)
    unit_price = Column(Integer, default=4999)  # In cents
    version = Column(Integer, default=0)  # Optimistic locking
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Reservation(Base):
    __tablename__ = "reservations"
    reservation_id = Column(String, primary_key=True)
    order_id = Column(String, nullable=False, index=True)
    product_id = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    status = Column(String, default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)


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
        # Seed initial inventory
        await _seed_inventory(conn)
    
    logger.info("Inventory Service started ✓")
    yield
    await kafka_producer.stop()
    await engine.dispose()


async def _seed_inventory(conn):
    """Seed initial inventory data."""
    result = await conn.execute(select(InventoryItem).limit(1))
    if result.first():
        return
    
    products = [
        ("PROD-001", "Wireless Headphones", 150, 4999),
        ("PROD-002", "USB-C Cable", 500, 1299),
        ("PROD-003", "Laptop Stand", 75, 7999),
        ("PROD-004", "Mechanical Keyboard", 200, 12999),
        ("PROD-005", "Webcam HD", 100, 8999),
    ]
    for pid, name, qty, price in products:
        await conn.execute(
            InventoryItem.__table__.insert().values(
                product_id=pid, product_name=name,
                quantity_available=qty, unit_price=price,
            )
        )


app = FastAPI(title="Inventory Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "inventory-service", "version": "1.0.0"}


@app.get("/api/v1/inventory/{product_id}")
async def get_inventory(product_id: str):
    async with async_session() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.product_id == product_id))
        item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Product not found")
    return {
        "product_id": item.product_id,
        "product_name": item.product_name,
        "quantity_available": item.quantity_available,
        "quantity_reserved": item.quantity_reserved,
        "unit_price": item.unit_price / 100,
    }


@app.get("/api/v1/inventory")
async def list_inventory():
    async with async_session() as session:
        result = await session.execute(select(InventoryItem))
        items = result.scalars().all()
    return {
        "items": [
            {
                "product_id": i.product_id,
                "product_name": i.product_name,
                "quantity_available": i.quantity_available,
                "quantity_reserved": i.quantity_reserved,
                "unit_price": i.unit_price / 100,
            }
            for i in items
        ]
    }


@app.put("/api/v1/inventory/{product_id}")
async def update_inventory(product_id: str, quantity: int):
    async with async_session() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.product_id == product_id))
        item = result.scalar_one_or_none()
        if not item:
            raise HTTPException(status_code=404, detail="Product not found")
        item.quantity_available = quantity
        item.updated_at = datetime.utcnow()
        await session.commit()
    return {"product_id": product_id, "quantity_available": quantity}


async def reserve_inventory(product_id: str, quantity: int, order_id: str, correlation_id: str = None):
    """Reserve inventory for an order. Implements optimistic locking."""
    async with async_session() as session:
        result = await session.execute(select(InventoryItem).where(InventoryItem.product_id == product_id))
        item = result.scalar_one_or_none()
        
        if not item:
            # Publish insufficient event
            await _publish_insufficient(product_id, quantity, 0, order_id, correlation_id)
            return
        
        available = item.quantity_available - item.quantity_reserved
        
        if available >= quantity:
            item.quantity_reserved += quantity
            item.version += 1
            item.updated_at = datetime.utcnow()
            
            reservation = Reservation(
                reservation_id=f"RES-{uuid.uuid4().hex[:8].upper()}",
                order_id=order_id,
                product_id=product_id,
                quantity=quantity,
                status="active",
            )
            session.add(reservation)
            await session.commit()
            
            # Publish reserved event
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "inventory.reserved",
                "timestamp": datetime.utcnow().isoformat(),
                "source_service": "inventory-service",
                "correlation_id": correlation_id or str(uuid.uuid4()),
                "payload": {
                    "reservation_id": reservation.reservation_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                },
            }
            await kafka_producer.publish(topic="inventory.events", event=event, key=order_id)
            
            # Notify order service
            await _notify_order_service(order_id, event)
            
            logger.info(f"Reserved {quantity}x {product_id} for order {order_id}")
        else:
            await session.commit()
            await _publish_insufficient(product_id, quantity, available, order_id, correlation_id)


async def release_inventory(order_id: str, correlation_id: str = None):
    """Release reserved inventory (for cancelled orders)."""
    async with async_session() as session:
        result = await session.execute(select(Reservation).where(Reservation.order_id == order_id, Reservation.status == "active"))
        reservation = result.scalar_one_or_none()
        
        if not reservation:
            return
        
        # Release stock
        inv_result = await session.execute(select(InventoryItem).where(InventoryItem.product_id == reservation.product_id))
        item = inv_result.scalar_one_or_none()
        if item:
            item.quantity_reserved -= reservation.quantity
            item.updated_at = datetime.utcnow()
        
        reservation.status = "released"
        await session.commit()
    
    # Publish released event
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "inventory.released",
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "inventory-service",
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "payload": {
            "order_id": order_id,
            "product_id": reservation.product_id,
            "quantity": reservation.quantity,
            "reason": "order_cancelled",
        },
    }
    await kafka_producer.publish(topic="inventory.events", event=event, key=order_id)
    logger.info(f"Released inventory for order {order_id}")


async def _publish_insufficient(product_id: str, requested: int, available: int, order_id: str, correlation_id: str = None):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "inventory.insufficient",
        "timestamp": datetime.utcnow().isoformat(),
        "source_service": "inventory-service",
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "payload": {
            "order_id": order_id,
            "product_id": product_id,
            "requested_quantity": requested,
            "available_quantity": available,
        },
    }
    await kafka_producer.publish(topic="inventory.events", event=event, key=order_id)
    await _notify_order_service(order_id, event)


async def _notify_order_service(order_id: str, event: dict):
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}/events", json=event, timeout=5.0)
    except Exception as e:
        logger.warning(f"Failed to notify order service: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
