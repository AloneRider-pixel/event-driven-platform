"""
Shared Pydantic models used across all microservices.
Ensures consistent data schemas across service boundaries.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


# ─── Enums ───

class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    FAILED = "failed"


class PaymentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class InventoryStatus(str, Enum):
    AVAILABLE = "available"
    RESERVED = "reserved"
    DEPLETED = "depleted"


class NotificationType(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class EventType(str, Enum):
    ORDER_CREATED = "order.created"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_UPDATED = "order.updated"
    PAYMENT_COMPLETED = "payment.completed"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_REFUNDED = "payment.refunded"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RELEASED = "inventory.released"
    INVENTORY_INSUFFICIENT = "inventory.insufficient"
    NOTIFICATION_SENT = "notification.sent"
    NOTIFICATION_FAILED = "notification.failed"


# ─── Order Models ───

class OrderItem(BaseModel):
    product_id: str
    product_name: str = ""
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)

    @property
    def total_price(self) -> float:
        return self.quantity * self.unit_price


class CreateOrderRequest(BaseModel):
    customer_id: str
    product_id: str
    quantity: int = Field(gt=0)
    idempotency_key: Optional[str] = None
    shipping_address: Optional[Dict[str, Any]] = None


class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    items: List[OrderItem]
    status: OrderStatus
    total_amount: float
    created_at: datetime
    updated_at: datetime
    idempotency_key: Optional[str] = None
    metadata: Dict[str, Any] = {}


# ─── Payment Models ───

class PaymentRequest(BaseModel):
    order_id: str
    amount: float
    customer_id: str
    payment_method: str = "credit_card"
    idempotency_key: Optional[str] = None


class PaymentResponse(BaseModel):
    payment_id: str
    order_id: str
    amount: float
    status: PaymentStatus
    transaction_id: Optional[str] = None
    created_at: datetime


# ─── Inventory Models ───

class InventoryItem(BaseModel):
    product_id: str
    product_name: str
    quantity_available: int = Field(ge=0)
    quantity_reserved: int = Field(ge=0)
    unit_price: float = Field(ge=0)
    status: InventoryStatus = InventoryStatus.AVAILABLE


class StockReservationRequest(BaseModel):
    product_id: str
    quantity: int
    order_id: str


class StockReservationResponse(BaseModel):
    reservation_id: str
    product_id: str
    quantity: int
    order_id: str
    status: str
    expires_at: Optional[datetime] = None


# ─── Notification Models ───

class NotificationRequest(BaseModel):
    customer_id: str
    type: NotificationType
    subject: str
    body: str
    order_id: Optional[str] = None


class NotificationResponse(BaseModel):
    notification_id: str
    customer_id: str
    type: NotificationType
    status: str
    sent_at: datetime


# ─── Common Models ───

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    timestamp: datetime
    dependencies: Dict[str, str] = {}


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int


class APIError(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
