"""
Event definitions for the event-driven platform.
All events follow a standard envelope pattern for consistency.
"""
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    """
    Standard event envelope wrapping all domain events.
    Provides correlation, ordering, and metadata for distributed tracing.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_service: str
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Domain payload
    payload: Dict[str, Any]
    
    # Metadata for tracing
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Idempotency
    idempotency_key: Optional[str] = None
    
    # Retry tracking
    attempt: int = 1
    
    def to_json(self) -> str:
        return self.model_dump_json()
    
    @classmethod
    def from_json(cls, data: str) -> "EventEnvelope":
        return cls.model_validate_json(data)
    
    def with_retry(self) -> "EventEnvelope":
        """Create a copy with incremented attempt counter."""
        return self.model_copy(update={"attempt": self.attempt + 1})


# ─── Order Events ───

class OrderCreatedPayload(BaseModel):
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    total_amount: float
    shipping_address: Optional[Dict[str, Any]] = None


class OrderCancelledPayload(BaseModel):
    order_id: str
    customer_id: str
    reason: str
    refund_amount: float


class OrderStatusUpdatedPayload(BaseModel):
    order_id: str
    previous_status: str
    new_status: str
    reason: Optional[str] = None


# ─── Payment Events ───

class PaymentCompletedPayload(BaseModel):
    payment_id: str
    order_id: str
    customer_id: str
    amount: float
    transaction_id: str
    payment_method: str


class PaymentFailedPayload(BaseModel):
    payment_id: str
    order_id: str
    customer_id: str
    amount: float
    failure_reason: str
    retry_eligible: bool = True


class PaymentRefundedPayload(BaseModel):
    refund_id: str
    order_id: str
    customer_id: str
    amount: float
    reason: str


# ─── Inventory Events ───

class InventoryReservedPayload(BaseModel):
    reservation_id: str
    order_id: str
    product_id: str
    quantity: int
    expires_at: Optional[str] = None


class InventoryReleasedPayload(BaseModel):
    reservation_id: str
    order_id: str
    product_id: str
    quantity: int
    reason: str


class InventoryInsufficientPayload(BaseModel):
    order_id: str
    product_id: str
    requested_quantity: int
    available_quantity: int


# ─── Notification Events ───

class NotificationSentPayload(BaseModel):
    notification_id: str
    customer_id: str
    notification_type: str
    order_id: Optional[str] = None


class NotificationFailedPayload(BaseModel):
    customer_id: str
    notification_type: str
    error: str
    order_id: Optional[str] = None


# ─── Event Factory ───

def create_event(
    event_type: str,
    source_service: str,
    payload: BaseModel,
    correlation_id: str = None,
    idempotency_key: str = None,
) -> EventEnvelope:
    """Factory function to create properly formatted events."""
    return EventEnvelope(
        event_type=event_type,
        source_service=source_service,
        payload=payload.model_dump(),
        correlation_id=correlation_id or str(uuid.uuid4()),
        idempotency_key=idempotency_key,
    )
