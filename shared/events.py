"""
Event definitions for the event-driven platform.
All events follow a standard envelope pattern for consistency.
"""
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    """
    Standard event envelope wrapping all domain events.
    Provides correlation, ordering, and metadata for distributed tracing.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(min_length=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_service: str = Field(min_length=1)
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), min_length=1)
    
    # Domain payload
    payload: Dict[str, Any]
    
    # Metadata for tracing
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Idempotency
    idempotency_key: Optional[str] = Field(default=None, min_length=1)
    
    # Retry tracking
    attempt: int = Field(default=1, ge=1)
    
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
    model_config = ConfigDict(extra="forbid")
    order_id: str
    customer_id: str
    product_id: str
    quantity: int = Field(gt=0)
    total_amount: float = Field(gt=0)
    shipping_address: Optional[Dict[str, Any]] = None


class OrderCancelledPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str
    customer_id: str
    reason: str
    refund_amount: float = Field(ge=0)


class OrderStatusUpdatedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str
    previous_status: str
    new_status: str
    reason: Optional[str] = None


# ─── Payment Events ───

class PaymentCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: str
    order_id: str
    customer_id: str
    amount: float = Field(gt=0)
    transaction_id: str
    payment_method: str


class PaymentFailedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: str
    order_id: str
    customer_id: str
    amount: float = Field(gt=0)
    failure_reason: str
    retry_eligible: bool = True


class PaymentRefundedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    refund_id: str
    order_id: str
    customer_id: str
    amount: float = Field(gt=0)
    reason: str


# ─── Inventory Events ───

class InventoryReservedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reservation_id: str
    order_id: str
    product_id: str
    quantity: int = Field(gt=0)
    expires_at: Optional[str] = None


class InventoryReleasedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reservation_id: str
    order_id: str
    product_id: str
    quantity: int = Field(gt=0)
    reason: str


class InventoryInsufficientPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str
    product_id: str
    requested_quantity: int = Field(gt=0)
    available_quantity: int = Field(ge=0)


# ─── Notification Events ───

class NotificationSentPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    notification_id: str
    customer_id: str
    notification_type: str
    order_id: Optional[str] = None


class NotificationFailedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
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
