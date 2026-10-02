"""Tests for the Order Service."""
import pytest
from shared.models import OrderStatus, PaymentStatus, CreateOrderRequest
from shared.events import EventEnvelope, create_event, OrderCreatedPayload


class TestEventModels:
    def test_event_envelope_creation(self):
        payload = OrderCreatedPayload(
            order_id="ORD-001",
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=2,
            total_amount=99.98,
        )
        event = create_event(
            event_type="order.created",
            source_service="order-service",
            payload=payload,
        )
        assert event.event_type == "order.created"
        assert event.source_service == "order-service"
        assert event.payload["order_id"] == "ORD-001"
        assert event.event_id is not None
        assert event.correlation_id is not None

    def test_event_serialization(self):
        payload = OrderCreatedPayload(
            order_id="ORD-002",
            customer_id="CUST-002",
            product_id="PROD-002",
            quantity=1,
            total_amount=49.99,
        )
        event = create_event("order.created", "order-service", payload)
        
        json_str = event.to_json()
        restored = EventEnvelope.from_json(json_str)
        
        assert restored.event_type == event.event_type
        assert restored.payload["order_id"] == "ORD-002"

    def test_event_with_retry(self):
        payload = OrderCreatedPayload(
            order_id="ORD-003", customer_id="CUST-001",
            product_id="PROD-001", quantity=1, total_amount=49.99,
        )
        event = create_event("order.created", "order-service", payload)
        assert event.attempt == 1
        
        retried = event.with_retry()
        assert retried.attempt == 2
        assert retried.event_id == event.event_id


class TestOrderModels:
    def test_create_order_request(self):
        req = CreateOrderRequest(
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=3,
        )
        assert req.customer_id == "CUST-001"
        assert req.quantity == 3
    
    def test_create_order_invalid_quantity(self):
        with pytest.raises(Exception):
            CreateOrderRequest(
                customer_id="CUST-001",
                product_id="PROD-001",
                quantity=0,  # Invalid: must be > 0
            )


class TestOrderStatus:
    def test_order_status_values(self):
        assert OrderStatus.PENDING == "pending"
        assert OrderStatus.CONFIRMED == "confirmed"
        assert OrderStatus.CANCELLED == "cancelled"
    
    def test_payment_status_values(self):
        assert PaymentStatus.COMPLETED == "completed"
        assert PaymentStatus.FAILED == "failed"
        assert PaymentStatus.REFUNDED == "refunded"


class TestTransactionalOutbox:
    def test_order_created_event_payload_is_durable(self):
        from main import OutboxEvent, build_order_created_event
        from shared.events import EventEnvelope

        event = build_order_created_event(
            order_id="ORD-OUTBOX",
            customer_id="CUST-OUTBOX",
            product_id="PROD-OUTBOX",
            quantity=2,
            total_amount=99.98,
            correlation_id="CORR-001",
            idempotency_key="IDEMP-001",
        )
        row = OutboxEvent(
            event_id=event.event_id,
            topic="order.events",
            message_key="ORD-OUTBOX",
            correlation_id=event.correlation_id,
            payload=event.to_json(),
        )

        restored = EventEnvelope.from_json(row.payload)
        assert row.published_at is None
        assert row.attempts == 0
        assert restored.event_id == row.event_id
        assert restored.payload["order_id"] == "ORD-OUTBOX"
        assert restored.idempotency_key == "IDEMP-001"

    def test_order_cancel_event_keeps_correlation_id(self):
        from main import build_order_cancelled_event

        event = build_order_cancelled_event(
            order_id="ORD-CANCEL",
            customer_id="CUST-1",
            refund_amount=49.99,
            correlation_id="CORR-CANCEL",
        )

        assert event.event_type == "order.cancelled"
        assert event.correlation_id == "CORR-CANCEL"
        assert event.payload["reason"] == "customer_request"
