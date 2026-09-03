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
