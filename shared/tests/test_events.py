"""Tests for the shared event envelope contract."""
from __future__ import annotations

from shared.events import EventEnvelope, OrderCreatedPayload, create_event


def test_event_round_trip_preserves_contract_metadata() -> None:
    payload = OrderCreatedPayload(
        order_id="ORD-001",
        customer_id="CUST-001",
        product_id="PROD-001",
        quantity=2,
        total_amount=49.99,
    )
    event = create_event(
        "order.created",
        "order-service",
        payload,
        correlation_id="corr-001",
        idempotency_key="idem-001",
    )
    assert event.schema_version == "1.0"
    assert event.correlation_id == "corr-001"
    assert event.idempotency_key == "idem-001"
    restored = EventEnvelope.from_json(event.to_json())
    assert restored == event


def test_retry_increments_attempt_without_changing_identity() -> None:
    event = EventEnvelope(
        event_type="order.created",
        source_service="order-service",
        payload={"order_id": "ORD-001"},
        correlation_id="corr-001",
        idempotency_key="idem-001",
    )
    retried = event.with_retry()
    assert retried.event_id == event.event_id
    assert retried.correlation_id == event.correlation_id
    assert retried.attempt == event.attempt + 1
    assert retried.schema_version == event.schema_version


def test_unknown_envelope_fields_are_rejected() -> None:
    payload = {
        "event_type": "order.created",
        "source_service": "order-service",
        "payload": {"order_id": "ORD-001"},
        "unexpected": True,
    }
    try:
        EventEnvelope.model_validate(payload)
    except ValueError:
        return
    raise AssertionError("EventEnvelope accepted an unexpected contract field")
