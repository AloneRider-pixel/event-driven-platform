"""Contract tests for the cross-service event envelope and payloads."""
from shared.events import (
    EventEnvelope,
    InventoryInsufficientPayload,
    InventoryReleasedPayload,
    InventoryReservedPayload,
    NotificationFailedPayload,
    NotificationSentPayload,
    OrderCancelledPayload,
    OrderCreatedPayload,
    OrderStatusUpdatedPayload,
    PaymentCompletedPayload,
    PaymentFailedPayload,
    PaymentRefundedPayload,
    create_event,
)

def test_event_timestamp_is_timezone_aware() -> None:
    payload = OrderCreatedPayload(order_id='ORD-001', customer_id='CUST-001', product_id='PROD-001', quantity=1, total_amount=10.0)
    event = create_event('order.created', 'order-service', payload)
    assert event.timestamp.tzinfo is not None
    assert event.timestamp.utcoffset() is not None

def test_envelope_round_trip_preserves_trace_identity() -> None:
    payload = PaymentCompletedPayload(payment_id='PAY-001', order_id='ORD-001', customer_id='CUST-001', amount=10.0, transaction_id='TX-001', payment_method='card')
    event = create_event('payment.completed', 'payment-service', payload, correlation_id='CORR-001', idempotency_key='IDEMP-001')
    restored = EventEnvelope.from_json(event.to_json())
    assert restored.event_id == event.event_id
    assert restored.correlation_id == 'CORR-001'
    assert restored.idempotency_key == 'IDEMP-001'
    assert restored.timestamp == event.timestamp

def test_all_domain_payloads_validate_required_fields() -> None:
    payloads = [
        OrderCreatedPayload(order_id='O', customer_id='C', product_id='P', quantity=1, total_amount=1.0),
        OrderCancelledPayload(order_id='O', customer_id='C', reason='customer', refund_amount=1.0),
        OrderStatusUpdatedPayload(order_id='O', previous_status='pending', new_status='confirmed'),
        PaymentCompletedPayload(payment_id='P', order_id='O', customer_id='C', amount=1.0, transaction_id='T', payment_method='card'),
        PaymentFailedPayload(payment_id='P', order_id='O', customer_id='C', amount=1.0, failure_reason='declined'),
        PaymentRefundedPayload(refund_id='R', order_id='O', customer_id='C', amount=1.0, reason='cancelled'),
        InventoryReservedPayload(reservation_id='R', order_id='O', product_id='P', quantity=1),
        InventoryReleasedPayload(reservation_id='R', order_id='O', product_id='P', quantity=1, reason='timeout'),
        InventoryInsufficientPayload(order_id='O', product_id='P', requested_quantity=2, available_quantity=1),
        NotificationSentPayload(notification_id='N', customer_id='C', notification_type='email'),
        NotificationFailedPayload(customer_id='C', notification_type='email', error='provider unavailable'),
    ]
    for payload in payloads:
        event = create_event('contract.test', 'contract-test', payload)
        assert event.payload
        assert event.source_service == 'contract-test'

def test_retry_preserves_identity_and_increments_attempt() -> None:
    payload = InventoryReservedPayload(reservation_id='R', order_id='O', product_id='P', quantity=1)
    event = create_event('inventory.reserved', 'inventory-service', payload)
    retried = event.with_retry()
    assert retried.event_id == event.event_id
    assert retried.correlation_id == event.correlation_id
    assert retried.attempt == event.attempt + 1

def test_event_contracts_reject_unknown_fields() -> None:
    payload = {"order_id": "O", "customer_id": "C", "product_id": "P", "quantity": 1, "total_amount": 10.0, "unexpected": True}
    try:
        OrderCreatedPayload.model_validate(payload)
    except ValueError:
        return
    raise AssertionError("Unknown event payload fields must be rejected")


def test_event_contracts_reject_invalid_money_and_quantity() -> None:
    try:
        OrderCreatedPayload(order_id="O", customer_id="C", product_id="P", quantity=0, total_amount=10.0)
    except ValueError:
        pass
    else:
        raise AssertionError("Non-positive order quantities must be rejected")

    try:
        PaymentCompletedPayload(payment_id="P", order_id="O", customer_id="C", amount=0, transaction_id="T", payment_method="card")
    except ValueError:
        pass
    else:
        raise AssertionError("Non-positive payment amounts must be rejected")
