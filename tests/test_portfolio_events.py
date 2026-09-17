from portfolio_hardening.events import EventEnvelope, IdempotencyStore


def _event() -> EventEnvelope:
    return EventEnvelope("evt-1", "order.created", 1, "order-42", "2026-09-17T10:00:00Z", {"total": 10}, "trace-1")


def test_event_is_canonical_and_versioned() -> None:
    event = _event()
    assert '"event_type":"order.created"' in event.canonical()
    assert event.idempotency_key() == event.idempotency_key()


def test_idempotency_store_reserves_once() -> None:
    store = IdempotencyStore()
    key = _event().idempotency_key()
    assert store.reserve(key) is True
    assert store.reserve(key) is False
    assert store.has_processed(key) is True


def test_invalid_event_fails_closed() -> None:
    bad = EventEnvelope("", "order.created", 0, "order-42", "bad", {}, "trace")
    try:
        bad.validate()
    except ValueError:
        pass
    else:
        raise AssertionError("invalid event should fail validation")
