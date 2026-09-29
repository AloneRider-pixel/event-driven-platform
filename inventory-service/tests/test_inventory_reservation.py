from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main


class FakeResult:
    def __init__(self, item):
        self.item = item

    def scalar_one_or_none(self):
        return self.item


class FakeSession:
    def __init__(self, item):
        self.item = item
        self.statements = []
        self.added = None
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement):
        self.statements.append(statement)
        if len(self.statements) == 1:
            return FakeResult(self.item)
        return SimpleNamespace(rowcount=1)

    def add(self, value):
        self.added = value

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


class FakeSessionFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self.session


@pytest.mark.asyncio
async def test_reservation_uses_atomic_version_and_stock_guard(monkeypatch):
    item = main.InventoryItem(
        product_id="PROD-001",
        product_name="Headphones",
        quantity_available=10,
        quantity_reserved=6,
        version=7,
    )
    session = FakeSession(item)
    monkeypatch.setattr(main, "async_session", FakeSessionFactory(session))
    producer = SimpleNamespace(publish=AsyncMock(return_value=True))
    monkeypatch.setattr(main, "kafka_producer", producer)
    monkeypatch.setattr(main, "_notify_order_service", AsyncMock())

    await main.reserve_inventory(
        product_id="PROD-001",
        quantity=3,
        order_id="ORD-001",
        correlation_id="corr-001",
    )

    assert len(session.statements) == 2
    reservation_update = str(
        session.statements[1].compile(compile_kwargs={"literal_binds": True})
    )
    assert "inventory.version = 7" in reservation_update
    assert "inventory.quantity_available - inventory.quantity_reserved >= 3" in reservation_update
    assert session.added.quantity == 3
    assert session.committed is True


@pytest.mark.asyncio
async def test_insufficient_atomic_update_publishes_insufficient_event(monkeypatch):
    item = main.InventoryItem(
        product_id="PROD-001",
        product_name="Headphones",
        quantity_available=10,
        quantity_reserved=9,
        version=2,
    )
    session = FakeSession(item)

    async def execute(statement):
        session.statements.append(statement)
        if len(session.statements) == 1:
            return FakeResult(item)
        if len(session.statements) == 2:
            return SimpleNamespace(rowcount=0)
        return FakeResult(item)

    session.execute = execute
    monkeypatch.setattr(main, "async_session", FakeSessionFactory(session))
    producer = SimpleNamespace(publish=AsyncMock(return_value=True))
    monkeypatch.setattr(main, "kafka_producer", producer)
    notify = AsyncMock()
    monkeypatch.setattr(main, "_notify_order_service", notify)

    await main.reserve_inventory(
        product_id="PROD-001",
        quantity=2,
        order_id="ORD-002",
        correlation_id="corr-002",
    )

    assert session.rolled_back is True
    producer.publish.assert_awaited_once()
    event = producer.publish.await_args.kwargs["event"]
    assert event["event_type"] == "inventory.insufficient"
    assert event["payload"]["available_quantity"] == 1
    notify.assert_awaited_once_with("ORD-002", event)
