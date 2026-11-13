"""Offline compatibility probes for dependency PRs. Never starts app lifespan."""
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
def load(service):
    spec = importlib.util.spec_from_file_location(service.replace('-', '_'), ROOT / service / 'main.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

@pytest.fixture
def notification():
    m = load('notification-service')
    m.kafka_producer = SimpleNamespace(publish=AsyncMock())
    return m

def test_notification_health_and_empty_list(notification):
    c = TestClient(notification.app)
    assert c.get('/health').json()['status'] == 'healthy'
    assert c.get('/api/v1/notifications').json() == {'notifications': [], 'total': 0}

@pytest.mark.asyncio
@pytest.mark.parametrize('handler,payload,subject', [
    ('handle_order_created', {'customer_id':'fixture','order_id':'ORD-1','total_amount':12}, 'Confirmed'),
    ('handle_order_cancelled', {'customer_id':'fixture','order_id':'ORD-1','refund_amount':12}, 'Cancelled'),
    ('handle_payment_completed', {'customer_id':'fixture','order_id':'ORD-1','amount':12}, 'Received'),
    ('handle_payment_failed', {'customer_id':'fixture','order_id':'ORD-1','failure_reason':'fixture'}, 'Failed'),
])
async def test_notification_handlers(notification,handler,payload,subject):
    await getattr(notification,handler)({'payload':payload})
    assert len(notification.notifications) == 1
    assert subject in notification.notifications[0]['subject']
    event = notification.kafka_producer.publish.call_args.kwargs['event']
    assert event['event_type'] == 'notification.sent'
    assert event['payload']['customer_id'] == 'fixture'

@pytest.fixture
def payment():
    m = load('payment-service')
    m.kafka_producer = SimpleNamespace(publish=AsyncMock())
    return m

def session_for(payment, existing):
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()
    manager = MagicMock()
    manager.__aenter__ = AsyncMock(return_value=session)
    manager.__aexit__ = AsyncMock(return_value=None)
    payment.async_session = MagicMock(return_value=manager)
    return session

def test_payment_health_and_404(payment):
    session_for(payment,None)
    c = TestClient(payment.app)
    assert c.get('/health').json()['service'] == 'payment-service'
    assert c.get('/api/v1/payments/fixture').status_code == 404

@pytest.mark.asyncio
async def test_payment_completed_idempotency(payment):
    session = session_for(payment, SimpleNamespace(status='completed'))
    await payment.process_payment({'order_id':'fixture','customer_id':'fixture','total_amount':12})
    payment.kafka_producer.publish.assert_not_called()
    session.commit.assert_not_called()

@pytest.mark.asyncio
@pytest.mark.parametrize('random_value,event_type',[(0.9,'payment.completed'),(0.01,'payment.failed')])
async def test_payment_result_events(payment,random_value,event_type):
    session = session_for(payment,None)
    manager = MagicMock()
    manager.__aenter__ = AsyncMock(return_value=SimpleNamespace(post=AsyncMock()))
    manager.__aexit__ = AsyncMock(return_value=None)
    with patch('random.random',return_value=random_value),patch('httpx.AsyncClient',return_value=manager):
        await payment.process_payment({'order_id':'fixture','customer_id':'fixture','total_amount':12})
    session.commit.assert_awaited_once()
    event = payment.kafka_producer.publish.call_args.kwargs['event']
    assert event['event_type'] == event_type
    assert event['payload']['amount'] == 12
    if event_type.endswith('failed'):
        assert event['payload']['failure_reason'] == 'Card declined'
    else:
        assert event['payload']['transaction_id'].startswith('TXN-')
