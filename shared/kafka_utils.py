"""
Kafka utilities for producing and consuming events.
Provides async producer/consumer with retries, DLQ, and error handling.
"""
import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """
    Async Kafka producer with:
    - Automatic serialization
    - Delivery confirmation
    - Error handling and retries
    """

    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        max_retries: int = 3,
        acks: str = "all",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.acks = acks
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        """Initialize the Kafka producer."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks=self.acks,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
            retry_backoff_ms=100,
            metadata_max_age_ms=30000,
        )
        await self._producer.start()
        logger.info(f"Kafka producer started: {self.bootstrap_servers}")

    async def stop(self):
        """Gracefully shut down the producer."""
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def publish(
        self,
        topic: str,
        event: Dict[str, Any],
        key: str = None,
        headers: Optional[List] = None,
    ) -> bool:
        """
        Publish an event to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            event: Event data (will be JSON serialized)
            key: Message key for partition routing
            headers: Optional message headers
        
        Returns:
            True if published successfully
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                result = await self._producer.send_and_wait(
                    topic=topic,
                    value=event,
                    key=key.encode("utf-8") if key else None,
                    headers=headers,
                )
                logger.info(
                    f"Event published: topic={topic}, "
                    f"partition={result.partition}, offset={result.offset}"
                )
                return True
            except KafkaError as e:
                logger.warning(
                    f"Kafka publish failed (attempt {attempt}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt * 0.1)  # Exponential backoff
                else:
                    logger.error(f"Kafka publish failed after {self.max_retries} attempts")
                    raise
        
        return False

    async def publish_with_dlq(
        self,
        topic: str,
        event: Dict[str, Any],
        dlq_topic: str = None,
        key: str = None,
    ) -> bool:
        """
        Publish with dead letter queue fallback.
        If publishing fails, sends to DLQ topic.
        """
        try:
            return await self.publish(topic, event, key=key)
        except Exception as e:
            dlq = dlq_topic or f"{topic}.dlq"
            logger.error(f"Publish failed, sending to DLQ: {dlq}")
            
            dlq_message = {
                "original_topic": topic,
                "original_event": event,
                "error": str(e),
                "failed_at": str(__import__("datetime").datetime.utcnow()),
            }
            
            try:
                await self.publish(dlq, dlq_message)
            except Exception as dlq_error:
                logger.critical(f"DLQ publish also failed: {dlq_error}")
            
            return False


class KafkaEventConsumer:
    """
    Async Kafka consumer with:
    - Consumer group support
    - Auto-commit with manual offset management
    - Dead letter queue routing
    - Graceful shutdown
    """

    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        group_id: str = "event-driven-platform",
        topics: List[str] = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics or []
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._handlers: Dict[str, Callable] = {}

    async def start(self):
        """Initialize the Kafka consumer."""
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda v: v.decode("utf-8") if v else None,
            max_poll_records=10,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        await self._consumer.start()
        self._running = True
        logger.info(f"Kafka consumer started: topics={self.topics}, group={self.group_id}")

    async def stop(self):
        """Gracefully shut down the consumer."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")

    def register_handler(self, event_type: str, handler: Callable):
        """Register an event handler for a specific event type."""
        self._handlers[event_type] = handler
        logger.info(f"Registered handler for: {event_type}")

    async def consume(self, dlq_producer: KafkaEventProducer = None):
        """
        Main consumption loop.
        Routes events to registered handlers based on event_type.
        """
        logger.info("Starting consumption loop...")
        
        try:
            async for message in self._consumer:
                if not self._running:
                    break
                
                try:
                    event = message.value
                    event_type = event.get("event_type", "unknown")
                    
                    logger.info(
                        f"Consumed event: type={event_type}, "
                        f"topic={message.topic}, partition={message.partition}, "
                        f"offset={message.offset}"
                    )
                    
                    # Route to handler
                    handler = self._handlers.get(event_type)
                    if handler:
                        await handler(event)
                    else:
                        # Check for wildcard handler
                        default_handler = self._handlers.get("*")
                        if default_handler:
                            await default_handler(event)
                        else:
                            logger.warning(f"No handler for event type: {event_type}")
                    
                    # Commit offset after successful processing
                    if not self.enable_auto_commit:
                        await self._consumer.commit()
                
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    
                    # Send to DLQ if configured
                    if dlq_producer:
                        dlq_topic = f"{message.topic}.dlq"
                        try:
                            await dlq_producer.publish(dlq_topic, {
                                "original_topic": message.topic,
                                "original_event": event,
                                "error": str(e),
                                "partition": message.partition,
                                "offset": message.offset,
                            })
                            # Commit offset even on failure (moved to DLQ)
                            if not self.enable_auto_commit:
                                await self._consumer.commit()
                        except Exception as dlq_error:
                            logger.critical(f"DLQ routing failed: {dlq_error}")
        
        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
