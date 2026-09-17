from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    event_type: str
    version: int
    aggregate_id: str
    occurred_at: str
    payload: dict[str, Any]
    trace_id: str

    def validate(self) -> None:
        if not self.event_id.strip() or not self.event_type.strip() or not self.aggregate_id.strip():
            raise ValueError("event identity fields are required")
        if self.version < 1:
            raise ValueError("event version must be >= 1")
        datetime.fromisoformat(self.occurred_at.replace("Z", "+00:00"))

    def canonical(self) -> str:
        self.validate()
        return json.dumps(
            {
                "event_id": self.event_id,
                "event_type": self.event_type,
                "version": self.version,
                "aggregate_id": self.aggregate_id,
                "occurred_at": self.occurred_at,
                "payload": self.payload,
                "trace_id": self.trace_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def idempotency_key(self) -> str:
        material = f"{self.event_type}:{self.version}:{self.aggregate_id}:{self.event_id}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self) -> None:
        self._processed: set[str] = set()

    def reserve(self, key: str) -> bool:
        if key in self._processed:
            return False
        self._processed.add(key)
        return True

    def has_processed(self, key: str) -> bool:
        return key in self._processed
