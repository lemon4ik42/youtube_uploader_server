import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.logging import logger


@dataclass
class _Subscriber:
    queue: asyncio.Queue[str]
    pending: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    connected: bool = True
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class NotificationService:
    """In-memory pub/sub with client acknowledgements.

    Events are kept in a per-client buffer until the client explicitly
    acknowledges them via ``POST /updates/ack``. This protects against
    transient network issues: if the client disconnects before receiving an
    event, the event is still available after reconnect using the same
    ``subscriber_id``.

    Note: buffers are kept in process memory. Restarting the server drops
    unacknowledged events for disconnected clients.
    """

    def __init__(
        self,
        max_pending: int = 2_000,
        disconnected_ttl: timedelta = timedelta(minutes=5),
        max_disconnected: int = 50,
    ) -> None:
        self._subscribers: dict[str, _Subscriber] = {}
        self._lock = asyncio.Lock()
        self._max_pending = max_pending
        self._disconnected_ttl = disconnected_ttl
        self._max_disconnected = max_disconnected

    async def subscribe(
        self, subscriber_id: str | None = None
    ) -> tuple[str, asyncio.Queue[str]]:
        async with self._lock:
            await self._cleanup_unlocked()

            if subscriber_id:
                existing = self._subscribers.get(subscriber_id)
                if existing and not existing.connected:
                    existing.queue = asyncio.Queue(maxsize=100)
                    existing.connected = True
                    existing.last_active = datetime.now(timezone.utc)
                    logger.debug(
                        "SSE subscriber resumed, total {}", len(self._subscribers)
                    )
                    return subscriber_id, existing.queue

            subscriber_id = str(uuid.uuid4())
            queue: asyncio.Queue[str] = asyncio.Queue(maxsize=100)
            subscriber = _Subscriber(queue=queue)
            self._subscribers[subscriber_id] = subscriber
            logger.debug("SSE subscriber added, total {}", len(self._subscribers))
            return subscriber_id, queue

    async def unsubscribe(self, subscriber_id: str) -> None:
        async with self._lock:
            subscriber = self._subscribers.get(subscriber_id)
            if subscriber is not None:
                subscriber.connected = False
                subscriber.last_active = datetime.now(timezone.utc)
        logger.debug("SSE subscriber disconnected, total {}", len(self._subscribers))

    async def broadcast(self, event_type: str, payload: dict[str, Any]) -> str:
        event_id = str(uuid.uuid4())
        message = {"id": event_id, "event": event_type, "payload": payload}
        dead: list[str] = []

        async with self._lock:
            subscribers = list(self._subscribers.items())

        for subscriber_id, subscriber in subscribers:
            async with subscriber.lock:
                if len(subscriber.pending) >= self._max_pending:
                    if subscriber.connected:
                        dead.append(subscriber_id)
                    continue
                subscriber.pending[event_id] = message
                subscriber.last_active = datetime.now(timezone.utc)
            if subscriber.connected:
                try:
                    subscriber.queue.put_nowait(event_id)
                except asyncio.QueueFull:
                    dead.append(subscriber_id)

        if dead:
            async with self._lock:
                for subscriber_id in dead:
                    self._subscribers.pop(subscriber_id, None)

        return event_id

    def get_message(self, subscriber_id: str, event_id: str) -> dict[str, Any] | None:
        subscriber = self._subscribers.get(subscriber_id)
        if subscriber is None:
            return None
        return subscriber.pending.get(event_id)

    def get_pending_messages(self, subscriber_id: str) -> list[dict[str, Any]]:
        subscriber = self._subscribers.get(subscriber_id)
        if subscriber is None:
            return []
        return list(subscriber.pending.values())

    async def ack(self, subscriber_id: str, event_ids: list[str]) -> int:
        subscriber = self._subscribers.get(subscriber_id)
        if subscriber is None:
            return 0
        removed = 0
        async with subscriber.lock:
            for event_id in event_ids:
                if event_id in subscriber.pending:
                    del subscriber.pending[event_id]
                    removed += 1
            subscriber.last_active = datetime.now(timezone.utc)
        return removed

    async def _cleanup_unlocked(self) -> None:
        now = datetime.now(timezone.utc)
        disconnected = [
            (sid, sub)
            for sid, sub in self._subscribers.items()
            if not sub.connected
        ]
        if len(disconnected) <= self._max_disconnected:
            cutoff = now - self._disconnected_ttl
            dead = [sid for sid, sub in disconnected if sub.last_active < cutoff]
        else:
            disconnected.sort(key=lambda item: item[1].last_active)
            to_remove = len(disconnected) - self._max_disconnected
            dead = [sid for sid, _ in disconnected[:to_remove]]
        for sid in dead:
            del self._subscribers[sid]

    async def purge_stale(self) -> int:
        """Drop expired disconnected subscribers and their buffered events.

        Cleanup otherwise only happens when a client subscribes, so a client
        that disconnects and never returns keeps its buffered payloads alive for
        the lifetime of the process. The maintenance loop calls this instead.
        """
        async with self._lock:
            before = len(self._subscribers)
            await self._cleanup_unlocked()
            removed = before - len(self._subscribers)
        if removed:
            logger.debug("Purged {} stale SSE subscriber(s)", removed)
        return removed


notifications = NotificationService()
