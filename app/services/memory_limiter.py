import asyncio
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)


class MemoryLimiter:
    """Global weighted semaphore that limits concurrent uploads by memory weight.

    Each upload acquires a weight equal to the file size before starting. This
    prevents reading too many large video chunks into memory at the same time.
    """

    def __init__(self, max_bytes: int):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.max_bytes = max_bytes
        self._used = 0
        self._condition = asyncio.Condition()

    async def acquire(self, weight: int) -> int:
        """Acquire up to ``weight`` bytes. Returns the actually booked weight."""
        booked = min(weight, self.max_bytes)
        async with self._condition:
            while self._used + booked > self.max_bytes:
                logger.debug(
                    "Memory limiter full: used %d/%d bytes, waiting for %d bytes",
                    self._used,
                    self.max_bytes,
                    booked,
                )
                await self._condition.wait()
            self._used += booked
            logger.debug(
                "Memory limiter acquired %d bytes (used %d/%d)",
                booked,
                self._used,
                self.max_bytes,
            )
        return booked

    async def release(self, weight: int) -> None:
        async with self._condition:
            self._used = max(0, self._used - weight)
            logger.debug(
                "Memory limiter released %d bytes (used %d/%d)",
                weight,
                self._used,
                self.max_bytes,
            )
            self._condition.notify_all()


# Global limiter shared across all channels. 1 GiB by default.
upload_memory_limiter = MemoryLimiter(settings.max_upload_memory_bytes)
