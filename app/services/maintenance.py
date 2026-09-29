"""Idle maintenance: release memory the process no longer needs.

The server spends most of its time idle, but the work it does in bursts (proxy
health checks over hundreds of endpoints, batch file hashing, JSON event
payloads) leaves two kinds of residue behind:

* cached ``httpx`` clients with their connection pools, and
* free blocks that CPython holds in its own arenas instead of returning to the
  OS, which keeps RSS at the high-water mark.

This loop clears both on a fixed interval so the idle footprint reflects what is
actually still referenced.
"""

from __future__ import annotations

import asyncio
import contextlib

from app.core.config import settings
from app.core.logging import logger
from app.core.memory import current_rss_mb, trim_memory
from app.services.http_client import evict_idle_proxy_clients
from app.services.notification_service import notifications

_task: asyncio.Task | None = None


async def _run_once() -> None:
    closed = await evict_idle_proxy_clients()
    purged = await notifications.purge_stale()
    before = current_rss_mb()
    trimmed = trim_memory()
    if closed or purged or trimmed:
        after = current_rss_mb()
        if before is not None and after is not None and before - after >= 1:
            logger.debug(
                "Maintenance: closed {} idle client(s), purged {} subscriber(s), "
                "RSS {} -> {} MB",
                closed,
                purged,
                before,
                after,
            )


async def _loop(interval: int) -> None:
    while True:
        try:
            await asyncio.sleep(interval)
            await _run_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Memory maintenance iteration failed")


def start_maintenance() -> None:
    global _task
    interval = settings.memory_maintenance_interval_seconds
    if interval <= 0:
        logger.info("Memory maintenance disabled")
        return
    if _task is not None and not _task.done():
        return
    _task = asyncio.create_task(_loop(interval))
    logger.info("Memory maintenance started (every {}s)", interval)


async def stop_maintenance() -> None:
    global _task
    task, _task = _task, None
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
