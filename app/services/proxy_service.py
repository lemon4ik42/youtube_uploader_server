from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID
from weakref import WeakKeyDictionary

import httpx
from socksio.exceptions import ProtocolError
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings
from app.core.db import async_session_maker
from app.core.logging import logger
from app.core.memory import trim_memory
from app.core.security import decrypt
from app.models.channel import Channel
from app.models.proxy import Proxy
from app.schemas.channel import ChannelOut
from app.schemas.proxy import ProxyOut
from app.services.http_client import (
    client_in_use,
    evict_idle_proxy_clients,
    get_client,
)
from app.services.notification_service import notifications


# A proxy can be checked from the API and from multiple channel workers at the
# same time. Keep one shared task per proxy so callers wait for the same network
# operation instead of creating duplicate checks.
_running_checks: dict[UUID, asyncio.Task[bool]] = {}
_running_check_all: asyncio.Task[dict] | None = None
_proxy_recovery_tasks: dict[UUID, asyncio.Task[None]] = {}
_proxy_recovery_stopping = False
_proxy_assignment_lock = asyncio.Lock()
_proxy_reservations: dict[UUID, list[float]] = {}
_PROXY_RESERVATION_SECONDS = 30.0

# One semaphore per event loop. Within a loop it is shared by every concurrent
# caller (check-all, auto-select, recovery) so the total number of simultaneous
# proxy health checks stays bounded. ``WeakKeyDictionary`` lets a finished loop
# (each test case runs its own) drop its semaphore instead of pinning the loop
# object for the process lifetime.
_proxy_check_semaphores: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = (
    WeakKeyDictionary()
)

# ``record_proxy_url_usage`` runs after every upload chunk. Resolving a URL to
# proxy ids used to load every proxy row and Fernet-decrypt each one on each
# call. The mapping only changes when proxies are created/updated/deleted, so it
# is cached and invalidated explicitly by those code paths.
_proxy_url_index: dict[str, list[UUID]] | None = None
# Successful results for the same proxy are collapsed within this window; a
# healthy proxy pushing 5 MiB chunks would otherwise open a database session per
# chunk only to discover nothing changed.
_PROXY_USAGE_OK_THROTTLE_SECONDS = 30.0
_proxy_usage_ok_seen: dict[str, float] = {}


def invalidate_proxy_url_index() -> None:
    """Drop the cached URL -> proxy id mapping after the proxy list changes."""
    global _proxy_url_index
    _proxy_url_index = None
    _proxy_usage_ok_seen.clear()


async def _get_proxy_url_index() -> dict[str, list[UUID]]:
    global _proxy_url_index
    if _proxy_url_index is not None:
        return _proxy_url_index
    index: dict[str, list[UUID]] = {}
    async with async_session_maker() as session:
        result = await session.exec(select(Proxy.id, Proxy.url_enc))
        for proxy_id, url_enc in result.all():
            try:
                url = decrypt(url_enc)
            except Exception:
                continue
            index.setdefault(url, []).append(proxy_id)
    _proxy_url_index = index
    return index


def _get_check_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _proxy_check_semaphores.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(max(1, settings.proxy_check_concurrency))
        _proxy_check_semaphores[loop] = semaphore
    return semaphore


# Concurrent per-channel recovery searches. Each search scans the proxy list, so
# without a bound N pending channels multiply one scan by N.
_recovery_semaphores: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = (
    WeakKeyDictionary()
)


def _get_recovery_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _recovery_semaphores.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(max(1, settings.proxy_recovery_concurrency))
        _recovery_semaphores[loop] = semaphore
    return semaphore


def _active_proxy_reservations() -> dict[UUID, int]:
    now = asyncio.get_running_loop().time()
    for proxy_id, deadlines in list(_proxy_reservations.items()):
        active = [deadline for deadline in deadlines if deadline > now]
        if active:
            _proxy_reservations[proxy_id] = active
        else:
            _proxy_reservations.pop(proxy_id, None)
    return {proxy_id: len(deadlines) for proxy_id, deadlines in _proxy_reservations.items()}


async def consume_proxy_reservation(proxy_id: UUID | None) -> None:
    if proxy_id is None:
        return
    async with _proxy_assignment_lock:
        _active_proxy_reservations()
        deadlines = _proxy_reservations.get(proxy_id)
        if not deadlines:
            return
        deadlines.pop(0)
        if not deadlines:
            _proxy_reservations.pop(proxy_id, None)


async def _check_proxy_network(proxy_url: str) -> bool:
    """Run one health check. Network only: no database, no notifications.

    Kept free of database work so a large check wave never holds a connection
    while waiting on a socket, and so results can be written in batches.
    """
    timeout = float(max(1, settings.proxy_check_timeout_seconds))
    # Bound the number of simultaneous health checks. Checking a large proxy list
    # without this limit opens one socket per proxy at once and exhausts file
    # descriptors/ephemeral ports, which makes healthy proxies fail.
    async with _get_check_semaphore():
        try:
            client = get_client(proxy_url)
            # ``client_in_use`` pins the cached client for the duration of the
            # request. Without it the LRU cap can close a client while its own
            # request is still in flight, which surfaces as a spurious failure.
            async with client_in_use(proxy_url):
                # This is a public YouTube Data API endpoint. A response proves the
                # proxy can reach the exact API used by channel metadata operations.
                response = await asyncio.wait_for(
                    client.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"part": "id", "id": "UC_x5XG1OV2P6uZZ5FSM9Ttw"},
                        timeout=httpx.Timeout(timeout),
                    ),
                    # httpx applies its timeout separately to connect/read/write/
                    # pool. This outer timeout limits the whole operation instead.
                    timeout=timeout,
                )
            # YouTube may return 400/401/403 without an API key or OAuth token; that
            # still proves the proxy reached YouTube and received an API response.
            return response.status_code < 500 and response.status_code != 407
        # httpx exposes some SOCKS transport errors directly instead of wrapping
        # them in HTTPError. RuntimeError covers a client closed underneath us.
        except (
            asyncio.TimeoutError,
            httpx.HTTPError,
            OSError,
            ProtocolError,
            RuntimeError,
        ):
            return False


async def _record_check_results(
    results: list[tuple[UUID, bool]], *, broadcast: bool = True
) -> None:
    """Persist a batch of check results using a single database session."""
    if not results:
        return
    payloads: list[dict] = []
    async with async_session_maker() as session:
        counts = await ProxyService._load_counts(session)
        now = datetime.now(timezone.utc)
        for proxy_id, ok in results:
            proxy = await session.get(Proxy, proxy_id)
            if proxy is None:
                continue
            proxy.last_checked_at = now
            proxy.last_check_ok = ok
            proxy.status = "working" if ok else "failed"
            proxy.failure_count = 0 if ok else proxy.failure_count + 1
            if broadcast:
                payloads.append(proxy_payload(proxy, counts.get(proxy_id, 0)))
        await session.commit()
    for payload in payloads:
        await notifications.broadcast("proxy.updated", {"proxy": payload})


async def _perform_shared_check(proxy_id: UUID, proxy_url: str) -> bool:
    """Check one proxy, record the result and start recovery if it failed."""
    ok = await _check_proxy_network(proxy_url)
    await _record_check_results([(proxy_id, ok)])
    if not ok:
        await start_proxy_recovery_for_proxy(proxy_id)
    return ok


def _remove_finished_check(proxy_id: UUID, task: asyncio.Task[bool]) -> None:
    if _running_checks.get(proxy_id) is task:
        _running_checks.pop(proxy_id, None)


def proxy_payload(proxy: Proxy, assigned_channels: int) -> dict:
    return ProxyOut(
        id=proxy.id, url=decrypt(proxy.url_enc),
        is_active=proxy.is_active, assigned_channels=assigned_channels,
        last_checked_at=proxy.last_checked_at, last_check_ok=proxy.last_check_ok,
        status=proxy.status,
        failure_count=proxy.failure_count, created_at=proxy.created_at,
        updated_at=proxy.updated_at,
    ).model_dump(mode="json")


class ProxyService:
    def __init__(self, session: AsyncSession | None = None):
        # ``session`` is optional. Read helpers that operate on a caller-provided
        # session (list/count/choose) use it; the long-running, network-bound
        # operations (check_all, choose_working, switch_channel) manage their own
        # short-lived sessions so no database connection is held while network
        # health checks run.
        self.session = session

    async def count(self, proxy_id: UUID) -> int:
        result = await self.session.exec(
            select(func.count(Channel.id)).where(Channel.proxy_id == proxy_id)
        )
        return result.scalar_one()

    @staticmethod
    async def _load_counts(
        session: AsyncSession, proxy_ids: list[UUID] | None = None
    ) -> dict[UUID, int]:
        query = select(Channel.proxy_id, func.count(Channel.id)).group_by(
            Channel.proxy_id
        )
        if proxy_ids is not None:
            query = query.where(Channel.proxy_id.in_(proxy_ids))
        result = await session.exec(query)
        return {proxy_id: count for proxy_id, count in result.all()}

    async def list(self) -> list[dict]:
        result = await self.session.exec(select(Proxy).order_by(Proxy.created_at))
        proxies = list(result.scalars().all())
        # One grouped count instead of one COUNT query per proxy (N+1), which
        # matters when the proxy list is large.
        count_result = await self.session.exec(
            select(Channel.proxy_id, func.count(Channel.id)).group_by(
                Channel.proxy_id
            )
        )
        counts = {proxy_id: count for proxy_id, count in count_result.all()}
        return [proxy_payload(p, counts.get(p.id, 0)) for p in proxies]

    async def choose(self, exclude_id: UUID | None = None) -> Proxy | None:
        result = await self.session.exec(
            select(Proxy).where(Proxy.is_active == True)  # noqa: E712
        )
        proxies = [p for p in result.scalars().all() if p.id != exclude_id]
        if not proxies:
            return None
        counts = {p.id: await self.count(p.id) for p in proxies}
        return min(proxies, key=lambda p: (counts[p.id], p.failure_count, p.created_at))

    async def _verify_working(
        self, proxies: list[Proxy], counts: dict[UUID, int]
    ) -> list[Proxy]:
        """Return the subset of ``proxies`` that currently work.

        Candidates are checked in priority order (last-known-working and least
        loaded first) in waves bounded by ``proxy_check_concurrency``. The scan
        stops as soon as at least one working proxy is found, so auto-select and
        automatic switching stay fast even with a very large proxy list.

        Checks here are network-only and results are written once per wave.
        Recording each result individually used to open a session per proxy and,
        for every failure, start a recovery loop — and each of those loops calls
        back into this method, so one manual "check all" on a large list turned
        into an exponentially growing storm of re-checks. Recovery is left to the
        caller that actually owns the channel.

        This method performs only network work; it must not be called while a
        database connection is held. ``counts`` is supplied by the caller from a
        short-lived session.
        """
        if not proxies:
            return []
        status_order = {"working": 0, "unknown": 1, "failed": 2}
        ordered = sorted(
            proxies,
            key=lambda proxy: (
                status_order.get(proxy.status, 1),
                counts.get(proxy.id, 0),
                proxy.failure_count,
                proxy.created_at,
            ),
        )
        wave_size = max(1, settings.proxy_check_concurrency)
        working: list[Proxy] = []
        for start in range(0, len(ordered), wave_size):
            wave = ordered[start : start + wave_size]
            results = await asyncio.gather(
                *(_checked_proxy(proxy.id, proxy.url_enc) for proxy in wave),
                return_exceptions=True,
            )
            outcomes = [
                (proxy.id, result is True) for proxy, result in zip(wave, results)
            ]
            await _record_check_results(outcomes)
            working.extend(
                proxy for proxy, result in zip(wave, results) if result is True
            )
            if working:
                break
            await asyncio.sleep(0)
        return working

    async def choose_working(
        self, exclude_id: UUID | None = None, *, reserve: bool = False
    ) -> Proxy | None:
        # Phase 1: short session to load candidates and current load. The session
        # is closed before any network health check runs.
        async with async_session_maker() as session:
            result = await session.exec(
                select(Proxy).where(Proxy.is_active == True)  # noqa: E712
            )
            proxies = [p for p in result.scalars().all() if p.id != exclude_id]
            if not proxies:
                return None
            counts = await self._load_counts(session)

        # Phase 2: network checks with no database connection held.
        working = await self._verify_working(proxies, counts)
        if not working:
            return None

        # Phase 3: short session under the assignment lock to pick the least
        # loaded proxy from the freshly verified pool.
        async with _proxy_assignment_lock:
            async with async_session_maker() as session:
                counts = await self._load_counts(
                    session, proxy_ids=[proxy.id for proxy in working]
                )
                reservations = _active_proxy_reservations()
                selected = min(
                    working,
                    key=lambda proxy: (
                        counts.get(proxy.id, 0) + reservations.get(proxy.id, 0),
                        proxy.failure_count,
                        proxy.created_at,
                    ),
                )
                if reserve:
                    deadline = (
                        asyncio.get_running_loop().time() + _PROXY_RESERVATION_SECONDS
                    )
                    _proxy_reservations.setdefault(selected.id, []).append(deadline)
                return selected

    async def switch_channel(
        self,
        channel_id: UUID,
        *,
        only_if_recovery_pending: bool = False,
        include_current: bool = False,
        keep_current_if_working: bool = False,
    ) -> Proxy | None:
        # Phase 1: short session to load the channel, its current proxy and the
        # candidate list. The session is closed before any network check runs, so
        # no database connection is held during the (potentially long) health
        # checks below.
        async with async_session_maker() as session:
            channel = await session.get(Channel, channel_id)
            if channel is None or channel.status == "deleted":
                return None
            if only_if_recovery_pending and not channel.proxy_recovery_pending:
                return None
            current_proxy_id = channel.proxy_id

            # While recovering a channel, keep tracking its own proxy. It is
            # checked in parallel with the replacement search; if it starts
            # working again, keep it and cancel the search instead of moving to
            # another proxy.
            current: Proxy | None = None
            if keep_current_if_working and current_proxy_id is not None:
                current = await session.get(Proxy, current_proxy_id)

            result = await session.exec(
                select(Proxy).where(Proxy.is_active == True)  # noqa: E712
            )
            candidates = [
                proxy
                for proxy in result.scalars().all()
                if include_current or proxy.id != current_proxy_id
            ]
            counts = await self._load_counts(session)

        # Phase 2: network checks with no database connection held.
        current_task = (
            asyncio.create_task(self.check(current))
            if current is not None and current.is_active
            else None
        )
        working = await self._verify_working(candidates, counts)

        if current_task is not None:
            try:
                current_ok = await current_task
            except Exception:
                current_ok = False
            if current_ok:
                await _keep_recovered_proxy(channel_id, current.id)
                return current

        if not working:
            return None

        # Phase 3: short session under the assignment lock to assign one of the
        # proxies verified working just now. Recompute load while holding the lock
        # so each completed assignment is visible to the next concurrent channel.
        async with _proxy_assignment_lock:
            async with async_session_maker() as session:
                channel = await session.get(
                    Channel, channel_id, populate_existing=True
                )
                if channel is None or channel.status == "deleted":
                    return None
                if only_if_recovery_pending and not channel.proxy_recovery_pending:
                    return None

                pool = [
                    proxy
                    for proxy in working
                    if include_current or proxy.id != channel.proxy_id
                ]
                if not pool:
                    return None
                counts = await self._load_counts(
                    session, proxy_ids=[proxy.id for proxy in pool]
                )
                reservations = _active_proxy_reservations()
                proxy = min(
                    pool,
                    key=lambda item: (
                        counts.get(item.id, 0) + reservations.get(item.id, 0),
                        item.failure_count,
                        item.created_at,
                    ),
                )
                previous_proxy_id = channel.proxy_id
                channel.proxy_id = proxy.id
                channel.proxy_recovery_pending = False
                await session.commit()
                await session.refresh(channel)
                # In-flight uploads resolve the proxy before every chunk from
                # a short-lived cache; drop it so the next chunk uses the new
                # proxy immediately.
                from app.services.channel_service import invalidate_channel_proxy_cache

                invalidate_channel_proxy_cache(channel_id)
                assigned_channels = await self._load_counts(session, proxy_ids=[proxy.id])
        await notifications.broadcast(
            "channel.proxy_switched",
            {
                "channel_id": str(channel_id),
                "previous_proxy_id": str(previous_proxy_id) if previous_proxy_id else None,
                "proxy_id": str(proxy.id),
                "proxy": proxy_payload(proxy, assigned_channels.get(proxy.id, 0)),
            },
        )
        return proxy

    async def check(self, proxy: Proxy) -> bool:
        check_task = _running_checks.get(proxy.id)
        if check_task is None:
            check_task = asyncio.create_task(
                _perform_shared_check(proxy.id, decrypt(proxy.url_enc))
            )
            _running_checks[proxy.id] = check_task
            check_task.add_done_callback(
                lambda finished, proxy_id=proxy.id: _remove_finished_check(
                    proxy_id, finished
                )
            )
        # Cancelling one caller does not cancel the shared task. Other API
        # requests and workers continue waiting for the same result.
        return await asyncio.shield(check_task)

    async def check_all(self) -> dict:
        global _running_check_all

        check_all_task = _running_check_all
        if check_all_task is None:
            check_all_task = asyncio.create_task(_perform_check_all())
            _running_check_all = check_all_task
            check_all_task.add_done_callback(_remove_finished_check_all)
        return await asyncio.shield(check_all_task)


async def _perform_check_all() -> dict:
    async with async_session_maker() as session:
        result = await session.exec(
            select(Proxy.id, Proxy.url_enc).where(Proxy.is_active == True)  # noqa: E712
        )
        targets = [(proxy_id, url_enc) for proxy_id, url_enc in result.all()]

    total = len(targets)
    if not total:
        payload = {"checked": 0, "working": 0, "failed": 0, "proxies": []}
        await notifications.broadcast("proxies.checked", payload)
        return payload

    # Process in waves of ``proxy_check_concurrency`` instead of creating one
    # task per proxy up front. Spawning thousands of coroutines that all then
    # queue on the same semaphore keeps every pending request object, its
    # decrypted URL and its client alive simultaneously, and starves unrelated
    # endpoints because the event loop has to service that whole backlog. A wave
    # only ever holds ``concurrency`` in-flight checks, so memory stays flat and
    # the loop keeps serving HTTP requests between waves.
    wave_size = max(1, settings.proxy_check_concurrency)
    results: list[bool] = []
    failed_ids: list[UUID] = []

    for start in range(0, total, wave_size):
        wave = targets[start : start + wave_size]
        wave_results = await asyncio.gather(
            *(
                _checked_proxy(proxy_id, url_enc)
                for proxy_id, url_enc in wave
            ),
            return_exceptions=True,
        )
        batch: list[tuple[UUID, bool]] = []
        for (proxy_id, _), outcome in zip(wave, wave_results):
            ok = outcome is True
            batch.append((proxy_id, ok))
            results.append(ok)
            if not ok:
                failed_ids.append(proxy_id)
        # One session per wave rather than one per proxy.
        await _record_check_results(batch)
        # Yield to the loop so queued HTTP requests are served between waves.
        await asyncio.sleep(0)

    # Recovery loops are started once, after every proxy has been classified.
    # Starting them per failure mid-scan spawned a switch_channel search that
    # re-checked the whole list again, multiplying the work.
    for proxy_id in failed_ids:
        await start_proxy_recovery_for_proxy(proxy_id)

    working = sum(results)
    async with async_session_maker() as session:
        payloads = await ProxyService(session).list()
    checked_ids = {str(proxy_id) for proxy_id, _ in targets}
    payload = {
        "checked": len(results),
        "working": working,
        "failed": len(results) - working,
        "proxies": [p for p in payloads if p["id"] in checked_ids],
    }
    await notifications.broadcast("proxies.checked", payload)
    # A full scan touches every proxy client; release what the wave allocated
    # instead of leaving it in the allocator arenas until the next maintenance
    # tick.
    await evict_idle_proxy_clients()
    trim_memory()
    return payload


async def _checked_proxy(proxy_id: UUID, url_enc: str) -> bool:
    """Network-only check that reuses an in-flight check for the same proxy."""
    try:
        proxy_url = decrypt(url_enc)
    except Exception:
        return False

    existing = _running_checks.get(proxy_id)
    if existing is not None:
        return await asyncio.shield(existing)

    task = asyncio.create_task(_check_proxy_network(proxy_url))
    _running_checks[proxy_id] = task
    task.add_done_callback(
        lambda finished, pid=proxy_id: _remove_finished_check(pid, finished)
    )
    return await asyncio.shield(task)


def _remove_finished_check_all(task: asyncio.Task[dict]) -> None:
    global _running_check_all
    if _running_check_all is task:
        _running_check_all = None


async def record_proxy_usage(proxy_id: UUID | None, ok: bool) -> None:
    """Update proxy health after an actual Google/YouTube request."""
    if proxy_id is None:
        return
    async with async_session_maker() as session:
        proxy = await session.get(Proxy, proxy_id)
        if proxy is None:
            return
        new_status = "working" if ok else "failed"
        if proxy.status == new_status and proxy.last_check_ok is ok:
            return
        proxy.last_checked_at = datetime.now(timezone.utc)
        proxy.last_check_ok = ok
        proxy.status = new_status
        proxy.failure_count = 0 if ok else proxy.failure_count + 1
        await session.commit()
        await session.refresh(proxy)
        count_result = await session.exec(
            select(func.count(Channel.id)).where(Channel.proxy_id == proxy_id)
        )
        await notifications.broadcast(
            "proxy.updated",
            {"proxy": proxy_payload(proxy, count_result.scalar_one())},
        )


async def record_proxy_url_usage(proxy_url: str, ok: bool) -> None:
    if ok:
        # A healthy proxy reports success after every uploaded chunk. Collapse
        # those into one database round-trip per window instead of loading and
        # decrypting the proxy list thousands of times per upload.
        now = asyncio.get_running_loop().time()
        last = _proxy_usage_ok_seen.get(proxy_url)
        if last is not None and now - last < _PROXY_USAGE_OK_THROTTLE_SECONDS:
            return
        _proxy_usage_ok_seen[proxy_url] = now
    else:
        _proxy_usage_ok_seen.pop(proxy_url, None)

    index = await _get_proxy_url_index()
    proxy_ids = index.get(proxy_url)
    if not proxy_ids:
        # The cache may predate a newly added proxy; rebuild once before giving up.
        invalidate_proxy_url_index()
        proxy_ids = (await _get_proxy_url_index()).get(proxy_url)
    if not proxy_ids:
        return
    await asyncio.gather(*(record_proxy_usage(proxy_id, ok) for proxy_id in proxy_ids))


async def schedule_proxy_recovery() -> None:
    """Start recovery loops for all channels currently waiting for a proxy."""
    if _proxy_recovery_stopping:
        return
    async with async_session_maker() as session:
        result = await session.exec(
            select(Channel).where(
                Channel.status != "deleted",
                Channel.proxy_recovery_pending == True,  # noqa: E712
            )
        )
        channel_ids = [channel.id for channel in result.scalars().all()]
    for channel_id in channel_ids:
        start_channel_proxy_recovery(channel_id)


async def _keep_recovered_proxy(channel_id: UUID, proxy_id: UUID) -> None:
    """Cancel proxy recovery after the channel's own proxy starts working again."""
    async with _proxy_assignment_lock:
        async with async_session_maker() as session:
            channel = await session.get(Channel, channel_id, populate_existing=True)
            if channel is None or channel.status == "deleted":
                return
            if not channel.proxy_recovery_pending or channel.proxy_id != proxy_id:
                # Recovery was cancelled or the proxy was changed manually.
                return
            channel.proxy_recovery_pending = False
            await session.commit()
            await session.refresh(channel)
            from app.services.channel_service import invalidate_channel_proxy_cache

            invalidate_channel_proxy_cache(channel_id)
            payload = ChannelOut.model_validate(channel).model_dump(mode="json")
    await notifications.broadcast("channel.proxy_updated", {"channel": payload})


async def start_proxy_recovery_for_proxy(proxy_id: UUID) -> None:
    if _proxy_recovery_stopping:
        return
    async with async_session_maker() as session:
        result = await session.exec(
            select(Channel).where(
                Channel.proxy_id == proxy_id,
                Channel.status != "deleted",
                Channel.proxy_auto_switch == True,  # noqa: E712
            )
        )
        channels = list(result.scalars().all())
        for channel in channels:
            channel.proxy_recovery_pending = True
        await session.commit()
    # Recovery invalidates any cached "route is usable" answer; uploaders must
    # go back to waiting for a working proxy instead of sending more chunks
    # through the dead one until the TTL expires.
    from app.services.channel_service import invalidate_channel_proxy_cache

    for channel in channels:
        invalidate_channel_proxy_cache(channel.id)
    for channel in channels:
        start_channel_proxy_recovery(channel.id)


def start_channel_proxy_recovery(channel_id: UUID) -> None:
    if _proxy_recovery_stopping:
        return
    task = _proxy_recovery_tasks.get(channel_id)
    if task is not None and not task.done():
        return
    task = asyncio.create_task(_channel_proxy_recovery_loop(channel_id))
    _proxy_recovery_tasks[channel_id] = task
    task.add_done_callback(
        lambda finished, cid=channel_id: _clear_channel_proxy_recovery(cid, finished)
    )


def _clear_channel_proxy_recovery(
    channel_id: UUID, task: asyncio.Task[None]
) -> None:
    if _proxy_recovery_tasks.get(channel_id) is task:
        _proxy_recovery_tasks.pop(channel_id, None)


async def stop_channel_proxy_recovery(channel_id: UUID) -> None:
    task = _proxy_recovery_tasks.pop(channel_id, None)
    if task is None or task is asyncio.current_task():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def stop_proxy_recovery() -> None:
    global _proxy_recovery_stopping
    _proxy_recovery_stopping = True
    tasks = list(_proxy_recovery_tasks.values())
    _proxy_recovery_tasks.clear()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def start_proxy_recovery() -> None:
    global _proxy_recovery_stopping
    _proxy_recovery_stopping = False
    await schedule_proxy_recovery()


async def _channel_proxy_recovery_loop(channel_id: UUID) -> None:
    while True:
        async with async_session_maker() as session:
            channel = await session.get(Channel, channel_id)
            if (
                channel is None
                or channel.status == "deleted"
                or not channel.proxy_recovery_pending
            ):
                return

        # Every recovery attempt scans the proxy list. Letting all pending
        # channels do that at once multiplies one scan by the channel count and
        # is what made a manual "check all" crawl. The semaphore keeps the number
        # of concurrent searches small; the others simply wait their turn.
        async with _get_recovery_semaphore():
            # Network health checks run inside switch_channel, which opens its own
            # short-lived sessions; no database connection is held while they run.
            selected = await ProxyService().switch_channel(
                channel_id,
                only_if_recovery_pending=True,
                keep_current_if_working=True,
            )
        if selected is not None:
            return

        # The client may have cancelled recovery during the checks.
        async with async_session_maker() as session:
            channel = await session.get(Channel, channel_id, populate_existing=True)
            if (
                channel is None
                or channel.status == "deleted"
                or not channel.proxy_recovery_pending
            ):
                return

        await asyncio.sleep(10)

async def replace_deleted_proxy_for_channels(channel_ids: list[UUID]) -> None:
    """Start persistent replacement searches after an assigned proxy is deleted."""
    async with async_session_maker() as session:
        for channel_id in channel_ids:
            channel = await session.get(Channel, channel_id)
            if channel is None or channel.status == "deleted":
                continue
            channel.proxy_recovery_pending = True
        await session.commit()
    from app.services.channel_service import invalidate_channel_proxy_cache

    for channel_id in channel_ids:
        invalidate_channel_proxy_cache(channel_id)
    for channel_id in channel_ids:
        start_channel_proxy_recovery(channel_id)
