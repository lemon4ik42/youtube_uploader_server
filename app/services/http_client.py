"""Shared HTTP clients for direct and proxied requests.

Two things used to dominate the idle footprint of a server with a large proxy
list:

1. ``httpx.AsyncClient`` builds its own ``ssl.SSLContext`` unless one is passed
   in. A context with the CA bundle loaded costs well over 1 MB, so 300 cached
   proxy clients retained ~420 MB of RSS while doing nothing. Every client now
   shares one lazily-created context.
2. Clients were cached forever. The cache is now bounded and idle entries are
   closed by the maintenance loop, so a proxy that is checked once does not keep
   a client and its connection pool alive for the lifetime of the process.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl
import time
from dataclasses import dataclass, field

import httpx
from socksio.exceptions import ProtocolError

from app.core.config import settings
from app.core.logging import logger

_client: httpx.AsyncClient | None = None
_ssl_context: ssl.SSLContext | None = None
# Clients evicted from a synchronous context; closed on the next async pass.
_pending_close: list[httpx.AsyncClient] = []


@dataclass
class _ProxyClient:
    client: httpx.AsyncClient
    last_used: float = field(default_factory=time.monotonic)
    # Requests currently using this client. A busy client is never evicted,
    # otherwise an in-flight upload would fail with "client has been closed".
    in_flight: int = 0


_proxy_clients: dict[str, _ProxyClient] = {}


def _get_ssl_context() -> ssl.SSLContext:
    """Return the process-wide SSL context shared by every client."""
    global _ssl_context
    if _ssl_context is None:
        _ssl_context = httpx.create_ssl_context()
    return _ssl_context


def _close_later(client: httpx.AsyncClient) -> None:
    """Schedule ``client.aclose()`` without blocking a synchronous caller."""
    try:
        asyncio.get_running_loop().create_task(client.aclose())
    except RuntimeError:
        _pending_close.append(client)


def _enforce_proxy_client_cap() -> None:
    """Drop the least recently used idle clients once the cache is over cap."""
    cap = max(1, settings.http_proxy_client_cache_size)
    if len(_proxy_clients) <= cap:
        return
    idle = sorted(
        (entry.last_used, url)
        for url, entry in _proxy_clients.items()
        if entry.in_flight == 0
    )
    for _, url in idle:
        if len(_proxy_clients) <= cap:
            break
        entry = _proxy_clients.pop(url)
        _close_later(entry.client)


def get_client(proxy_url: str | None = None) -> httpx.AsyncClient:
    """Return a client for ``proxy_url``, or the shared direct client."""
    if proxy_url:
        entry = _proxy_clients.get(proxy_url)
        if entry is None:
            entry = _ProxyClient(
                client=httpx.AsyncClient(
                    # A proxy serves a few channels at a time, so a large
                    # per-proxy pool only reserves memory and sockets.
                    limits=httpx.Limits(
                        max_connections=settings.http_proxy_max_connections,
                        max_keepalive_connections=(
                            settings.http_proxy_max_keepalive_connections
                        ),
                        keepalive_expiry=settings.http_keepalive_expiry_seconds,
                    ),
                    timeout=httpx.Timeout(settings.http_timeout_seconds),
                    proxy=proxy_url,
                    verify=_get_ssl_context(),
                )
            )
            _proxy_clients[proxy_url] = entry
            _enforce_proxy_client_cap()
        else:
            entry.last_used = time.monotonic()
        return entry.client
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=settings.http_max_connections,
                max_keepalive_connections=settings.http_max_keepalive_connections,
                keepalive_expiry=settings.http_keepalive_expiry_seconds,
            ),
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            verify=_get_ssl_context(),
        )
    return _client


@contextlib.asynccontextmanager
async def client_in_use(proxy_url: str | None):
    """Protect a cached proxy client from eviction while a request runs."""
    entry = _proxy_clients.get(proxy_url) if proxy_url else None
    if entry is None:
        yield
        return
    entry.in_flight += 1
    try:
        yield
    finally:
        entry.in_flight -= 1
        entry.last_used = time.monotonic()


async def proxy_request(
    method: str,
    url: str,
    *,
    proxy_url: str | None = None,
    **kwargs,
) -> httpx.Response:
    client = get_client(proxy_url)
    try:
        async with client_in_use(proxy_url):
            response = await client.request(method, url, **kwargs)
    # Some SOCKS transport failures bypass httpx.HTTPError.
    except (httpx.HTTPError, OSError, ProtocolError):
        if proxy_url:
            from app.services.proxy_service import record_proxy_url_usage

            await record_proxy_url_usage(proxy_url, False)
        raise
    if proxy_url:
        from app.services.proxy_service import record_proxy_url_usage

        reachable = response.status_code < 500 and response.status_code != 407
        await record_proxy_url_usage(proxy_url, reachable)
    return response


async def evict_idle_proxy_clients(max_idle: float | None = None) -> int:
    """Close cached proxy clients that are unused and idle.

    Returns the number of clients closed. Called by the maintenance loop so a
    burst of proxy health checks does not leave hundreds of pools behind.
    """
    while _pending_close:
        with contextlib.suppress(Exception):
            await _pending_close.pop().aclose()

    if max_idle is None:
        max_idle = float(settings.http_proxy_client_idle_seconds)
    now = time.monotonic()
    stale = [
        url
        for url, entry in _proxy_clients.items()
        if entry.in_flight == 0 and now - entry.last_used >= max_idle
    ]
    for url in stale:
        entry = _proxy_clients.pop(url)
        with contextlib.suppress(Exception):
            await entry.client.aclose()
    if stale:
        logger.debug(
            "Closed {} idle proxy client(s), {} cached",
            len(stale),
            len(_proxy_clients),
        )
    return len(stale)


def cached_proxy_client_count() -> int:
    return len(_proxy_clients)


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
    entries = list(_proxy_clients.values())
    _proxy_clients.clear()
    for entry in entries:
        with contextlib.suppress(Exception):
            await entry.client.aclose()
    while _pending_close:
        with contextlib.suppress(Exception):
            await _pending_close.pop().aclose()
