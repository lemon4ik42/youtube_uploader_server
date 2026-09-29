"""Idle memory characteristics of the shared HTTP client cache.

A server with a large proxy list used to retain one ``ssl.SSLContext`` per
cached ``httpx.AsyncClient`` (>1 MB each) and never dropped a client once
created, so ~300 proxies cost hundreds of megabytes of RSS while the server was
completely idle. These tests pin the two properties that fix it: contexts are
shared, and the cache is bounded and drains when idle.
"""

from __future__ import annotations

import asyncio

import pytest

import app.services.http_client as hc
from app.core.config import Settings


@pytest.fixture
def bounded_cache(monkeypatch, tmp_path):
    settings = Settings(
        base_dir=str(tmp_path),
        app_secret_key="x",
        api_token="x",
        http_proxy_client_cache_size=4,
        http_proxy_client_idle_seconds=0,
    )
    monkeypatch.setattr(hc, "settings", settings)
    monkeypatch.setattr(hc, "_proxy_clients", {})
    monkeypatch.setattr(hc, "_pending_close", [])
    return settings


def _rss_mb() -> int:
    with open("/proc/self/status", encoding="ascii") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) // 1024
    raise RuntimeError("VmRSS unavailable")


def test_all_clients_share_one_ssl_context(bounded_cache, monkeypatch):
    monkeypatch.setattr(
        hc,
        "settings",
        Settings(
            base_dir=".",
            app_secret_key="x",
            api_token="x",
            http_proxy_client_cache_size=100,
        ),
    )
    contexts = {
        id(hc.get_client(f"socks5://u:p@10.0.0.{i}:1080")._transport._pool._ssl_context)
        for i in range(20)
    }
    direct = hc.get_client()._transport._pool._ssl_context
    assert len(contexts) == 1, "each proxy client must reuse the shared context"
    assert id(direct) in contexts, "the direct client shares it too"


@pytest.mark.asyncio
async def test_many_proxy_clients_stay_within_cache_cap(bounded_cache):
    for i in range(200):
        hc.get_client(f"socks5://u:p@10.0.0.{i % 250}:{1080 + i}")
    assert hc.cached_proxy_client_count() <= 4
    await hc.close_client()


@pytest.mark.asyncio
async def test_idle_clients_are_evicted(bounded_cache):
    hc.get_client("socks5://u:p@10.0.0.1:1080")
    hc.get_client("socks5://u:p@10.0.0.2:1080")
    assert hc.cached_proxy_client_count() == 2

    closed = await hc.evict_idle_proxy_clients(max_idle=0)
    assert closed == 2
    assert hc.cached_proxy_client_count() == 0


@pytest.mark.asyncio
async def test_in_flight_client_is_not_evicted(bounded_cache):
    url = "socks5://u:p@10.0.0.9:1080"
    client = hc.get_client(url)

    async with hc.client_in_use(url):
        assert await hc.evict_idle_proxy_clients(max_idle=0) == 0
        assert hc.cached_proxy_client_count() == 1
        assert not client.is_closed

    assert await hc.evict_idle_proxy_clients(max_idle=0) == 1


@pytest.mark.asyncio
async def test_three_hundred_proxies_do_not_cost_hundreds_of_megabytes(
    bounded_cache, monkeypatch
):
    """Creating clients for 300 proxies must not blow up RSS.

    Before the fix this allocated ~300 SSL contexts and retained every client,
    costing over 400 MB. With a shared context and a bounded cache the growth is
    a rounding error.
    """
    monkeypatch.setattr(
        hc,
        "settings",
        Settings(
            base_dir=".",
            app_secret_key="x",
            api_token="x",
            http_proxy_client_cache_size=32,
            http_proxy_client_idle_seconds=0,
        ),
    )
    # Build the shared context first so its one-time cost is not attributed to
    # the loop below.
    hc._get_ssl_context()
    baseline = _rss_mb()

    for i in range(300):
        hc.get_client(f"socks5://u:p@10.0.0.{i % 250}:{1080 + i}")
    await asyncio.sleep(0)

    growth = _rss_mb() - baseline
    assert hc.cached_proxy_client_count() <= 32
    assert growth < 40, f"300 proxy clients grew RSS by {growth} MB"

    await hc.close_client()
