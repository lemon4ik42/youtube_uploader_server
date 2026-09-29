"""Proxy health checks must be concurrency-bounded.

Checking a very large proxy list must not open one socket per proxy at once
(that exhausts file descriptors/ephemeral ports and misreports healthy proxies
as failed). The number of simultaneous checks is limited by
``proxy_check_concurrency``, and each proxy must still be classified correctly.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import app.services.proxy_service as ps
from app.core.config import Settings


class FakeSession:
    """Session stub: ``get`` returns None so no DB write/broadcast happens."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, model, proxy_id):
        return None

    async def exec(self, query):
        class _Empty:
            def all(self):
                return []

            def scalars(self):
                return self

        return _Empty()

    async def commit(self):
        return None


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


@pytest.mark.asyncio
async def test_shared_checks_are_bounded_and_correct(monkeypatch, tmp_path):
    settings = Settings(
        base_dir=str(tmp_path),
        app_secret_key="x",
        api_token="x",
        proxy_check_concurrency=10,
        proxy_check_timeout_seconds=5,
    )
    monkeypatch.setattr(ps, "settings", settings)
    monkeypatch.setattr(ps, "async_session_maker", lambda: FakeSession())

    concurrency = 0
    max_concurrency = 0

    class FakeClient:
        def __init__(self, url: str):
            self._bad = "bad" in url

        async def get(self, *args, **kwargs):
            nonlocal concurrency, max_concurrency
            concurrency += 1
            if concurrency > max_concurrency:
                max_concurrency = concurrency
            await asyncio.sleep(0.02)
            concurrency -= 1
            return FakeResponse(500 if self._bad else 200)

    monkeypatch.setattr(ps, "get_client", lambda url: FakeClient(url))

    total = 60
    urls = [
        f"socks5://{'bad' if i % 2 else 'good'}-{i}.example:1080" for i in range(total)
    ]
    results = await asyncio.gather(
        *(ps._perform_shared_check(f"id-{i}", urls[i]) for i in range(total))
    )

    expected = [False if "bad" in url else True for url in urls]
    assert results == expected, "each proxy must be classified correctly"
    assert max_concurrency <= 10, "simultaneous checks must respect the bound"
    assert max_concurrency > 1, "checks should still run concurrently"


@pytest.mark.asyncio
async def test_check_failure_from_network_error_is_bounded(monkeypatch, tmp_path):
    """Network errors (e.g. exhausted resources) must also stay within the bound."""
    settings = Settings(
        base_dir=str(tmp_path),
        app_secret_key="x",
        api_token="x",
        proxy_check_concurrency=5,
        proxy_check_timeout_seconds=5,
    )
    monkeypatch.setattr(ps, "settings", settings)
    monkeypatch.setattr(ps, "async_session_maker", lambda: FakeSession())

    concurrency = 0
    max_concurrency = 0

    class BrokenClient:
        async def get(self, *args, **kwargs):
            nonlocal concurrency, max_concurrency
            concurrency += 1
            if concurrency > max_concurrency:
                max_concurrency = concurrency
            await asyncio.sleep(0.01)
            concurrency -= 1
            raise OSError("simulated resource exhaustion")

    monkeypatch.setattr(ps, "get_client", lambda url: BrokenClient())

    results = await asyncio.gather(
        *(ps._perform_shared_check(f"id-{i}", f"socks5://p{i}") for i in range(30))
    )

    assert results == [False] * 30
    assert max_concurrency <= 5


class _EmptyCounts:
    def all(self):
        return []


class _FakeServiceSession:
    async def exec(self, query):
        return _EmptyCounts()


@pytest.mark.asyncio
async def test_verify_working_stops_early_on_large_list(monkeypatch, tmp_path):
    """With many proxies, the search stops once a working proxy is found."""
    settings = Settings(
        base_dir=str(tmp_path),
        app_secret_key="x",
        api_token="x",
        proxy_check_concurrency=5,
        proxy_check_timeout_seconds=5,
    )
    monkeypatch.setattr(ps, "settings", settings)
    monkeypatch.setattr(ps, "async_session_maker", lambda: FakeSession())
    monkeypatch.setattr(ps, "decrypt", lambda value: value)

    checked_urls: list[str] = []

    class FakeClient:
        def __init__(self, url: str):
            self._url = url

        async def get(self, *args, **kwargs):
            checked_urls.append(self._url)
            return FakeResponse(500 if "bad" in self._url else 200)

    monkeypatch.setattr(ps, "get_client", lambda url: FakeClient(url))

    service = ps.ProxyService(_FakeServiceSession())
    now = datetime.now(timezone.utc)

    proxies = [
        SimpleNamespace(
            id=f"bad-{i}", status="failed", failure_count=1,
            created_at=now, url_enc=f"socks5://bad-{i}", is_active=True,
        )
        for i in range(5)
    ] + [
        SimpleNamespace(
            id=f"good-{i}", status="working", failure_count=0,
            created_at=now, url_enc=f"socks5://good-{i}", is_active=True,
        )
        for i in range(7)
    ]

    working = await service._verify_working(proxies, {})

    assert working, "expected at least one working proxy"
    assert all("good" in p.url_enc for p in working)
    # Working-status candidates are checked first; the scan stops after the first
    # working wave instead of checking all 12 proxies.
    assert len(checked_urls) < len(proxies)
    assert all("good" in url for url in checked_urls)
