"""A full proxy scan must stay bounded in memory and must not starve the API.

The old implementation created one task per proxy up front, recorded each result
in its own database session, and started a recovery loop for every failure while
the scan was still running. Each of those recovery loops rescans the whole proxy
list, so one manual "check all" over a few hundred proxies turned into a storm of
overlapping scans: memory climbed into the hundreds of megabytes, the scan took
far longer than ``proxies / concurrency * timeout``, and unrelated endpoints
(``PUT /channels/bulk-settings``) were starved because the event loop had to
service the whole backlog first.
"""

from __future__ import annotations

import asyncio

import pytest

import app.services.proxy_service as ps
from app.core.config import Settings


class _Empty:
    def all(self):
        return []

    def scalars(self):
        return self


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, model, key, **kwargs):
        return None

    async def exec(self, query):
        return _Empty()

    async def commit(self):
        return None


@pytest.fixture
def scan_settings(monkeypatch, tmp_path):
    settings = Settings(
        base_dir=str(tmp_path),
        app_secret_key="x",
        api_token="x",
        proxy_check_concurrency=10,
        proxy_check_timeout_seconds=5,
    )
    monkeypatch.setattr(ps, "settings", settings)
    monkeypatch.setattr(ps, "async_session_maker", lambda: FakeSession())
    monkeypatch.setattr(ps, "decrypt", lambda value: value)
    monkeypatch.setattr(ps, "_running_checks", {})
    return settings


@pytest.mark.asyncio
async def test_check_all_never_exceeds_the_concurrency_bound(scan_settings, monkeypatch):
    """In-flight checks stay within the bound for the whole scan."""
    total = 250
    targets = [(f"id-{i}", f"socks5://p{i}.example:1080") for i in range(total)]

    async def fake_exec(self, query):
        class _Rows:
            def all(self_inner):
                return targets

            def scalars(self_inner):
                return _Empty()

        return _Rows()

    monkeypatch.setattr(FakeSession, "exec", fake_exec)

    in_flight = 0
    peak = 0

    async def fake_check(proxy_url: str) -> bool:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.001)
        in_flight -= 1
        return "bad" not in proxy_url

    recorded: list[tuple[str, bool]] = []
    record_calls = 0

    async def fake_record(results, broadcast=True):
        nonlocal record_calls
        record_calls += 1
        recorded.extend(results)

    recovery_started: list[str] = []

    async def fake_recovery(proxy_id):
        recovery_started.append(proxy_id)

    monkeypatch.setattr(ps, "_check_proxy_network", fake_check)
    monkeypatch.setattr(ps, "_record_check_results", fake_record)
    monkeypatch.setattr(ps, "start_proxy_recovery_for_proxy", fake_recovery)
    monkeypatch.setattr(ps, "evict_idle_proxy_clients", lambda: _noop())
    monkeypatch.setattr(ps, "trim_memory", lambda: False)
    monkeypatch.setattr(
        ps.ProxyService, "list", lambda self: _empty_list()
    )

    payload = await ps._perform_check_all()

    assert payload["checked"] == total
    assert payload["working"] == total
    assert peak <= 10, f"in-flight checks peaked at {peak}, bound is 10"
    assert len(recorded) == total
    # Results are written per wave, not per proxy.
    assert record_calls <= (total // 10) + 1


@pytest.mark.asyncio
async def test_check_all_does_not_start_recovery_mid_scan(scan_settings, monkeypatch):
    """Recovery must start only after every proxy has been classified.

    Recovery rescans the proxy list, so starting it while waves are still
    running multiplies the scan by the number of failures.
    """
    total = 30
    targets = [(f"id-{i}", f"socks5://bad{i}.example:1080") for i in range(total)]

    async def fake_exec(self, query):
        class _Rows:
            def all(self_inner):
                return targets

            def scalars(self_inner):
                return _Empty()

        return _Rows()

    monkeypatch.setattr(FakeSession, "exec", fake_exec)

    checks_done = 0
    recovery_at: list[int] = []

    async def fake_check(proxy_url: str) -> bool:
        nonlocal checks_done
        await asyncio.sleep(0.001)
        checks_done += 1
        return False

    async def fake_recovery(proxy_id):
        # Snapshot how many checks had finished when recovery was requested.
        recovery_at.append(checks_done)

    async def fake_record(results, broadcast=True):
        return None

    monkeypatch.setattr(ps, "_check_proxy_network", fake_check)
    monkeypatch.setattr(ps, "_record_check_results", fake_record)
    monkeypatch.setattr(ps, "start_proxy_recovery_for_proxy", fake_recovery)
    monkeypatch.setattr(ps, "evict_idle_proxy_clients", lambda: _noop())
    monkeypatch.setattr(ps, "trim_memory", lambda: False)
    monkeypatch.setattr(ps.ProxyService, "list", lambda self: _empty_list())

    payload = await ps._perform_check_all()

    assert payload["failed"] == total
    assert len(recovery_at) == total
    assert all(seen == total for seen in recovery_at), (
        "recovery was requested before the scan classified every proxy: "
        f"{sorted(set(recovery_at))}"
    )


@pytest.mark.asyncio
async def test_other_endpoints_progress_during_a_scan(scan_settings, monkeypatch):
    """A concurrent request must keep making progress while a scan runs.

    The scan awaits between waves, so an unrelated coroutine (standing in for
    ``PUT /channels/bulk-settings``) is scheduled repeatedly instead of only
    after the whole scan has finished.
    """
    targets = [(f"id-{i}", f"socks5://p{i}.example:1080") for i in range(200)]

    async def fake_exec(self, query):
        class _Rows:
            def all(self_inner):
                return targets

            def scalars(self_inner):
                return _Empty()

        return _Rows()

    monkeypatch.setattr(FakeSession, "exec", fake_exec)

    async def fake_check(proxy_url: str) -> bool:
        await asyncio.sleep(0.002)
        return True

    async def fake_record(results, broadcast=True):
        return None

    monkeypatch.setattr(ps, "_check_proxy_network", fake_check)
    monkeypatch.setattr(ps, "_record_check_results", fake_record)
    monkeypatch.setattr(ps, "start_proxy_recovery_for_proxy", lambda pid: _noop())
    monkeypatch.setattr(ps, "evict_idle_proxy_clients", lambda: _noop())
    monkeypatch.setattr(ps, "trim_memory", lambda: False)
    monkeypatch.setattr(ps.ProxyService, "list", lambda self: _empty_list())

    ticks = 0
    stop = False

    async def other_endpoint():
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0.001)

    worker = asyncio.create_task(other_endpoint())
    await ps._perform_check_all()
    stop = True
    await worker

    assert ticks > 10, (
        f"unrelated endpoint only got {ticks} slices during the scan; "
        "the scan is starving the event loop"
    )


async def _noop():
    return None


async def _empty_list():
    return []
