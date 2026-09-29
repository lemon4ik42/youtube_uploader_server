"""Functional tests for the refactored proxy switching/selection.

These verify that ``switch_channel`` and ``choose_working`` still assign the
correct proxy after being reworked to use short-lived sessions (no database
connection held during network health checks).
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models.channel  # noqa: F401
import app.models.channel_project_auth  # noqa: F401
import app.models.oauth_state  # noqa: F401
import app.models.project  # noqa: F401
import app.models.proxy  # noqa: F401
import app.models.rejected_hash  # noqa: F401
import app.models.upload_task  # noqa: F401
import app.models.uploaded_hash  # noqa: F401
import app.services.proxy_service as ps
from app.models.channel import Channel
from app.models.proxy import Proxy
from datetime import datetime, timezone


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


def make_client_factory(checked: list[str]):
    class FakeClient:
        def __init__(self, url: str):
            self._url = url

        async def get(self, *args, **kwargs):
            checked.append(self._url)
            return FakeResponse(500 if "bad" in self._url else 200)

    return lambda url: FakeClient(url)


async def _setup(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'proxy.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    session_maker = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(ps, "async_session_maker", session_maker)
    monkeypatch.setattr(ps, "decrypt", lambda value: value)

    good = Proxy(id=uuid4(), url_enc="socks5://good.example:1080", status="unknown")
    bad = Proxy(id=uuid4(), url_enc="socks5://bad.example:1080", status="unknown")
    async with session_maker() as s:
        s.add(good)
        s.add(bad)
        await s.commit()
    return engine, session_maker, good, bad


def make_channel(proxy_id=None, recovery_pending=True) -> Channel:
    return Channel(
        youtube_channel_id=f"yt-{uuid4().hex[:8]}",
        title="Test",
        refresh_token_enc="r",
        access_token_enc="a",
        access_token_expires_at=datetime.now(timezone.utc),
        proxy_id=proxy_id,
        proxy_auto_switch=True,
        proxy_recovery_pending=recovery_pending,
    )


@pytest.mark.asyncio
async def test_switch_channel_assigns_working_proxy(tmp_path, monkeypatch):
    engine, session_maker, good, bad = await _setup(tmp_path, monkeypatch)
    checked: list[str] = []
    monkeypatch.setattr(ps, "get_client", make_client_factory(checked))

    channel = make_channel(proxy_id=bad.id, recovery_pending=True)
    async with session_maker() as s:
        s.add(channel)
        await s.commit()
        await s.refresh(channel)
        channel_id = channel.id

    selected = await ps.ProxyService().switch_channel(
        channel_id, only_if_recovery_pending=True, keep_current_if_working=True
    )

    assert selected is not None and selected.id == good.id
    async with session_maker() as s:
        updated = await s.get(Channel, channel_id)
        assert updated.proxy_id == good.id
        assert updated.proxy_recovery_pending is False
    await engine.dispose()


@pytest.mark.asyncio
async def test_switch_channel_keeps_current_when_it_works(tmp_path, monkeypatch):
    engine, session_maker, good, bad = await _setup(tmp_path, monkeypatch)
    checked: list[str] = []
    monkeypatch.setattr(ps, "get_client", make_client_factory(checked))

    # Channel currently on the GOOD proxy, waiting for recovery.
    channel = make_channel(proxy_id=good.id, recovery_pending=True)
    async with session_maker() as s:
        s.add(channel)
        await s.commit()
        await s.refresh(channel)
        channel_id = channel.id

    selected = await ps.ProxyService().switch_channel(
        channel_id, only_if_recovery_pending=True, keep_current_if_working=True
    )

    # The current (working) proxy is kept, not swapped.
    assert selected is not None and selected.id == good.id
    async with session_maker() as s:
        updated = await s.get(Channel, channel_id)
        assert updated.proxy_id == good.id
        assert updated.proxy_recovery_pending is False
    await engine.dispose()


@pytest.mark.asyncio
async def test_switch_channel_noop_when_not_pending(tmp_path, monkeypatch):
    engine, session_maker, good, bad = await _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(ps, "get_client", make_client_factory([]))

    channel = make_channel(proxy_id=bad.id, recovery_pending=False)
    async with session_maker() as s:
        s.add(channel)
        await s.commit()
        await s.refresh(channel)
        channel_id = channel.id

    selected = await ps.ProxyService().switch_channel(
        channel_id, only_if_recovery_pending=True, keep_current_if_working=True
    )
    assert selected is None
    async with session_maker() as s:
        updated = await s.get(Channel, channel_id)
        assert updated.proxy_id == bad.id  # unchanged
    await engine.dispose()


@pytest.mark.asyncio
async def test_choose_working_returns_working_proxy(tmp_path, monkeypatch):
    engine, session_maker, good, bad = await _setup(tmp_path, monkeypatch)
    checked: list[str] = []
    monkeypatch.setattr(ps, "get_client", make_client_factory(checked))

    selected = await ps.ProxyService().choose_working()
    assert selected is not None and selected.id == good.id
    await engine.dispose()


@pytest.mark.asyncio
async def test_choose_working_none_when_all_fail(tmp_path, monkeypatch):
    engine, session_maker, good, bad = await _setup(tmp_path, monkeypatch)

    class AllBadClient:
        def __init__(self, url: str):
            pass

        async def get(self, *args, **kwargs):
            return FakeResponse(500)

    monkeypatch.setattr(ps, "get_client", lambda url: AllBadClient(url))

    selected = await ps.ProxyService().choose_working()
    assert selected is None
    await engine.dispose()
