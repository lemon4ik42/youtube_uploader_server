"""POST /channels/auth/start must return the authorization link quickly.

The endpoint optionally pre-checks whether the channel's YouTube session is
still active (to answer 409 instead of showing a pointless re-authorization
form). That check talks to YouTube through the channel's proxy. It used to
run inline with no deadline, so with a dead or slow proxy the request hung
for up to the HTTP timeout — 300 seconds per network call — before the user
ever received the authorization URL. Starting an authorization should just
persist the OAuth state and return the link.

These tests pin the bounded behavior: the pre-check is limited to a short
deadline, and when it cannot answer in time (timeout, unreachable YouTube,
missing proxy) the authorization link is issued anyway. The pre-check still
answers 409 when YouTube confirms an active session, and marks the session
expired when the refresh token is rejected so the flow continues normally.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.api.routes.channels as channels_mod
from app.core import security
from app.core.config import Settings
from app.models.channel import Channel
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject
from app.services.channel_service import ChannelService
from app.services.youtube_auth import ChannelSessionExpiredError


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


@pytest.fixture
async def app_client(tmp_path, monkeypatch):
    from app.api.deps import get_session
    from app.main import app

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'auth.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with TestSession() as s:
        project = GCPProject(
            name="p",
            client_id="cid",
            client_secret_enc=_enc("cs"),
            auth_uri="https://example.com/auth",
            token_uri="https://example.com/token",
            redirect_uri="https://example.com/redirect",
            scope="scope",
            api_upload="https://example.com/upload",
        )
        s.add(project)
        await s.commit()
        await s.refresh(project)
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            project_id=project.id,
            refresh_token_enc="r",
            access_token_enc=_enc("a"),
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
        )
        s.add(channel)
        await s.commit()
        await s.refresh(channel)
        channel_id = channel.id

    # A tight pre-check deadline keeps the timeout test fast; the production
    # default is 8s. The auth-token check uses the global settings, so the
    # client headers follow it.
    from app.core.config import settings as global_settings

    api_token = global_settings.api_token
    monkeypatch.setattr(
        channels_mod,
        "settings",
        Settings(
            base_dir=str(tmp_path),
            app_secret_key="test-secret",
            api_token=api_token,
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'auth.db'}",
            oauth_session_precheck_timeout_seconds=0.2,
        ),
    )

    async def override_get_session():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    class _Ctx:
        pass

    ctx = _Ctx()
    ctx.channel_id = channel_id
    ctx.TestSession = TestSession
    ctx.client = AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {api_token}"},
    )
    yield ctx
    app.dependency_overrides.clear()
    await ctx.client.aclose()
    await engine.dispose()


async def _fake_token_ok(monkeypatch, channel_id):
    async def fake_ensure(self, channel, *, request_reauth=True):
        return "token"

    monkeypatch.setattr(ChannelService, "ensure_access_token", fake_ensure)


@pytest.mark.asyncio
async def test_precheck_timeout_still_issues_auth_url(app_client, monkeypatch):
    """A hanging YouTube check must not delay the authorization link."""
    await _fake_token_ok(monkeypatch, app_client.channel_id)

    async def hanging_fetch(access_token, proxy_url=None):
        await asyncio.sleep(30)  # simulates a dead proxy
        return {}

    monkeypatch.setattr(channels_mod, "fetch_youtube_channel_info", hanging_fetch)

    started = time.perf_counter()
    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": str(app_client.channel_id), "project_id": None},
    )
    elapsed = time.perf_counter() - started

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "authorization_url" in body
    assert body["state"]
    # Bounded by the pre-check deadline (0.2s here), not by the 30s hang.
    assert elapsed < 5, f"auth/start took {elapsed:.2f}s"

    # The state row really exists for the returned state string.
    async with app_client.TestSession() as s:
        from sqlalchemy import select

        state = (
            await s.exec(
                select(OAuthState).where(OAuthState.state == body["state"])
            )
        ).scalars().first()
        assert state is not None


@pytest.mark.asyncio
async def test_precheck_confirmed_active_returns_409(app_client, monkeypatch):
    """A fast, healthy answer still protects against needless re-auth."""
    await _fake_token_ok(monkeypatch, app_client.channel_id)

    async def ok_fetch(access_token, proxy_url=None):
        return {"id": "yc", "title": "t"}

    monkeypatch.setattr(channels_mod, "fetch_youtube_channel_info", ok_fetch)

    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": str(app_client.channel_id)},
    )
    assert resp.status_code == 409
    assert "still active" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_precheck_expired_marks_session_and_issues_url(app_client, monkeypatch):
    """A rejected refresh token clears the session and continues the flow."""
    async def expired_ensure(self, channel, *, request_reauth=True):
        raise ChannelSessionExpiredError("Refresh token rejected: invalid_grant")

    monkeypatch.setattr(ChannelService, "ensure_access_token", expired_ensure)

    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": str(app_client.channel_id)},
    )
    assert resp.status_code == 200, resp.text
    assert "authorization_url" in resp.json()

    async with app_client.TestSession() as s:
        channel = await s.get(Channel, app_client.channel_id)
        assert channel.session_status == "expired"
        assert channel.error_message


@pytest.mark.asyncio
async def test_precheck_network_error_still_issues_auth_url(app_client, monkeypatch):
    """A fast network failure (dead proxy) must not block the link either."""
    await _fake_token_ok(monkeypatch, app_client.channel_id)

    async def failing_fetch(access_token, proxy_url=None):
        raise RuntimeError("proxy connect failed")

    monkeypatch.setattr(channels_mod, "fetch_youtube_channel_info", failing_fetch)

    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": str(app_client.channel_id)},
    )
    assert resp.status_code == 200, resp.text
    assert "authorization_url" in resp.json()


@pytest.mark.asyncio
async def test_new_channel_auth_start_is_pure_db(app_client):
    """A new channel never touches the network: immediate link + state."""
    started = time.perf_counter()
    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_title": "Fresh channel"},
    )
    elapsed = time.perf_counter() - started

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["authorization_url"].startswith("https://example.com/auth")
    assert body["channel_title"] == "Fresh channel"
    assert elapsed < 2

    async with app_client.TestSession() as s:
        from sqlalchemy import select

        state = (
            await s.exec(
                select(OAuthState).where(OAuthState.state == body["state"])
            )
        ).scalars().first()
        assert state is not None
        assert state.channel_title == "Fresh channel"


@pytest.mark.asyncio
async def test_precheck_disabled_issues_url_immediately(app_client, monkeypatch):
    """Setting the timeout to 0 skips the pre-check entirely."""
    monkeypatch.setattr(
        channels_mod.settings, "oauth_session_precheck_timeout_seconds", 0
    )
    await _fake_token_ok(monkeypatch, app_client.channel_id)

    called = []

    async def ok_fetch(access_token, proxy_url=None):
        called.append(1)
        return {"id": "yc", "title": "t"}

    monkeypatch.setattr(channels_mod, "fetch_youtube_channel_info", ok_fetch)

    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": str(app_client.channel_id)},
    )
    assert resp.status_code == 200, resp.text
    assert "authorization_url" in resp.json()
    assert called == []
