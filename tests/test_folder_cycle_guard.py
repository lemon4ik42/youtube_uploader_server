"""Video folder sharing: one-way references only, never circular.

Letting channel A use channel B's folder adds the directed link A -> B. The
server must keep that graph acyclic:

* the referenced channel's own folder never changes (B keeps exactly what it
  had — the previous UI made it *look* like B started referencing A back,
  because it displayed any folder *sharer* as the folder owner);
* an adoption that would create a cycle (A -> B while B -> A, longer chains,
  or a channel referencing itself) is rejected with a clear 400.

The same rule guards POST /channels/auth/start (early feedback) and the
callback (re-checked at completion time, where a detected cycle skips the
adoption instead of failing the already-exchanged authorization).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.api.routes.channels as channels_mod
import app.services.channel_service as cs_mod
from app.core import security
from app.core.config import Settings
from app.models.channel import Channel
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


@pytest.fixture
async def app_client(tmp_path, monkeypatch):
    """Real app over a throwaway DB; channels get real dedicated folders."""
    from app.api.deps import get_session
    from app.main import app

    db_path = tmp_path / "folders.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    # Both the routes module and the service module must agree on video_base_dir
    # so dedicated-folder paths resolve identically.
    test_settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="test-secret",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{db_path}",
        oauth_session_precheck_timeout_seconds=0,
    )
    monkeypatch.setattr(channels_mod, "settings", test_settings)
    monkeypatch.setattr(cs_mod, "settings", test_settings)

    (tmp_path / "videos").mkdir(exist_ok=True)

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

        channels = {}
        for name in ("a", "b", "c"):
            ch = Channel(
                youtube_channel_id=f"yt-{uuid4().hex[:8]}",
                title=name,
                project_id=project.id,
                refresh_token_enc="r",
                access_token_enc=_enc("a"),
                access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                status="active",
                session_status="active",
            )
            s.add(ch)
            await s.commit()
            await s.refresh(ch)
            dedicated = tmp_path / "videos" / str(ch.id)
            dedicated.mkdir(parents=True, exist_ok=True)
            ch.folder_path = f"videos/{ch.id}"
            await s.commit()
            await s.refresh(ch)
            channels[name] = ch
        ids = {k: str(v.id) for k, v in channels.items()}

    async def override_get_session():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    from app.core.config import settings as global_settings

    class _Ctx:
        pass

    ctx = _Ctx()
    ctx.ids = ids
    ctx.TestSession = TestSession
    ctx.tmp_path = tmp_path
    ctx.client = AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {global_settings.api_token}"},
    )
    yield ctx
    app.dependency_overrides.clear()
    await ctx.client.aclose()
    await engine.dispose()


async def _folder_of(Session, channel_id: str) -> str | None:
    async with Session() as s:
        from uuid import UUID as UUIDType

        ch = await s.get(Channel, UUIDType(channel_id))
        return ch.folder_path


@pytest.mark.asyncio
async def test_adopting_folder_never_changes_source(app_client):
    """A switches to B's folder: only A's folder_path changes, B's is frozen."""
    a, b = app_client.ids["a"], app_client.ids["b"]
    b_folder_before = await _folder_of(app_client.TestSession, b)

    resp = await app_client.client.put(
        f"/channels/{a}", json={"folder_from_channel_id": b}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["folder_path"] == b_folder_before

    assert await _folder_of(app_client.TestSession, a) == b_folder_before
    assert await _folder_of(app_client.TestSession, b) == b_folder_before


@pytest.mark.asyncio
async def test_mutual_folder_reference_rejected(app_client):
    """A -> B is fine; B -> A afterwards must be rejected as circular."""
    a, b = app_client.ids["a"], app_client.ids["b"]

    resp = await app_client.client.put(
        f"/channels/{a}", json={"folder_from_channel_id": b}
    )
    assert resp.status_code == 200, resp.text

    resp = await app_client.client.put(
        f"/channels/{b}", json={"folder_from_channel_id": a}
    )
    assert resp.status_code == 400, resp.text
    assert "Circular video folder reference" in resp.json()["detail"]
    assert a in resp.json()["detail"] and b in resp.json()["detail"]

    # B's folder is untouched by the rejected request.
    assert await _folder_of(app_client.TestSession, b) == f"videos/{b}"


@pytest.mark.asyncio
async def test_self_reference_rejected(app_client):
    a = app_client.ids["a"]
    resp = await app_client.client.put(
        f"/channels/{a}", json={"folder_from_channel_id": a}
    )
    assert resp.status_code == 400, resp.text
    assert "Circular video folder reference" in resp.json()["detail"]
    assert await _folder_of(app_client.TestSession, a) == f"videos/{a}"


@pytest.mark.asyncio
async def test_three_channel_cycle_rejected(app_client):
    """C -> A while A -> B and B -> C forms a cycle: rejected."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]

    assert (
        await app_client.client.put(
            f"/channels/{a}", json={"folder_from_channel_id": b}
        )
    ).status_code == 200
    assert (
        await app_client.client.put(
            f"/channels/{b}", json={"folder_from_channel_id": c}
        )
    ).status_code == 200

    resp = await app_client.client.put(
        f"/channels/{c}", json={"folder_from_channel_id": a}
    )
    assert resp.status_code == 400, resp.text

    # Nothing was applied: C keeps its dedicated folder.
    assert await _folder_of(app_client.TestSession, c) == f"videos/{c}"


@pytest.mark.asyncio
async def test_chain_without_cycle_allowed(app_client):
    """Several channels may share one folder owner: A -> B and C -> B is fine."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]

    for cid in (a, c):
        resp = await app_client.client.put(
            f"/channels/{cid}", json={"folder_from_channel_id": b}
        )
        assert resp.status_code == 200, resp.text

    assert await _folder_of(app_client.TestSession, a) == f"videos/{b}"
    assert await _folder_of(app_client.TestSession, c) == f"videos/{b}"
    assert await _folder_of(app_client.TestSession, b) == f"videos/{b}"


@pytest.mark.asyncio
async def test_auth_start_rejects_circular_folder(app_client, monkeypatch):
    """Starting re-authorization with a folder that leads back is a 400."""
    a, b = app_client.ids["a"], app_client.ids["b"]

    # a -> b
    assert (
        await app_client.client.put(
            f"/channels/{a}", json={"folder_from_channel_id": b}
        )
    ).status_code == 200

    # b re-authorizes and asks for a's folder: would close the cycle.
    resp = await app_client.client.post(
        "/channels/auth/start",
        json={"channel_id": b, "folder_from_channel_id": a},
    )
    assert resp.status_code == 400, resp.text
    assert "Circular video folder reference" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_auth_callback_skips_folder_that_became_circular(
    app_client, monkeypatch
):
    """A cycle that appears between /auth/start and the callback skips the
    folder adoption instead of failing the already-exchanged authorization."""
    a, b = app_client.ids["a"], app_client.ids["b"]
    from uuid import UUID as UUIDType

    # b -> a: re-authorizing a with folder_from b would close the cycle.
    assert (
        await app_client.client.put(
            f"/channels/{b}", json={"folder_from_channel_id": a}
        )
    ).status_code == 200

    from sqlalchemy import select as sa_select

    async with app_client.TestSession() as s:
        project = (await s.exec(sa_select(GCPProject))).scalars().first()
        s.add(
            OAuthState(
                state="state-cycle",
                project_id=project.id,
                channel_id=UUIDType(a),
                code_verifier="v",
                redirect_uri="https://example.com/redirect",
                channel_title="",
                folder_from_channel_id=UUIDType(b),
                proxy_id=None,
                proxy_auto_switch=False,
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            )
        )
        await s.commit()

    import app.api.routes.channels as routes_mod

    async def fake_exchange(code, redirect_uri, code_verifier, project, ps, proxy_url=None):
        return {
            "access_token": "tok",
            "refresh_token": "ref",
            "expires_in": 3600,
        }

    async def fake_info(access_token, proxy_url=None):
        return {
            "id": "yt-reauthed-a",
            "title": "Channel A",
            "handle": "@a",
            "thumbnail_url": None,
            "subscriber_count": 0,
            "view_count": 0,
        }

    monkeypatch.setattr(routes_mod, "exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr(routes_mod, "fetch_youtube_channel_info", fake_info)
    # The callback wakes the channel worker and the metadata refresher; both
    # would open sessions through the production session maker.
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        channels_mod.coordinator, "wake_channel", AsyncMock()
    )
    monkeypatch.setattr(
        channels_mod.coordinator, "start_channel_metadata", AsyncMock()
    )

    resp = await app_client.client.post(
        "/channels/auth/callback", json={"state": "state-cycle", "code": "c"}
    )
    assert resp.status_code == 200, resp.text

    # The re-authorization succeeded and a kept its own folder: adopting b's
    # folder (which is a's dedicated folder) would have closed the cycle.
    assert resp.json()["folder_path"] == f"videos/{a}"
    assert await _folder_of(app_client.TestSession, b) == f"videos/{a}"
