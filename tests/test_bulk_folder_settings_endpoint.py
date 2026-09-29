"""PUT /channels/bulk-folder-settings — bulk video folder switching.

Bulk twin of the ``folder_from_channel_id`` field on ``PUT
/channels/{channel_id}``: every listed channel either switches to the
referenced channel's folder or (explicit ``null``) back to its own dedicated
folder under VIDEO_BASE_DIR.

Semantics pinned here:

* only the listed channels' folders change — the source channel's folder is
  never modified;
* validation is all-or-nothing and covers every listed channel BEFORE
  anything is applied: one bad choice (self-reference, missing/deleted
  source, circular reference) rejects the whole request with a specific
  error naming the offending channel, and no channel is saved;
* routing: ``/bulk-folder-settings`` must not be captured by the
  ``/{channel_id}`` PUT route, and changes must persist to the database.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models  # noqa: F401
from app.api.deps import get_session
from app.core import security
from app.core.config import Settings
from app.main import app
from app.models.channel import Channel
from app.models.project import GCPProject


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


@pytest.fixture
async def app_client(tmp_path, monkeypatch):
    db_path = tmp_path / "bulk-folder.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    import app.services.channel_service as cs_mod

    test_settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="test-secret",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{db_path}",
    )
    monkeypatch.setattr(cs_mod, "settings", test_settings)
    import app.api.routes.channels as routes_mod

    monkeypatch.setattr(routes_mod, "settings", test_settings)

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
        for name in ("a", "b", "c", "d"):
            ch = Channel(
                youtube_channel_id=f"yt-{uuid4().hex[:8]}",
                title=name,
                project_id=project.id,
                refresh_token_enc="r",
                access_token_enc=_enc("a"),
                access_token_expires_at=datetime.now(timezone.utc)
                + timedelta(hours=1),
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


async def _folders_of(Session, *channel_ids: str) -> dict[str, str | None]:
    from uuid import UUID as UUIDType

    async with Session() as s:
        result = {}
        for cid in channel_ids:
            ch = await s.get(Channel, UUIDType(cid))
            result[cid] = ch.folder_path
        return result


def _assert_not_saved(resp_status_code, folders_before, folders_after, message):
    assert resp_status_code == 400, message
    assert folders_after == folders_before, (
        "a rejected bulk folder request must not save ANY channel"
    )


@pytest.mark.asyncio
async def test_bulk_adopts_source_folder_and_never_touches_source(app_client):
    """A and B switch to C's folder; C's folder stays as-is."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, b], "folder_from_channel_id": c},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["not_found"] == []
    assert len(body["updated"]) == 2
    for ch in body["updated"]:
        assert ch["folder_path"] == f"videos/{c}"

    folders = await _folders_of(app_client.TestSession, a, b, c)
    assert folders[a] == f"videos/{c}"
    assert folders[b] == f"videos/{c}"
    assert folders[c] == f"videos/{c}"  # untouched


@pytest.mark.asyncio
async def test_bulk_null_returns_every_channel_to_dedicated_folder(app_client):
    """Explicit null: every listed channel gets its own dedicated folder."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]

    # First share C's folder, then switch back.
    assert (
        await app_client.client.put(
            "/channels/bulk-folder-settings",
            json={"channel_ids": [a, b], "folder_from_channel_id": c},
        )
    ).status_code == 200

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, b], "folder_from_channel_id": None},
    )
    assert resp.status_code == 200, resp.text

    folders = await _folders_of(app_client.TestSession, a, b)
    assert folders[a] == f"videos/{a}"
    assert folders[b] == f"videos/{b}"
    assert (app_client.tmp_path / "videos" / a).is_dir()
    assert (app_client.tmp_path / "videos" / b).is_dir()


@pytest.mark.asyncio
async def test_bulk_all_channels_not_found_returns_404(app_client):
    missing = str(uuid4())
    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [missing], "folder_from_channel_id": None},
    )
    assert resp.status_code == 404
    assert "No channels found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_bulk_reports_mixed_not_found_but_applies_valid(app_client):
    """Missing ids land in not_found; the existing ones are still saved."""
    a, c = app_client.ids["a"], app_client.ids["c"]
    missing = str(uuid4())

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, missing], "folder_from_channel_id": c},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["not_found"] == [missing]
    assert len(body["updated"]) == 1
    assert (await _folders_of(app_client.TestSession, a))[a] == f"videos/{c}"


@pytest.mark.asyncio
async def test_bulk_self_reference_rejects_everything(app_client):
    """A channel in the list referencing itself as source saves nobody."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]
    before = await _folders_of(app_client.TestSession, a, b, c)

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, b], "folder_from_channel_id": a},
    )

    _assert_not_saved(
        resp.status_code,
        before,
        await _folders_of(app_client.TestSession, a, b, c),
        resp.text,
    )
    assert "cannot reference its own video folder" in resp.json()["detail"]
    assert a in resp.json()["detail"]


@pytest.mark.asyncio
async def test_bulk_circular_reference_rejects_everything(app_client):
    """B already uses A's folder; making A (and friends) adopt B's folder is
    circular — the whole request fails and no channel is saved."""
    a, b, c, d = (
        app_client.ids["a"],
        app_client.ids["b"],
        app_client.ids["c"],
        app_client.ids["d"],
    )

    # b -> a
    assert (
        await app_client.client.put(
            "/channels/bulk-folder-settings",
            json={"channel_ids": [b], "folder_from_channel_id": a},
        )
    ).status_code == 200

    before = await _folders_of(app_client.TestSession, a, b, c, d)

    # Would-be adoption: c and a both switch to b's folder. `a` adopting
    # b's folder is circular (b -> a), so nothing must be saved — not even
    # the safe `c`.
    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [c, a], "folder_from_channel_id": b},
    )

    _assert_not_saved(
        resp.status_code,
        before,
        await _folders_of(app_client.TestSession, a, b, c, d),
        resp.text,
    )
    assert "circular video folder reference" in resp.json()["detail"]
    assert a in resp.json()["detail"]


@pytest.mark.asyncio
async def test_bulk_source_in_list_cycle_rejects_everything(app_client):
    """Swapping two channels' folders through one bulk request must fail:
    B and C adopting each other's folders is a circular reference."""
    b, c, d = app_client.ids["b"], app_client.ids["c"], app_client.ids["d"]
    before = await _folders_of(app_client.TestSession, b, c, d)

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [b, c], "folder_from_channel_id": c},
    )
    # c references its own folder -> rejected.
    _assert_not_saved(
        resp.status_code,
        before,
        await _folders_of(app_client.TestSession, b, c, d),
        resp.text,
    )


@pytest.mark.asyncio
async def test_bulk_missing_source_returns_404(app_client):
    a = app_client.ids["a"]
    missing = str(uuid4())
    before = await _folders_of(app_client.TestSession, a)

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a], "folder_from_channel_id": missing},
    )
    assert resp.status_code == 404
    assert "Source channel for folder not found" in resp.json()["detail"]
    assert (await _folders_of(app_client.TestSession, a)) == before


@pytest.mark.asyncio
async def test_bulk_deleted_source_rejects_everything(app_client):
    from uuid import UUID as UUIDType

    a, d = app_client.ids["a"], app_client.ids["d"]
    before = await _folders_of(app_client.TestSession, a, d)

    async with app_client.TestSession() as s:
        source = await s.get(Channel, UUIDType(d))
        source.status = "deleted"
        await s.commit()

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a], "folder_from_channel_id": d},
    )
    _assert_not_saved(
        resp.status_code,
        before,
        await _folders_of(app_client.TestSession, a, d),
        resp.text,
    )
    assert "deleted" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_bulk_deduplicates_channel_ids(app_client):
    a, c = app_client.ids["a"], app_client.ids["c"]
    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, a], "folder_from_channel_id": c},
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["updated"]) == 1


@pytest.mark.asyncio
async def test_bulk_folder_missing_key_is_422(app_client):
    """folder_from_channel_id is required (nullable, but the key must exist):
    omitting it is a schema error, not a silent no-op."""
    a = app_client.ids["a"]
    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a]},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_bulk_folder_empty_ids_is_422(app_client):
    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [], "folder_from_channel_id": None},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_bulk_folder_persists_after_reload(app_client):
    """Routing + persistence end-to-end: /bulk-folder-settings is not
    captured by /{channel_id} and survives reading rows back."""
    a, b, c = app_client.ids["a"], app_client.ids["b"], app_client.ids["c"]

    resp = await app_client.client.put(
        "/channels/bulk-folder-settings",
        json={"channel_ids": [a, b], "folder_from_channel_id": c},
    )
    assert resp.status_code == 200, resp.text

    async with app_client.TestSession() as s:
        rows = (await s.exec(select(Channel))).scalars().all()
        by_id = {str(r.id): r.folder_path for r in rows}
        assert by_id[a] == f"videos/{c}"
        assert by_id[b] == f"videos/{c}"
        assert by_id[c] == f"videos/{c}"
