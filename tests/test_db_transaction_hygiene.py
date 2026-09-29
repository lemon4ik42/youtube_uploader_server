"""Read transactions must not stay open across slow non-database work.

SQLite WAL snapshots (open read transactions) block WAL checkpoints for as
long as they live. Several code paths used to hold one across minutes-long
work that has nothing to do with the database — which starves checkpoints,
grows the WAL without bound, and slows every connection (NullPool opens one
per session) and every write, including channel-settings saves:

* the channel metadata loop refreshes OAuth tokens and fetches snippets
  through proxies whose HTTP timeout is 300s — the read transaction opened by
  the channel/project lookup stayed open across both network calls;
* the manual-upload endpoints streamed multi-gigabyte client files to disk
  while the channel-validation read transaction was still open;
* the worker hashed a large batch of new video files with the limit-check
  transaction still open.

These tests pin the fix: the transaction is committed (ended) before the slow
work starts. With ``expire_on_commit=False`` (the production session factory)
the ORM objects stay usable afterwards.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.services.channel_service as cs_mod
import app.services.coordinator as coord_mod
from app.core import security
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.upload_task import UploadTask
from app.services.channel_service import ChannelService
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import notifications


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


@pytest.fixture
async def db_env(tmp_path, monkeypatch):
    """A throwaway SQLite database with the production engine settings."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'hygiene.db'}"
    engine = create_async_engine(url, connect_args={"timeout": 30})

    @event.listens_for(engine.sync_engine, "connect")
    def _configure(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    import app.core.db as db_mod

    monkeypatch.setattr(db_mod, "async_session_maker", Session)

    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=url,
    )
    monkeypatch.setattr(cs_mod, "settings", settings)
    monkeypatch.setattr(coord_mod, "settings", settings)

    return Session


async def _seed_channel(Session, *, token_expires_in_hours: float = 1) -> tuple:
    async with Session() as session:
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
        session.add(project)
        await session.commit()
        await session.refresh(project)

        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            project_id=project.id,
            refresh_token_enc="r",
            access_token_enc=_enc("a"),
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=token_expires_in_hours),
            status="active",
            session_status="active",
        )
        session.add(channel)
        await session.commit()
        await session.refresh(channel)
        return channel.id, project.id


@pytest.mark.asyncio
async def test_ensure_access_token_ends_transaction_before_cached_return(db_env):
    """Returning a still-valid token must not leave a WAL snapshot open."""
    channel_id, _ = await _seed_channel(db_env)

    async with db_env() as session:
        channel = await session.get(Channel, channel_id)
        assert session.in_transaction()  # the lookup opened one

        service = ChannelService(session)
        token = await service.ensure_access_token(channel, request_reauth=False)

        assert token == "a"
        assert not session.in_transaction()


@pytest.mark.asyncio
async def test_ensure_access_token_ends_transaction_during_refresh(db_env, monkeypatch):
    """The OAuth refresh is network work; no transaction may be open for it."""
    channel_id, _ = await _seed_channel(db_env, token_expires_in_hours=-1)

    observed = []

    async def fake_refresh(channel, project, project_service, proxy_url=None):
        observed.append(channel._sa_instance_state.session.in_transaction())
        channel.access_token_enc = _enc("new-token")
        channel.access_token_expires_at = datetime.now(
            timezone.utc
        ) + timedelta(hours=1)
        return "new-token"

    monkeypatch.setattr(cs_mod, "refresh_access_token", fake_refresh)

    async with db_env() as session:
        channel = await session.get(Channel, channel_id)
        service = ChannelService(session)
        token = await service.ensure_access_token(channel, request_reauth=False)

    assert token == "new-token"
    assert observed == [False]


@pytest.mark.asyncio
async def test_refresh_channel_snippet_no_transaction_during_fetch(
    db_env, monkeypatch
):
    """The YouTube snippet request is network work; keep it transaction-free."""
    channel_id, _ = await _seed_channel(db_env)

    observed = []

    async def fake_fetch(access_token, proxy_url=None):
        return {
            "id": "yc",
            "title": "New title",
            "handle": "@new",
            "thumbnail_url": "https://example.com/t.jpg",
            "subscriber_count": 1,
            "view_count": 2,
        }

    import app.services.youtube_auth as ya_mod

    async with db_env() as session:
        channel = await session.get(Channel, channel_id)  # opens a transaction
        service = ChannelService(session)

        # Capture the session state the moment the (faked) network call runs.
        async def probing_fetch(access_token, proxy_url=None):
            observed.append(session.in_transaction())
            return await fake_fetch(access_token, proxy_url)

        monkeypatch.setattr(ya_mod, "fetch_youtube_channel_info", probing_fetch)

        changed = await service.refresh_channel_snippet(channel, "token")

    assert changed is True
    assert observed == [False]
    assert channel.youtube_title == "New title"


@pytest.mark.asyncio
async def test_collect_new_files_hashes_without_open_transaction(
    db_env, tmp_path, monkeypatch
):
    """Hashing a new batch can take minutes; no WAL snapshot may be held."""
    channel_id, _ = await _seed_channel(db_env)
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    (folder / "v0.mp4").write_bytes(b"data")

    async with db_env() as session:
        channel = await session.get(Channel, channel_id)
        channel.folder_path = f"videos/{channel_id}"
        await session.commit()
        await session.refresh(channel)
        assert session.in_transaction()  # refresh re-opened one

        observed = []

        async def fake_hash(path):
            observed.append(session.in_transaction())
            return "hash0"

        monkeypatch.setattr(coord_mod, "_compute_file_hash", fake_hash)

        coordinator = UploadCoordinator(notifications)
        files, limit = await coordinator._collect_new_files(
            session, channel, ChannelService(session)
        )

    assert files == [(f"videos/{channel_id}/v0.mp4", "hash0")]
    assert limit > 0
    assert observed and all(flag is False for flag in observed)


@pytest.mark.asyncio
async def test_cleanup_old_tasks_bulk_deletes_oldest(db_env):
    """Cleanup keeps the newest tasks and removes the oldest terminal ones."""
    channel_id, _ = await _seed_channel(db_env)

    async with db_env() as session:
        base = datetime.now(timezone.utc) - timedelta(hours=2)
        for i in range(600):
            session.add(
                UploadTask(
                    channel_id=channel_id,
                    file_path=f"videos/v{i}.mp4",
                    file_hash=f"h{i}",
                    status="completed",
                    created_at=base + timedelta(seconds=i),
                    completed_at=base + timedelta(seconds=i),
                )
            )
        await session.commit()

    async with db_env() as session:
        service = ChannelService(session)
        await service.cleanup_old_tasks(channel_id, max_tasks=500)

    async with db_env() as session:
        from sqlalchemy import func, select

        remaining = (
            await session.exec(
                select(func.count(UploadTask.id)).where(
                    UploadTask.channel_id == channel_id
                )
            )
        ).scalar_one()
        assert remaining == 500

        oldest = (
            await session.exec(
                select(UploadTask).where(UploadTask.channel_id == channel_id)
                .order_by(UploadTask.created_at.asc())
                .limit(1)
            )
        ).scalars().first()
        # The oldest 100 (v0..v99) are gone; v100 is now the oldest.
        assert oldest.file_path == "videos/v100.mp4"


@pytest.mark.asyncio
async def test_manual_upload_streams_file_without_open_transaction(
    db_env, tmp_path, monkeypatch
):
    """Streaming a client file to disk must not hold a WAL snapshot."""
    from app.api.deps import get_session
    from app.api.routes import uploads as uploads_mod
    from app.main import app

    channel_id, _ = await _seed_channel(db_env)
    async with db_env() as session:
        channel = await session.get(Channel, channel_id)
        channel.folder_path = f"videos/{channel_id}"
        await session.commit()

    observed = []

    async def fake_save(channel, file, session):
        observed.append(session.in_transaction())
        return "v.mp4", False, None

    monkeypatch.setattr(uploads_mod, "_save_upload_to_channel_folder", fake_save)

    async def override_get_session():
        async with db_env() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        from app.core.config import settings

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {settings.api_token}"},
        ) as client:
            resp = await client.post(
                f"/uploads/{channel_id}/upload",
                files={"file": ("v.mp4", b"video-data", "video/mp4")},
            )
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 201, resp.text
    assert observed == [False]
