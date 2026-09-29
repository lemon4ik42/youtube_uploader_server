"""Video folder changes must survive while a channel batch is running.

The worker holds the Channel ORM object for the whole batch (which can span
minutes: inter-upload delays plus the uploads themselves). After each
completed upload it broadcasts ``channel.updated`` with that long-lived
object. If the user changes the video folder while the batch runs, the
broadcast used to carry the stale folder_path and the client UI reverted to
the old folder right after saving.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models  # noqa: F401
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.upload_task import UploadTask
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
import app.services.coordinator as coord_mod
import app.services.channel_service as cs_mod


@pytest.mark.asyncio
async def test_folder_change_during_batch_not_reverted_by_broadcast(
    tmp_path, monkeypatch
):
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        channel_scan_interval_seconds=1,
    )
    engine = create_async_engine(
        settings.database_url, echo=False, connect_args={"timeout": 30}
    )
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    monkeypatch.setattr(coord_mod, "settings", settings)
    monkeypatch.setattr(coord_mod, "async_session_maker", Session)
    monkeypatch.setattr(cs_mod, "settings", settings)
    import app.core.db as db_mod

    monkeypatch.setattr(db_mod, "async_session_maker", Session)

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    (folder / "v0.mp4").write_bytes(b"video-data")

    async with Session() as session:
        project = GCPProject(
            name="p",
            client_id="x",
            client_secret_enc="x",
            redirect_uri="http://localhost",
            scope="x",
            token_uri="x",
            auth_uri="x",
            api_upload="https://www.googleapis.com/upload/youtube/v3/videos",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)

        session.add(
            Channel(
                id=channel_id,
                youtube_channel_id="yc",
                title="t",
                refresh_token_enc="x",
                access_token_enc="x",
                access_token_expires_at=datetime.now(timezone.utc)
                + timedelta(hours=1),
                folder_path=f"videos/{channel_id}",
                project_id=project.id,
                max_batch_size=10,
                min_batch_size=1,
                inter_upload_delay_seconds=0,
                upload_limit_enabled=False,
            )
        )
        await session.commit()

    # Upload that blocks until the test releases it.
    upload_started = asyncio.Event()
    release_upload = asyncio.Event()

    async def fake_create_session(
        project, channel, access_token, file_path, title, description, proxy_url=None
    ):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(
        session_url,
        file_path,
        access_token,
        progress_callback=None,
        proxy_url=None,
        proxy_resolver=None,
        start_offset=0,
        **kwargs,
    ):
        upload_started.set()
        await release_upload.wait()
        return {"id": Path(file_path).name}

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )

    async def _none(cid):
        return None

    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )

    class CapturingNotifications(NotificationService):
        def __init__(self):
            super().__init__()
            self.channel_updated_payloads: list[dict] = []

        async def broadcast(self, event_type, payload):
            if event_type == "channel.updated":
                self.channel_updated_payloads.append(payload)
            return await super().broadcast(event_type, payload)

    notifications = CapturingNotifications()
    coordinator = UploadCoordinator(notifications)
    await coordinator.wake_channel(channel_id)

    # Wait until the upload is in flight.
    await asyncio.wait_for(upload_started.wait(), timeout=10)

    # While the batch is running, the user changes the video folder
    # (same effect as PUT /channels/{id} with folder_from_channel_id).
    async with Session() as session:
        channel = await session.get(Channel, channel_id)
        channel.folder_path = "videos/shared-folder"
        await session.commit()

    release_upload.set()

    # Wait for the task to complete.
    async def wait_done(timeout=15):
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            async with Session() as session:
                task = (
                    await session.exec(
                        select(UploadTask).where(UploadTask.channel_id == channel_id)
                    )
                ).scalars().first()
                if task and task.status == "completed":
                    return task
            await asyncio.sleep(0.05)
        raise AssertionError("task never completed")

    await wait_done()
    await coordinator.stop_channel_worker(channel_id)

    # The DB must keep the new folder.
    async with Session() as session:
        channel = await session.get(Channel, channel_id)
        assert channel.folder_path == "videos/shared-folder"

    # The broadcast must not carry the old folder back to the client.
    assert notifications.channel_updated_payloads, "no channel.updated broadcast"
    for payload in notifications.channel_updated_payloads:
        assert payload["channel"]["folder_path"] == "videos/shared-folder", (
            "channel.updated broadcast reverted the video folder to "
            f"{payload['channel']['folder_path']!r}"
        )

    await engine.dispose()


@pytest.mark.asyncio
async def test_folder_change_on_paused_channel_persists(tmp_path, monkeypatch):
    """PUT /channels/{id} with folder_from_channel_id works on a paused channel."""
    from httpx import ASGITransport, AsyncClient

    from app.api.deps import get_session
    from app.core.config import settings as global_settings
    from app.main import app

    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
    )
    engine = create_async_engine(
        settings.database_url, echo=False, connect_args={"timeout": 30}
    )
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    async with Session() as session:
        project = GCPProject(
            name="p",
            client_id="x",
            client_secret_enc="x",
            redirect_uri="http://localhost",
            scope="x",
            token_uri="x",
            auth_uri="x",
            api_upload="https://www.googleapis.com/upload/youtube/v3/videos",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)

        target_id, source_id = uuid4(), uuid4()
        for cid, folder in (
            (target_id, f"videos/{target_id}"),
            (source_id, "videos/source-folder"),
        ):
            session.add(
                Channel(
                    id=cid,
                    youtube_channel_id=f"yt-{cid.hex[:8]}",
                    title="t",
                    refresh_token_enc="x",
                    access_token_enc="x",
                    access_token_expires_at=datetime.now(timezone.utc)
                    + timedelta(hours=1),
                    folder_path=folder,
                    project_id=project.id,
                    status="paused" if cid == target_id else "active",
                    pause_reason="manual",
                    upload_limit_enabled=False,
                )
            )
        await session.commit()

    async def override_get_session():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            headers = {"Authorization": f"Bearer {global_settings.api_token}"}
            resp = await client.put(
                f"/channels/{target_id}",
                headers=headers,
                json={"folder_from_channel_id": str(source_id)},
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["folder_path"] == "videos/source-folder"

            resp = await client.get(f"/channels/{target_id}", headers=headers)
            assert resp.status_code == 200, resp.text
            assert resp.json()["folder_path"] == "videos/source-folder"
    finally:
        app.dependency_overrides.clear()

    async with Session() as session:
        channel = await session.get(Channel, target_id)
        assert channel.folder_path == "videos/source-folder"

    await engine.dispose()
    await engine.dispose()
