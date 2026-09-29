"""Pause/resume behaviour for the long-running channel worker.

The worker should sit idle while the channel is paused and pick the files up
as soon as the channel becomes active again — without anyone re-spawning it.
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
from app.models.uploaded_hash import UploadedVideoHash
from app.services.notification_service import NotificationService


async def _count_uploads(session_maker, channel_id) -> int:
    async with session_maker() as session:
        hashes = (
            await session.exec(
                select(UploadedVideoHash).where(
                    UploadedVideoHash.channel_id == channel_id
                )
            )
        ).scalars().all()
        return len(hashes)


@pytest.mark.asyncio
async def test_worker_waits_while_paused_and_resumes_after_unpause(
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
    engine = create_async_engine(settings.database_url, echo=False)
    TestSession = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    import app.services.coordinator as coord_mod
    import app.services.channel_service as cs_mod

    monkeypatch.setattr(coord_mod, "settings", settings)
    monkeypatch.setattr(coord_mod, "async_session_maker", TestSession)
    monkeypatch.setattr(cs_mod, "settings", settings)

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    for i in range(5):
        (folder / f"v{i}.mp4").write_bytes(f"data-{i}".encode())

    async with TestSession() as session:
        project = GCPProject(
            name="p", client_id="x", client_secret_enc="x",
            redirect_uri="http://localhost", scope="x", token_uri="x", auth_uri="x",
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
                access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                folder_path=f"videos/{channel_id}",
                project_id=project.id,
                max_batch_size=10,
                min_batch_size=1,
                inter_upload_delay_seconds=0,
                upload_limit_enabled=False,
                status="paused",
                pause_reason="manual",
            )
        )
        await session.commit()

    async def fake_create_session(project, channel, access_token, file_path, title, description, proxy_url=None):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(session_url, file_path, access_token, progress_callback=None, proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs):
        return {"id": Path(file_path).name}

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(cs_mod.ChannelService, "ensure_access_token", fake_ensure_token)

    async def fake_proxy_url(channel_id):
        return None

    monkeypatch.setattr(coord_mod, "get_current_channel_proxy_url", fake_proxy_url)

    coordinator = coord_mod.UploadCoordinator(NotificationService())

    try:
        await coordinator.wake_channel(channel_id)
        # Give the worker a couple of iterations to (not) do anything.
        await asyncio.sleep(2.5)
        assert await _count_uploads(TestSession, channel_id) == 0, (
            "no uploads should happen while the channel is manually paused"
        )

        # Now unpause and let the worker notice on its next iteration.
        async with TestSession() as session:
            ch = (
                await session.exec(select(Channel).where(Channel.id == channel_id))
            ).scalars().first()
            ch.status = "active"
            ch.pause_reason = None
            await session.commit()

        # Wait for uploads to start flowing.
        deadline = asyncio.get_event_loop().time() + 15
        while True:
            n = await _count_uploads(TestSession, channel_id)
            if n >= 5:
                break
            assert asyncio.get_event_loop().time() < deadline, (
                f"worker did not resume after unpause; completed: {n}"
            )
            await asyncio.sleep(0.2)
    finally:
        await coordinator.stop()
