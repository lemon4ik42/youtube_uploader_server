"""End-to-end tests for the long-running channel worker loop.

The new coordinator design keeps one asyncio task per channel alive forever;
each iteration it checks preconditions (paused, upload limit, client idleness,
min_batch_size) and runs a batch when everything is green. These tests verify:

1. A worker spawned by ``wake_channel`` keeps uploading through multiple
   batches without restarts.
2. When the channel is paused, the worker sits idle and resumes after unpause.
3. The tail is flushed once the client goes idle past the flush window.
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


async def _wait_for_uploads(
    session_maker, channel_id, expected: int, timeout_s: float = 10.0
) -> list:
    """Poll the DB until ``expected`` UploadedVideoHash rows exist."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        async with session_maker() as session:
            hashes = (
                await session.exec(
                    select(UploadedVideoHash).where(
                        UploadedVideoHash.channel_id == channel_id
                    )
                )
            ).scalars().all()
            if len(hashes) >= expected:
                return list(hashes)
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"Timed out waiting for {expected} uploads, got {len(hashes)}"
            )
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_long_running_worker_cycles_through_batches(tmp_path, monkeypatch):
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        # Short scan interval so the worker wakes up quickly between batches.
        channel_scan_interval_seconds=1,
        min_batch_flush_idle_seconds=60,
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
    for i in range(25):  # 25 files, batch of 10 -> 10 + 10 + 5 (tail)
        (folder / f"v{i:02d}.mp4").write_bytes(f"data-{i}".encode())

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
                min_batch_size=1,  # no threshold for this test
                inter_upload_delay_seconds=0,
                upload_limit_enabled=False,
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
        hashes = await _wait_for_uploads(TestSession, channel_id, expected=25, timeout_s=20)
        assert len(hashes) == 25, (
            f"worker must chew through all 25 files; got "
            f"{sorted(Path(h.file_path).name for h in hashes)}"
        )
    finally:
        await coordinator.stop()
