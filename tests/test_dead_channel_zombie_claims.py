"""Zombie tasks of dead channels must not block a shared folder.

User-reported symptom: after a server restart the limit widget shows
"N remaining" (uploads allowed), yet nothing uploads. Root cause: a channel
in ``error``/``deleted`` state (or with a non-active session) never runs its
worker, but its ``pending``/``session_created``/``uploading`` tasks keep
CLAIMING files in the shared folder. The active sharer's collection skips
those files forever — the queue is stuck across restarts.

The scan loop now fails a dead channel's active tasks, which releases the
files to the active sharers; if the channel is later repaired, the files are
re-collected (hashes were never recorded, so nothing double-uploads).
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
import app.core.db as db_mod


def _patch_transport(monkeypatch, spans):
    async def fake_create_session(
        project, channel, access_token, file_path, title, description,
        proxy_url=None,
    ):
        return f"https://u/{channel.title}/{Path(file_path).name}"

    async def fake_upload(
        session_url, file_path, access_token, progress_callback=None,
        proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs,
    ):
        spans.append(("upload_start", asyncio.get_running_loop().time(),
                      f"{channel_title(session_url)}:{Path(file_path).name}"))
        await asyncio.sleep(0.1)
        return {"id": Path(file_path).name}

    def channel_title(session_url):
        return session_url.split("/")[-2]

    async def fake_ensure_token(self, channel, *, request_reauth=True):
        return "token"

    async def fake_refresh_snippet(self, channel, access_token):
        return False

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    monkeypatch.setattr(
        cs_mod.ChannelService, "refresh_channel_snippet", fake_refresh_snippet
    )

    async def _none(cid):
        return None

    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )


async def _make_world(tmp_path, monkeypatch):
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        channel_scan_interval_seconds=1,
    )
    engine = create_async_engine(settings.database_url, connect_args={"timeout": 30})
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    monkeypatch.setattr(coord_mod, "settings", settings)
    monkeypatch.setattr(coord_mod, "async_session_maker", Session)
    monkeypatch.setattr(cs_mod, "settings", settings)
    monkeypatch.setattr(db_mod, "async_session_maker", Session)

    shared = tmp_path / "videos" / "shared"
    shared.mkdir(parents=True)
    # Two files: one claimed by the dead channel's zombie, one free.
    for i in range(2):
        (shared / f"v{i}.mp4").write_bytes(f"video-data-{i}".encode())

    async with Session() as session:
        project = GCPProject(
            name="p", client_id="x", client_secret_enc="x",
            redirect_uri="http://localhost", scope="x", token_uri="x", auth_uri="x",
            api_upload="https://www.googleapis.com/upload/youtube/v3/videos",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)

        live_id, dead_id = uuid4(), uuid4()
        # The live channel: limit 5/24h, delay 1s — "2 remaining" scenario.
        session.add(Channel(
            id=live_id, youtube_channel_id="yt-live", title="live",
            project_id=project.id,
            refresh_token_enc="x", access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path="videos/shared",
            max_batch_size=1, min_batch_size=1,
            inter_upload_delay_seconds=1,
            upload_limit_enabled=True,
            upload_limit_count=5,
            upload_limit_period_hours=24,
        ))
        # The dead channel shares the folder and holds a session_created
        # zombie task for v0.mp4 (e.g. left by a crash before it died).
        session.add(Channel(
            id=dead_id, youtube_channel_id="yt-dead", title="dead",
            project_id=project.id,
            refresh_token_enc="x", access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path="videos/shared",
            status="error",
            error_message="Authorization error during upload",
            max_batch_size=1, min_batch_size=1,
            inter_upload_delay_seconds=1,
        ))
        session.add(UploadTask(
            channel_id=dead_id,
            file_path="videos/shared/v0.mp4",
            status="session_created",
            file_hash="hash-v0",
            session_url="https://u/dead/v0.mp4",
        ))
        await session.commit()
    return settings, Session, (live_id, dead_id), engine


async def _wait_uploads(Session, expected, timeout=30):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    rows = []
    while loop.time() < deadline:
        async with Session() as session:
            rows = (
                await session.exec(
                    select(UploadTask).where(UploadTask.status == "completed")
                )
            ).scalars().all()
            if len(rows) >= expected:
                return rows
        await asyncio.sleep(0.1)
    return rows


@pytest.mark.asyncio
async def test_dead_channel_zombie_releases_shared_folder(tmp_path, monkeypatch):
    """The file claimed by a dead sharer's zombie uploads via the live one."""
    spans: list = []
    settings, Session, (live_id, dead_id), engine = await _make_world(
        tmp_path, monkeypatch
    )
    _patch_transport(monkeypatch, spans)
    coordinator = UploadCoordinator(NotificationService())
    monkeypatch.setattr(coord_mod, "coordinator", coordinator)

    try:
        # Simulate "after restart": start the scan loop only.
        coordinator._scan_task = asyncio.create_task(coordinator._scan_loop())
        try:
            rows = await _wait_uploads(Session, expected=2, timeout=20)
            assert len(rows) == 2, (
                f"only {len(rows)}/2 files uploaded; the dead sharer's "
                "session_created zombie still blocks the shared folder"
            )

            # The zombie itself must be failed, not left pending forever.
            async with Session() as session:
                zombie = (
                    await session.exec(
                        select(UploadTask).where(
                            UploadTask.channel_id == dead_id
                        )
                    )
                ).scalars().first()
                assert zombie is not None
                assert zombie.status == "failed", (
                    f"dead channel's zombie task is {zombie.status}; it "
                    "must be failed so it cannot claim files forever"
                )
        finally:
            coordinator._stop_event.set()
            coordinator._scan_task.cancel()
            try:
                await coordinator._scan_task
            except asyncio.CancelledError:
                pass
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_retry_rows_purged(tmp_path, monkeypatch):
    """Legacy ``retry`` rows (old server version) are purged, files upload."""
    spans: list = []
    settings, Session, (live_id, dead_id), engine = await _make_world(
        tmp_path, monkeypatch
    )
    _patch_transport(monkeypatch, spans)
    coordinator = UploadCoordinator(NotificationService())
    monkeypatch.setattr(coord_mod, "coordinator", coordinator)

    async with Session() as session:
        session.add(UploadTask(
            channel_id=live_id,
            file_path="videos/shared/v1.mp4",
            status="retry",
            file_hash="hash-v1-legacy",
        ))
        await session.commit()

    try:
        coordinator._scan_task = asyncio.create_task(coordinator._scan_loop())
        try:
            rows = await _wait_uploads(Session, expected=2, timeout=20)
            assert len(rows) == 2
            async with Session() as session:
                legacy = (
                    await session.exec(
                        select(UploadTask).where(
                            UploadTask.channel_id == live_id,
                            UploadTask.status == "retry",
                        )
                    )
                ).scalars().all()
                assert legacy == [], "legacy retry rows must be purged"
        finally:
            coordinator._stop_event.set()
            coordinator._scan_task.cancel()
            try:
                await coordinator._scan_task
            except asyncio.CancelledError:
                pass
    finally:
        await coordinator.stop()
        await engine.dispose()
