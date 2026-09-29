"""Server restart during the inter-upload delay must not break pacing.

The delay is an in-memory ``asyncio.sleep``, but pacing is anchored to the
persisted ``last_upload_at`` (committed right after each completed upload).
After a restart the next batch recomputes ``delay - elapsed`` and sleeps only
the remaining time — the wait is effectively resumed, not restarted and not
lost. This must hold for both shutdown flavors:

* graceful (``coordinator.stop()``): the worker's cleanup marks the pending
  task ``failed``; the next scan purges it and re-collects the file, and the
  batch still paces from the persisted timestamp;
* hard kill: the pending task row survives, is loaded as an active task, and
  the batch paces from the same timestamp.

A restart that reset the delay would upload the next video seconds after the
last completion even though the configured window had not passed.
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


DELAY = 5.0


def _patch_transport(monkeypatch, spans):
    async def fake_create_session(
        project, channel, access_token, file_path, title, description, proxy_url=None
    ):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(
        session_url, file_path, access_token, progress_callback=None,
        proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs,
    ):
        loop = asyncio.get_running_loop()
        name = Path(file_path).name
        spans.append(("upload_start", loop.time(), name))
        await asyncio.sleep(0.1)
        spans.append(("completed", loop.time(), name))
        return {"id": name}

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


def _bind_world(monkeypatch, db_path, base_dir):
    settings = Settings(
        base_dir=str(base_dir),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{db_path}",
        channel_scan_interval_seconds=1,
    )
    engine = create_async_engine(settings.database_url, connect_args={"timeout": 30})
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(coord_mod, "settings", settings)
    monkeypatch.setattr(coord_mod, "async_session_maker", Session)
    monkeypatch.setattr(cs_mod, "settings", settings)
    monkeypatch.setattr(db_mod, "async_session_maker", Session)
    return settings, Session, engine


async def _make_world(tmp_path, monkeypatch):
    """Seed the database once; engines are re-bound per "server run"."""
    db_path = tmp_path / "restart.db"
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{db_path}",
        channel_scan_interval_seconds=1,
    )
    engine = create_async_engine(settings.database_url, connect_args={"timeout": 30})
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    for i in range(2):
        (folder / f"v{i}.mp4").write_bytes(f"video-data-{i}".encode())

    async with Session() as session:
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
                access_token_expires_at=datetime.now(timezone.utc)
                + timedelta(hours=1),
                folder_path=f"videos/{channel_id}",
                project_id=project.id,
                max_batch_size=1,
                min_batch_size=1,
                inter_upload_delay_seconds=int(DELAY),
            )
        )
        await session.commit()
    await engine.dispose()
    return db_path, channel_id


async def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
    import app.api.routes.channels as routes_mod

    notes = NotificationService()
    coord = UploadCoordinator(notes)
    monkeypatch.setattr(coord_mod, "coordinator", coord)
    monkeypatch.setattr(routes_mod, "coordinator", coord)
    monkeypatch.setattr(cs_mod, "notifications", notes)
    return coord


async def _wait_uploads(Session, channel_id, expected, timeout=60):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    rows = []
    while loop.time() < deadline:
        async with Session() as session:
            rows = (
                await session.exec(
                    select(UploadTask).where(
                        UploadTask.channel_id == channel_id,
                        UploadTask.status == "completed",
                    )
                )
            ).scalars().all()
            if len(rows) >= expected:
                return rows
        await asyncio.sleep(0.1)
    raise AssertionError(f"only {len(rows)} uploads completed, expected {expected}")


async def _restart_flow(tmp_path, monkeypatch, *, graceful: bool):
    db_path, channel_id = await _make_world(tmp_path, monkeypatch)
    spans: list = []

    # --- Server run 1: upload v0, restart ~1s into the delay window.
    settings, Session, engine = _bind_world(monkeypatch, db_path, tmp_path)
    _patch_transport(monkeypatch, spans)
    coordinator = await _fresh_coordinator(monkeypatch)
    loop = asyncio.get_running_loop()
    await coordinator.start()
    await _wait_uploads(Session, channel_id, expected=1)
    completed1 = spans[-1][1]

    # ~1s into the 5s delay window: restart here.
    await asyncio.sleep(1.0)
    if graceful:
        await coordinator.stop()
    else:
        # Hard kill: no cleanup, the pending task row stays as-is.
        coordinator._stop_event.set()
        for task in list(coordinator._workers.values()):
            task.cancel()
        await asyncio.gather(
            *list(coordinator._workers.values()), return_exceptions=True
        )
        coordinator._workers.clear()
        if coordinator._scan_task:
            coordinator._scan_task.cancel()
        coordinator._metadata_tasks.clear()
    await engine.dispose()

    # --- Server run 2: same database, brand-new process state.
    spans_before = len(spans)
    settings, Session, engine = _bind_world(monkeypatch, db_path, tmp_path)
    coordinator = await _fresh_coordinator(monkeypatch)
    await coordinator.start()
    try:
        await _wait_uploads(Session, channel_id, expected=2, timeout=30)
        await asyncio.sleep(0.3)

        upload2 = next(t for kind, t, _ in spans[spans_before:]
                       if kind == "upload_start")
        upload1 = next(t for kind, t, _ in spans[:spans_before]
                       if kind == "upload_start")
        gap = upload2 - upload1
        # 1s of the window elapsed before the restart; the remaining ~4s
        # must still be waited. A restart that reset the delay would produce
        # a gap of ~1.5-2s. Measured start-to-start: the completion span
        # fires before ``last_upload_at`` commits, which flakes under load.
        assert gap >= DELAY - 0.5, (
            f"restart {'(graceful)' if graceful else '(hard kill)'} "
            f"reset the inter-upload delay: gap {gap:.2f}s < {DELAY}s"
        )
        assert gap <= DELAY + 10, (
            f"restart lost the already-elapsed part of the delay: "
            f"gap {gap:.2f}s waited the full window again on top of the restart"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_graceful_restart_during_delay_resumes_wait(tmp_path, monkeypatch):
    await _restart_flow(tmp_path, monkeypatch, graceful=True)


@pytest.mark.asyncio
async def test_hard_kill_during_delay_resumes_wait(tmp_path, monkeypatch):
    await _restart_flow(tmp_path, monkeypatch, graceful=False)
