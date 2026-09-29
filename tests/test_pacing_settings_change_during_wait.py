"""Saving pacing/limit settings during a running inter-upload wait.

The wait inside ``_process_batch`` used to be a plain ``asyncio.sleep`` to
the deadline computed from the *old* settings: a delay lowered mid-wait (for
example from 1 hour to 10 seconds when 10 seconds had already passed) was
ignored until the old window expired — the batch kept sleeping out the
remaining ~59 minutes. The wake path (``wake_pacing``) only existed for the
scan loop, which merely ensures a worker exists and never interrupts a
running wait.

The wait is now interruptible. Saving the channel through any settings
endpoint (``PUT /channels/{channel_id}``,
``PUT /channels/{channel_id}/batch-settings``,
``PUT /channels/{channel_id}/upload-limit``,
``PUT /channels/bulk-settings``) wakes it; the wait loop re-reads the fresh
values and recomputes:

* delay lowered below the already-elapsed time -> the batch starts
  immediately;
* delay raised -> the wait extends;
* upload limit enabled/lowered so it is exhausted -> the channel pauses
  instead of uploading;
* limit lowered below the batch size -> the batch is trimmed.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.upload_task import UploadTask
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
import app.services.coordinator as coord_mod
import app.services.channel_service as cs_mod
import app.core.db as db_mod


async def _make_world(tmp_path, monkeypatch, *, n_files=2, delay=8, batch=1):
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

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    for i in range(n_files):
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
                max_batch_size=batch,
                min_batch_size=1,
                inter_upload_delay_seconds=int(delay),
                upload_limit_enabled=False,
            )
        )
        await session.commit()

    return settings, Session, channel_id, engine


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
        spans.append(("upload_start", loop.time(), Path(file_path).name))
        await asyncio.sleep(0.1)
        spans.append(("completed", loop.time(), Path(file_path).name))
        return {"id": Path(file_path).name}

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


async def _put_channel(monkeypatch, Session, channel_id, payload):
    from app.main import app

    async def override_get_session():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        from app.api.deps import settings as global_settings

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {global_settings.api_token}"},
        ) as client:
            return await client.put(f"/channels/{channel_id}", json=payload)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_delay_lowered_below_elapsed_starts_batch_immediately(
    tmp_path, monkeypatch
):
    """The user's exact scenario, scaled: delay 8s, lowered to 1s at ~2s in.

    The old code kept sleeping out the remaining ~6s (in production: the
    remaining ~59 minutes of a 1-hour window). Now the save wakes the wait,
    re-reads the delay, sees the window already elapsed and starts the batch.
    """
    spans: list = []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=2, delay=8
    )
    _patch_transport(monkeypatch, spans)
    coordinator = await _fresh_coordinator(monkeypatch)
    loop = asyncio.get_running_loop()

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=1)
        first_completed = next(t for k, t, _ in spans if k == "completed")

        # ~2s into the 8s wait: lower the delay below the elapsed time.
        await asyncio.sleep(2.0)
        saved_at = loop.time()
        resp = await _put_channel(
            monkeypatch, Session, channel_id,
            {"inter_upload_delay_seconds": 1},
        )
        assert resp.status_code == 200, resp.text

        await _wait_uploads(Session, channel_id, expected=2, timeout=15)
        second_start = next(
            t for k, t, _ in spans[1:] if k == "upload_start"
        ) if len(spans) > 1 else None
        # find the second upload's start robustly
        starts = [t for k, t, _ in spans if k == "upload_start"]
        assert len(starts) == 2
        second_start = starts[1]

        # Prompt: within ~2s of the save, far below the old ~6s remainder.
        assert second_start - saved_at < 4.0, (
            f"batch started {second_start - saved_at:.2f}s after the save; "
            "the lowered delay was not applied to the running wait"
        )
        # Still never before the (new) 1s window since the last completion.
        assert second_start - first_completed >= 1.0 - 0.3
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_delay_raised_during_wait_extends_wait(tmp_path, monkeypatch):
    """Raising the delay mid-wait recomputes the remaining time upward."""
    spans: list = []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=2, delay=3
    )
    _patch_transport(monkeypatch, spans)
    coordinator = await _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=1)
        first_completed = next(t for k, t, _ in spans if k == "completed")

        # ~1.5s into the 3s wait: raise the delay to 7s. The wait must now
        # run until ~7s after the first completion, not the old deadline.
        await asyncio.sleep(1.5)
        resp = await _put_channel(
            monkeypatch, Session, channel_id,
            {"inter_upload_delay_seconds": 7},
        )
        assert resp.status_code == 200, resp.text

        await _wait_uploads(Session, channel_id, expected=2, timeout=20)
        starts = [t for k, t, _ in spans if k == "upload_start"]
        assert len(starts) == 2
        gap = starts[1] - first_completed
        assert gap >= 7.0 - 1.5, (
            f"raised delay ignored: gap {gap:.2f}s < 7s"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_limit_exhausted_during_wait_pauses_instead_of_uploading(
    tmp_path, monkeypatch
):
    """Enabling an exhausted limit mid-wait must hold the batch."""
    spans: list = []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=2, delay=8
    )
    _patch_transport(monkeypatch, spans)
    coordinator = await _fresh_coordinator(monkeypatch)
    loop = asyncio.get_running_loop()

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=1)

        # ~2s into the 8s wait: enable a 1-per-24h limit. The one completed
        # upload already exhausts it.
        await asyncio.sleep(2.0)
        resp = await _put_channel(
            monkeypatch, Session, channel_id,
            {
                "upload_limit_enabled": True,
                "upload_limit_count": 1,
                "upload_limit_period_hours": 24,
            },
        )
        assert resp.status_code == 200, resp.text

        # No second upload for 5s (the old window would have started it at
        # ~8s; here we just assert the limit holds the batch).
        deadline = loop.time() + 5
        while loop.time() < deadline:
            assert not any(
                k == "upload_start" for k, _, _ in spans[1:]
            ), "batch uploaded although the limit was exhausted mid-wait"
            await asyncio.sleep(0.1)

        async with Session() as session:
            channel = await session.get(Channel, channel_id)
            assert channel.status == "paused"
            assert channel.pause_reason == "upload_limit"
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_batch_trimmed_when_limit_lowered_during_wait(
    tmp_path, monkeypatch
):
    """A limit lowered mid-wait trims the waiting batch to the new remaining."""
    spans: list = []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=3, delay=4, batch=3
    )
    _patch_transport(monkeypatch, spans)
    coordinator = await _fresh_coordinator(monkeypatch)

    try:
        from app.services.channel_service import ChannelService

        # Three pre-collected tasks and a recent completion, so the batch
        # paces; it runs as a task so the HTTP save can land mid-wait.
        async with Session() as session:
            channel = await session.get(Channel, channel_id)
            channel.last_upload_at = datetime.now(timezone.utc)
            await session.commit()

            tasks = [
                UploadTask(
                    channel_id=channel_id,
                    file_path=f"videos/{channel_id}/v{i}.mp4",
                    status="pending",
                    attempt=0,
                )
                for i in range(3)
            ]
            for t in tasks:
                session.add(t)
            await session.commit()
            for t in tasks:
                await session.refresh(t)

            batch_task = asyncio.create_task(
                coordinator._process_batch(
                    session, ChannelService(session), channel, tasks, "token"
                )
            )

            await asyncio.sleep(1.0)
            resp = await _put_channel(
                monkeypatch, Session, channel_id,
                {
                    "upload_limit_enabled": True,
                    "upload_limit_count": 1,
                    "upload_limit_period_hours": 24,
                },
            )
            assert resp.status_code == 200, resp.text

            # The single trimmed upload exhausts the new 1/24h limit, so the
            # batch legitimately ends paused.
            try:
                await asyncio.wait_for(batch_task, timeout=15)
            except coord_mod.ChannelPaused:
                pass

        await asyncio.sleep(0.3)
        starts = [k for k, _, _ in spans if k == "upload_start"]
        # 0 completed at save time -> remaining 1 -> the batch is trimmed
        # to a single upload; the other two stay pending.
        assert len(starts) == 1, (
            f"batch of 3 uploaded {len(starts)} task(s) after the limit was "
            "lowered to 1 remaining mid-wait"
        )

        async with Session() as session:
            rows = (
                await session.exec(
                    select(UploadTask).where(
                        UploadTask.channel_id == channel_id
                    )
                )
            ).scalars().all()
            by_status = {}
            for row in rows:
                by_status[row.status] = by_status.get(row.status, 0) + 1
            assert by_status.get("completed", 0) == 1
            assert by_status.get("pending", 0) == 2
    finally:
        await coordinator.stop()
        await engine.dispose()
