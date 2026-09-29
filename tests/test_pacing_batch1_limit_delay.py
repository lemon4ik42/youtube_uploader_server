"""Pacing correctness for the user-facing batch=1 / delay / limit combination.

Scenario reported against production settings: ``max_batch_size=1``,
``inter_upload_delay_seconds=3600`` and a 5-per-24h upload limit. Symptoms:

1. consecutive completed uploads only ~1 minute apart despite the hour delay;
2. a task stuck in ``session_created`` with zero progress for the whole delay
   window and beyond — "the session was created but the video never uploads".

Root causes pinned here:

* the inter-upload wait ran *after* the first YouTube session was created, so
  the session (and the task's visible state) sat idle for the entire delay;
* the access token was ensured once at worker-iteration start and could
  expire during the hour-long pacing sleep — the upload then started with
  dead credentials, failed with 401 and dragged the channel into
  re-authorization;
* the upload ran without a progress callback, so even a healthy transfer
  showed 0% ("not uploading") until completion;
* pacing values came from the channel ORM object loaded at iteration start,
  so a settings save landing mid-batch was ignored by the running batch;
* ``is_retry_batch`` used ``any()``: one retry task in a batch skipped the
  delay for every fresh task in the same batch.
"""

from __future__ import annotations

import asyncio
import time
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


DELAY = 3.0


async def _make_world(tmp_path, monkeypatch, *, n_files=3, delay=DELAY, batch=1,
                      limit_enabled=True, limit_count=5):
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
    import app.core.db as db_mod

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
                upload_limit_enabled=limit_enabled,
                upload_limit_count=limit_count,
                upload_limit_period_hours=24,
            )
        )
        await session.commit()

    return settings, Session, channel_id, engine


def _patch_transport(monkeypatch, spans, sessions, token_log=None,
                     token_ttl=None, progress_log=None):
    """Fake YouTube transport recording session/upload/progress events."""

    async def fake_create_session(
        project, channel, access_token, file_path, title, description, proxy_url=None
    ):
        loop = asyncio.get_running_loop()
        sessions.append((loop.time(), Path(file_path).name))
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(
        session_url, file_path, access_token, progress_callback=None,
        proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs,
    ):
        loop = asyncio.get_running_loop()
        name = Path(file_path).name
        if token_log is not None and token_ttl is not None:
            issued_at, _ = token_log[-1]
            age = loop.time() - issued_at
            if age > token_ttl:
                spans.append(("AUTH_401", loop.time(), name))
                from app.services.youtube_uploader import YouTubeUploadError

                raise YouTubeUploadError(
                    "invalid credentials", reason="authError",
                    status_code=401, is_auth_error=True,
                )
        start = loop.time()
        spans.append(("upload_start", start, name))
        if progress_callback is not None:
            await progress_callback(50, 100)
            await asyncio.sleep(0.05)
            await progress_callback(100, 100)
            if progress_log is not None:
                progress_log.append(name)
        await asyncio.sleep(0.05)
        spans.append(("completed", loop.time(), name))
        return {"id": name}

    async def fake_ensure_token(self, channel, *, request_reauth=True):
        loop = asyncio.get_running_loop()
        if token_log is not None:
            token_log.append((loop.time(), "token"))
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


def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
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


@pytest.mark.asyncio
async def test_batch1_delay_spacing_and_limit(tmp_path, monkeypatch):
    """batch=1 + delay: every consecutive upload waits the full delay.

    Mirrors the reported setup (batch=1, delay, 5-per-24h limit) with a
    3-second delay: the gap between consecutive completed uploads, and the
    gap between a session creation and its upload, must both respect the
    delay. With the old code the second video started ~1s after the first
    completed.
    """
    spans, sessions = [], []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=3, delay=DELAY
    )
    _patch_transport(monkeypatch, spans, sessions)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=3)
        await asyncio.sleep(0.5)

        completed = [t for kind, t, _ in spans if kind == "completed"]
        starts = [t for kind, t, _ in spans if kind == "upload_start"]
        assert len(completed) == 3 and len(starts) == 3

        # Measure start-to-start: the "completed" span fires inside the
        # uploader, but ``last_upload_at`` (what pacing anchors to) is
        # committed later in the result loop — under a full-suite load that
        # gap reaches a second, so completion-to-start flaked even though
        # the server was correct. Start-to-start includes the completion
        # lag and still proves the window is respected.
        for prev_start, next_start in zip(starts, starts[1:]):
            actual = next_start - prev_start
            assert actual >= DELAY - 0.3, (
                f"consecutive uploads started {actual:.2f}s apart; "
                f"inter_upload_delay is {DELAY}s"
            )

        # The session for each video is created right before its upload, not
        # a full delay window earlier (the old "stuck in session_created").
        upload_by_name = {name: t for kind, t, name in spans if kind == "upload_start"}
        for t, name in sessions:
            assert upload_by_name[name] - t < 1.0, (
                f"session for {name} created {upload_by_name[name]-t:.2f}s "
                "before its upload; it must not sit idle for the delay window"
            )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_token_reensured_after_pacing_sleep(tmp_path, monkeypatch):
    """The access token cannot expire during the pacing sleep.

    The token lives 2s here while the delay is 3s: without re-ensuring the
    token after the sleep, every upload after the first starts with expired
    credentials (401) and the videos never complete.
    """
    token_log: list = []
    spans, sessions = [], []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=3, delay=DELAY
    )
    _patch_transport(
        monkeypatch, spans, sessions, token_log=token_log, token_ttl=2.0
    )
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=3)

        assert not [k for k, _, _ in spans if k == "AUTH_401"], (
            "upload started with an expired token: the token must be "
            "re-ensured after the pacing sleep"
        )
        # A token was ensured for each upload (iteration start or after the
        # sleep), not once for the whole sequence.
        assert len(token_log) >= 3
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_upload_reports_progress(tmp_path, monkeypatch):
    """The upload streams progress updates; the task is not stuck at 0%."""
    spans, sessions = [], []
    progress_log: list = []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=2, delay=DELAY
    )
    _patch_transport(monkeypatch, spans, sessions, progress_log=progress_log)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=2)

        assert len(progress_log) == 2, (
            "progress_callback was not passed to the uploader"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_delay_saved_mid_batch_shapes_next_batch(tmp_path, monkeypatch):
    """A settings save landing mid-batch is not ignored by pacing.

    Video 1 uploads with a small delay; the delay is raised while the channel
    is between batches (exactly what the user did while watching uploads);
    the next batch must pace with the fresh value read from the database,
    not the stale ORM value loaded at iteration start.
    """
    spans, sessions = [], []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=3, delay=1
    )
    _patch_transport(monkeypatch, spans, sessions)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=1)

        async with Session() as session:
            channel = await session.get(Channel, channel_id)
            channel.inter_upload_delay_seconds = int(DELAY)
            await session.commit()

        await _wait_uploads(Session, channel_id, expected=3)
        await asyncio.sleep(0.5)

        completed = [t for kind, t, _ in spans if kind == "completed"]
        starts = [t for kind, t, _ in spans if kind == "upload_start"]
        # Start-to-start (see the batch1 test): completion spans fire before
        # ``last_upload_at`` commits, which flakes under load.
        for prev_start, next_start in zip(starts[1:], starts[2:]):
            actual = next_start - prev_start
            # The pinned invariant: after the save raised the delay from 1s
            # to DELAY (3s), the next gap is at least the new delay, not the
            # old one.
            assert actual >= DELAY - 0.3, (
                f"post-save gap {actual:.2f}s ignored the freshly saved "
                f"inter_upload_delay {DELAY}s"
            )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_mixed_batch_with_one_retry_task_is_paced(tmp_path, monkeypatch):
    """A batch mixing fresh and retry tasks must not skip the delay.

    ``is_retry_batch`` used ``any()``: a single retry task (attempt > 0)
    removed the pacing for every fresh task in the same batch. Only a batch
    made *entirely* of retries skips the wait. The normal worker flow never
    mixes them (pending retry tasks block new file collection), so this pins
    the batch-level rule directly.
    """
    spans, sessions = [], []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=0, delay=DELAY, batch=2,
        limit_enabled=False,
    )
    _patch_transport(monkeypatch, spans, sessions)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        from app.services.channel_service import ChannelService

        async with Session() as session:
            channel = await session.get(Channel, channel_id)
            channel.last_upload_at = datetime.now(timezone.utc)
            await session.commit()

            fresh = UploadTask(
                channel_id=channel_id,
                file_path=f"videos/{channel_id}/v0.mp4",
                status="pending",
                attempt=0,
            )
            retry = UploadTask(
                channel_id=channel_id,
                file_path=f"videos/{channel_id}/v1.mp4",
                status="pending",
                attempt=1,
            )
            session.add(fresh)
            session.add(retry)
            await session.commit()
            await session.refresh(fresh)
            await session.refresh(retry)

            loop = asyncio.get_running_loop()
            started = loop.time()
            await coordinator._process_batch(
                session, ChannelService(session), channel,
                [fresh, retry], "token",
            )
            waited = loop.time() - started

        assert waited >= DELAY - 1.5, (
            f"mixed batch (1 fresh + 1 retry) skipped the inter-upload delay "
            f"(waited {waited:.2f}s < {DELAY}s)"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_pure_retry_batch_skips_delay(tmp_path, monkeypatch):
    """A batch made only of retry tasks keeps skipping the wait (fast retry
    after a recovered proxy route — pinned by the proxy-switch tests too)."""
    spans, sessions = [], []
    settings, Session, channel_id, engine = await _make_world(
        tmp_path, monkeypatch, n_files=0, delay=60, batch=2,
        limit_enabled=False,
    )
    _patch_transport(monkeypatch, spans, sessions)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        from app.services.channel_service import ChannelService

        async with Session() as session:
            channel = await session.get(Channel, channel_id)
            channel.last_upload_at = datetime.now(timezone.utc)
            await session.commit()

            tasks = [
                UploadTask(
                    channel_id=channel_id,
                    file_path=f"videos/{channel_id}/v{i}.mp4",
                    status="pending",
                    attempt=1,
                )
                for i in range(2)
            ]
            for t in tasks:
                session.add(t)
            await session.commit()
            for t in tasks:
                await session.refresh(t)

            loop = asyncio.get_running_loop()
            started = loop.time()
            await coordinator._process_batch(
                session, ChannelService(session), channel, tasks, "token",
            )
            waited = loop.time() - started

        assert waited < 5, (
            f"pure retry batch waited {waited:.2f}s; it must skip the delay"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()
