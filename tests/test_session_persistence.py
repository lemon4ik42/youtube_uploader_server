"""Upload sessions are persisted, reused and cleaned up.

A resumable YouTube session URL is written to the task row as soon as it is
created, so a mid-batch interruption (dead proxy, restart, wait for a manual
route fix) does not waste the already-committed bytes:

* on the next pass the saved session is queried for its committed offset and
  the upload resumes from there;
* a dead saved session (404/410 — sessions expire after ~24h) is replaced with a
  new one instead of retried forever;
* the URL is cleared once the task reaches a terminal state (completed or
  failed), so rows do not accumulate dead URLs.
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
import app.services.youtube_uploader as uploader_mod


async def _make_world(tmp_path, monkeypatch):
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        channel_scan_interval_seconds=1,
        inter_upload_delay_seconds=0,
        proxy_retry_attempts=5,
        proxy_switch_wait_timeout_seconds=2,
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

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    (folder / "v0.mp4").write_bytes(b"video-bytes" * 10)

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
                access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                folder_path=f"videos/{channel_id}",
                project_id=project.id,
                max_batch_size=10,
                min_batch_size=1,
                inter_upload_delay_seconds=0,
                upload_limit_enabled=False,
            )
        )
        await session.commit()

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    async def fake_proxy_url(channel_id):
        return None

    monkeypatch.setattr(coord_mod, "get_current_channel_proxy_url", fake_proxy_url)

    return Session, channel_id


async def _get_task(Session, channel_id) -> UploadTask | None:
    async with Session() as session:
        result = await session.exec(
            select(UploadTask).where(UploadTask.channel_id == channel_id)
        )
        return result.scalars().first()


async def _wait_for(Session, channel_id, predicate, timeout=15):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        task = await _get_task(Session, channel_id)
        if task is not None and predicate(task):
            return task
        await asyncio.sleep(0.05)
    task = await _get_task(Session, channel_id)
    raise AssertionError(
        f"condition never met; last state: status={task.status if task else None} "
        f"session_url={bool(task.session_url) if task else None}"
    )


@pytest.mark.asyncio
async def test_session_url_is_persisted_after_creation(tmp_path, monkeypatch):
    """The resumable URL lands in the DB row, not just in memory."""
    Session, channel_id = await _make_world(tmp_path, monkeypatch)

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return "https://upload.example.com/session-1"

    # Never completes: the goal is to observe the persisted session mid-flight.
    hang = asyncio.Event()

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None,
                          proxy_resolver=None, start_offset=0):
        await hang.wait()
        return {"id": "vid"}

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)

    coordinator = UploadCoordinator(NotificationService())
    await coordinator.wake_channel(channel_id)

    task = await _wait_for(
        Session, channel_id, lambda t: t.status == "session_created"
    )
    assert task.session_url == "https://upload.example.com/session-1", (
        "session URL must be persisted on the task row right after creation"
    )
    hang.set()
    await _wait_for(Session, channel_id, lambda t: t.status == "completed")
    await coordinator.stop_channel_worker(channel_id)

    final = await _get_task(Session, channel_id)
    assert final.status == "completed"
    assert final.session_url is None, "completed tasks must clear the session URL"


@pytest.mark.asyncio
async def test_saved_session_is_reused_and_resumed_from_offset(
    tmp_path, monkeypatch
):
    """A second pass resumes the saved session instead of creating a new one."""
    Session, channel_id = await _make_world(tmp_path, monkeypatch)

    create_calls: list[str] = []
    resume_offsets: list[int] = []
    # Block the first pass's data transfer so stopping the worker leaves the
    # task in session_created with a saved, never-yet-uploaded session.
    block_uploads = asyncio.Event()

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        create_calls.append(Path(file_path).name)
        return "https://upload.example.com/session-1"

    async def fake_query_progress(session_url, access_token, *, proxy_url=None,
                                  proxy_resolver=None):
        # YouTube reports 3 chunks already committed.
        return 30

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None,
                          proxy_resolver=None, start_offset=0):
        await block_uploads.wait()
        resume_offsets.append(start_offset)
        return {"id": "vid"}

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "query_upload_progress", fake_query_progress)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)

    coordinator = UploadCoordinator(NotificationService())

    # First pass: the session is created but the transfer is blocked, so
    # stopping the worker leaves the task in session_created with a saved URL.
    await coordinator.wake_channel(channel_id)
    task = await _wait_for(
        Session, channel_id, lambda t: t.status == "session_created"
    )
    assert task.session_url == "https://upload.example.com/session-1"
    # Simulate the worker dying with the session persisted.
    await coordinator.stop_channel_worker(channel_id, preserve_tasks=True)

    saved = await _get_task(Session, channel_id)
    assert saved.session_url == "https://upload.example.com/session-1"
    assert saved.status in ("session_created", "pending")

    # Second pass: same session, resumed from the committed offset.
    await coordinator.wake_channel(channel_id)
    await asyncio.sleep(0.5)
    block_uploads.set()
    await _wait_for(Session, channel_id, lambda t: t.status == "completed")
    await coordinator.stop_channel_worker(channel_id)

    assert create_calls == ["v0.mp4"], (
        "the saved session must be reused, not replaced by a duplicate"
    )
    assert resume_offsets == [30], (
        "the upload must resume from the committed offset, not from zero"
    )


@pytest.mark.asyncio
async def test_dead_saved_session_is_replaced(tmp_path, monkeypatch):
    """An expired saved session (404/410) falls back to a fresh one."""
    Session, channel_id = await _make_world(tmp_path, monkeypatch)

    create_calls: list[str] = []
    block_uploads = asyncio.Event()
    # Second pass: YouTube answers the status query with 404 (expired).
    query_mode = {"expired": False}

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        create_calls.append(Path(file_path).name)
        return f"https://upload.example.com/session-{len(create_calls)}"

    async def fake_query_progress(session_url, access_token, *, proxy_url=None,
                                  proxy_resolver=None):
        if not query_mode["expired"]:
            return 0
        raise uploader_mod.YouTubeUploadError(
            "Session expired or not found.",
            reason="notFound",
            status_code=404,
            is_session_expired=True,
        )

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None,
                          proxy_resolver=None, start_offset=0):
        await block_uploads.wait()
        return {"id": "vid"}

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "query_upload_progress", fake_query_progress)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)

    coordinator = UploadCoordinator(NotificationService())

    # First pass: session created, transfer blocked, worker stopped with the
    # task preserved in session_created.
    await coordinator.wake_channel(channel_id)
    task = await _wait_for(
        Session, channel_id, lambda t: t.status == "session_created"
    )
    dead_url = task.session_url
    await coordinator.stop_channel_worker(channel_id, preserve_tasks=True)

    # The saved session has since expired on YouTube's side.
    query_mode["expired"] = True
    block_uploads.set()

    # Second pass: the dead URL is detected and replaced by a fresh session.
    await coordinator.wake_channel(channel_id)
    await _wait_for(Session, channel_id, lambda t: t.status == "completed")
    await coordinator.stop_channel_worker(channel_id)

    final = await _get_task(Session, channel_id)
    assert final.status == "completed"
    assert final.session_url is None
    assert create_calls == ["v0.mp4", "v0.mp4"], (
        "a dead saved session must trigger exactly one replacement"
    )
    assert dead_url is not None


@pytest.mark.asyncio
async def test_failed_task_clears_session_url(tmp_path, monkeypatch):
    """Terminal failure must not leave the dead session URL on the row."""
    Session, channel_id = await _make_world(tmp_path, monkeypatch)

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return "https://upload.example.com/session-1"

    async def fake_query_progress(session_url, access_token, *, proxy_url=None,
                                  proxy_resolver=None):
        return 0

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None,
                          proxy_resolver=None, start_offset=0):
        raise uploader_mod.YouTubeUploadError(
            "Invalid file", reason="unsupportedFormat", is_invalid_file=True
        )

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "query_upload_progress", fake_query_progress)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)

    coordinator = UploadCoordinator(NotificationService())
    await coordinator.wake_channel(channel_id)
    await _wait_for(Session, channel_id, lambda t: t.status == "failed")
    await coordinator.stop_channel_worker(channel_id)

    final = await _get_task(Session, channel_id)
    assert final.status == "failed"
    assert final.session_url is None, (
        "a terminally failed task must not keep its session URL"
    )
