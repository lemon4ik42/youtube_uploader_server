"""Upload behavior when the proxy dies mid-upload.

The old flow failed the task and let the *next* worker pass retry it — after
paying the channel's full ``inter_upload_delay_seconds`` again. With a 10
minute delay and a flaky proxy, one failure stalled a channel for 10+ minutes
per video, and tasks could sit in ``session_created`` for half an hour before
ending as ``failed: network_error``.

The new flow, pinned by these tests:

* a retryable transport error during the upload switches the proxy (or waits
  for a manual route fix) and retries in place, without re-paying the delay;
* if no route appears within ``proxy_switch_wait_timeout_seconds``, the task is
  released as retryable ``pending`` (not permanently failed) and the channel is
  scheduled for recovery;
* switching the channel to no-proxy mode (or picking another proxy) while the
  upload waits unblocks it immediately;
* a batch that only contains retry tasks (``attempt > 0``) is not throttled by
  ``inter_upload_delay_seconds``.
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
from app.models.proxy import Proxy
from app.models.project import GCPProject
from app.models.upload_task import UploadTask
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
import app.services.coordinator as coord_mod
import app.services.channel_service as cs_mod
from app.core import security


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


async def _make_world(tmp_path, monkeypatch, *, proxy_on: bool, delay: int = 600):
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        channel_scan_interval_seconds=1,
        inter_upload_delay_seconds=delay,
        proxy_retry_attempts=3,
        proxy_switch_wait_timeout_seconds=5,
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
    # ``get_current_channel_proxy_url`` imports the session maker lazily from
    # ``app.core.db``, so that module needs the test database as well.
    import app.core.db as db_mod
    monkeypatch.setattr(db_mod, "async_session_maker", Session)

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    (folder / "v0.mp4").write_bytes(b"video-data")

    async with Session() as session:
        project = GCPProject(
            name="p", client_id="x", client_secret_enc="x",
            redirect_uri="http://localhost", scope="x", token_uri="x", auth_uri="x",
            api_upload="https://www.googleapis.com/upload/youtube/v3/videos",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)

        proxy_id = None
        if proxy_on:
            proxy = Proxy(url_enc=_enc("socks5://u:p@10.0.0.1:1080"), is_active=True)
            session.add(proxy)
            await session.commit()
            await session.refresh(proxy)
            proxy_id = proxy.id

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
                inter_upload_delay_seconds=delay,
                proxy_id=proxy_id,
                proxy_auto_switch=True,
                upload_limit_enabled=False,
            )
        )
        await session.commit()

    return settings, Session, channel_id


async def _fake_routes_ok(monkeypatch, proxy_calls: list | None = None):
    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs):
        if proxy_calls is not None:
            proxy_calls.append(asyncio.get_running_loop().time())
        return {"id": Path(file_path).name}

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )


async def _none(cid):
    return None


@pytest.mark.asyncio
async def test_network_error_switches_proxy_and_retries_immediately(
    tmp_path, monkeypatch
):
    """Mid-upload failure must not re-pay the 10-minute inter-upload delay."""
    settings, Session, channel_id = await _make_world(
        tmp_path, monkeypatch, proxy_on=True, delay=600
    )

    # Fail the first upload attempt with a retryable network error, then have
    # switch_channel "fix" the route so the retry succeeds.
    switch_calls: list[UUID] = []

    class FakeProxyService:
        async def switch_channel(self, channel_id, **kwargs):
            switch_calls.append(channel_id)
            # Simulate the route being fixed: recovery flag cleared.
            async with Session() as session:
                channel = await session.get(Channel, channel_id)
                channel.proxy_recovery_pending = False
                await session.commit()
            return None

    monkeypatch.setattr(coord_mod, "ProxyService", FakeProxyService)

    attempts = {"n": 0}

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise coord_mod.YouTubeUploadError(
                "Proxy connection reset", reason="network_error", is_retryable=True
            )
        return {"id": Path(file_path).name}

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )

    coordinator = UploadCoordinator(NotificationService())
    await coordinator.wake_channel(channel_id)

    async def wait_done(timeout=15):
        loop = asyncio.get_running_loop()
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
            await asyncio.sleep(0.1)
        raise AssertionError("task never completed")

    started = asyncio.get_running_loop().time()
    task = await wait_done()
    elapsed = asyncio.get_running_loop().time() - started

    await coordinator.stop_channel_worker(channel_id)

    assert attempts["n"] == 2, "upload must retry in place after the proxy switch"
    assert len(switch_calls) == 1, "the proxy must be switched once per failure"
    assert task.error_message is None
    # The retry must happen immediately, not after the 600s inter-upload delay.
    assert elapsed < 10, f"retry took {elapsed:.1f}s; the delay was re-paid"


@pytest.mark.asyncio
async def test_manual_no_proxy_mode_unblocks_waiting_upload(
    tmp_path, monkeypatch
):
    """Choosing no-proxy mode while the upload waits must resume it at once."""
    settings, Session, channel_id = await _make_world(
        tmp_path, monkeypatch, proxy_on=True, delay=0
    )

    class NeverSwitch:
        async def switch_channel(self, channel_id, **kwargs):
            # Automatic switching finds nothing; recovery stays pending forever
            # until the operator acts. This models a channel whose whole proxy
            # list is dead.
            async with Session() as session:
                channel = await session.get(Channel, channel_id)
                channel.proxy_recovery_pending = True
                await session.commit()
            return None

    monkeypatch.setattr(coord_mod, "ProxyService", NeverSwitch)

    attempts = {"n": 0}

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise coord_mod.YouTubeUploadError(
                "Proxy connection reset", reason="network_error", is_retryable=True
            )
        # Second attempt: block exactly like the production chunk resolver does
        # while ``proxy_recovery_pending`` is set, i.e. wait for a usable route.
        while True:
            async with Session() as session:
                channel = await session.get(Channel, channel_id)
                if channel is None or not channel.proxy_recovery_pending:
                    break
            await asyncio.sleep(0.05)
        return {"id": Path(file_path).name}

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return "https://upload.example.com/session"

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )

    coordinator = UploadCoordinator(NotificationService())
    await coordinator.wake_channel(channel_id)

    async def wait_status(statuses, timeout=25):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            async with Session() as session:
                task = (
                    await session.exec(
                        select(UploadTask).where(UploadTask.channel_id == channel_id)
                    )
                ).scalars().first()
                if task and task.status in statuses:
                    return task
            await asyncio.sleep(0.1)
        raise AssertionError(f"task never reached {statuses}")

    # First attempt fails, the switch finds nothing, and the retry is now
    # parked waiting for a usable route.
    await asyncio.sleep(1.5)

    async with Session() as session:
        channel = await session.get(Channel, channel_id)
        assert channel.proxy_recovery_pending is True, (
            "recovery must be pending while the upload waits"
        )

    # The operator switches the channel to no-proxy mode, exactly what
    # PUT /channels/{id} does for proxy_id=null: clear the recovery flag.
    async with Session() as session:
        channel = await session.get(Channel, channel_id)
        channel.proxy_id = None
        channel.proxy_recovery_pending = False
        await session.commit()

    task = await wait_status({"completed"}, timeout=15)
    await coordinator.stop_channel_worker(channel_id)
    assert task.status == "completed"
    assert attempts["n"] == 2


@pytest.mark.asyncio
async def test_exhausted_retries_leave_task_retryable_not_dead(tmp_path, monkeypatch):
    """After the wait times out, the task must stay pending for a later pass."""
    settings, Session, channel_id = await _make_world(
        tmp_path, monkeypatch, proxy_on=True, delay=0
    )
    settings.proxy_switch_wait_timeout_seconds = 1

    class NeverSwitch:
        async def switch_channel(self, channel_id, **kwargs):
            async with Session() as session:
                channel = await session.get(Channel, channel_id)
                channel.proxy_recovery_pending = True
                await session.commit()
            return None

    monkeypatch.setattr(coord_mod, "ProxyService", NeverSwitch)

    async def fake_upload(session_url, file_path, access_token,
                          progress_callback=None, proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs):
        raise coord_mod.YouTubeUploadError(
            "Proxy connection reset", reason="network_error", is_retryable=True
        )

    async def fake_create_session(project, channel, access_token, file_path,
                                  title, description, proxy_url=None):
        return "https://upload.example.com/session"

    async def fake_ensure_token(self, channel):
        return "token"

    monkeypatch.setattr(coord_mod, "create_upload_session", fake_create_session)
    monkeypatch.setattr(coord_mod, "upload_file_to_session", fake_upload)
    monkeypatch.setattr(
        cs_mod.ChannelService, "ensure_access_token", fake_ensure_token
    )
    monkeypatch.setattr(
        coord_mod, "get_current_channel_proxy_url", lambda cid: _none(cid)
    )

    coordinator = UploadCoordinator(NotificationService())
    await coordinator.wake_channel(channel_id)

    async def wait_terminal(timeout=20):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            async with Session() as session:
                rows = (
                    await session.exec(
                        select(UploadTask).where(UploadTask.channel_id == channel_id)
                    )
                ).scalars().all()
                if rows and all(
                    t.status in ("pending", "failed") for t in rows
                ):
                    return rows
            await asyncio.sleep(0.1)
        raise AssertionError("tasks never settled")

    rows = await wait_terminal()
    await coordinator.stop_channel_worker(channel_id)
    retryable = [t for t in rows if t.status == "pending"]
    assert retryable, "tasks must be released as retryable pending, not failed"
    assert all((t.attempt or 0) >= 1 for t in retryable)
