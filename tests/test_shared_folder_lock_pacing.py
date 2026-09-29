"""Fixes for the two user-reported bugs, pinned by these tests.

Bug 1 — changing ``inter_upload_delay_seconds`` mid-wait had no effect for a
channel sharing its video folder: the whole batch (including the pacing
wait) ran under the shared-folder lock, so the "blocked" channel was not in
a pacing wait at all — it was waiting for the lock, and ``wake_pacing``
could not reach it. The pacing wait now runs WITHOUT the lock; the lock is
taken for the transfer only, and the delay window is re-checked under the
lock (another sharer may have completed an upload while this batch waited).

Bug 2 — the last video allowed by the limit never uploaded: a channel whose
own delay window was still open CLAIMED files from the shared folder and
parked them behind its delay, so a ready sharer could not take them; the
queue stalled for the whole window even though the limit allowed more
uploads. A channel with an open window now skips collection; the next tick
retries once the window elapses (or is lowered by a settings save).

Also pinned: the ``NameError: Proxy`` that failed every upload retried after
a mid-upload proxy failure (``_proxy_route_ready`` referenced ``Proxy``
without importing it).
"""

from __future__ import annotations

import asyncio
import time
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


async def _make_shared_world(
    tmp_path, monkeypatch, *, n_files=5, a_delay=2, b_delay=30,
    a_limit_count=5, a_limit_enabled=True,
):
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
    for i in range(n_files):
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

        a_id, b_id = uuid4(), uuid4()
        session.add(Channel(
            id=a_id, youtube_channel_id="ytA", title="A",
            project_id=project.id,
            refresh_token_enc="x", access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path="videos/shared",
            max_batch_size=1, min_batch_size=1,
            inter_upload_delay_seconds=a_delay,
            upload_limit_enabled=a_limit_enabled,
            upload_limit_count=a_limit_count,
            upload_limit_period_hours=24,
        ))
        session.add(Channel(
            id=b_id, youtube_channel_id="ytB", title="B",
            project_id=project.id,
            refresh_token_enc="x", access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path="videos/shared",
            max_batch_size=1, min_batch_size=1,
            inter_upload_delay_seconds=b_delay,
        ))
        await session.commit()

    return settings, Session, (a_id, b_id), engine


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
        loop = asyncio.get_running_loop()
        parts = [p for p in session_url.split("/") if p]
        who, name = parts[-2], parts[-1]
        spans.append(("upload_start", loop.time(), f"{who}:{name}"))
        await asyncio.sleep(0.2)
        spans.append(("completed", loop.time(), f"{who}:{name}"))
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


def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
    import app.api.routes.channels as routes_mod

    notes = NotificationService()
    coord = UploadCoordinator(notes)
    monkeypatch.setattr(coord_mod, "coordinator", coord)
    monkeypatch.setattr(routes_mod, "coordinator", coord)
    monkeypatch.setattr(cs_mod, "notifications", notes)
    return coord


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


async def _wait_uploads(Session, expected, timeout=60):
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
async def test_delay_change_reaches_channel_behind_shared_folder_lock(
    tmp_path, monkeypatch
):
    """Bug 1: the sharer blocked by another sharer's long pacing wait.

    The lock used to be held across the whole batch including the pacing
    wait; the blocked channel was waiting for the LOCK, not pacing, so
    lowering its delay through PUT /channels/{id} did nothing until the
    other channel's window expired. Now the wait runs without the lock and
    the wake applies within seconds.
    """
    spans: list = []
    settings, Session, (a_id, b_id), engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=4, a_delay=60, b_delay=60,
        a_limit_enabled=False,
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)
    loop = asyncio.get_running_loop()

    try:
        await coordinator.start()
        # First upload goes up immediately (no completion history yet).
        rows = await _wait_uploads(Session, expected=1, timeout=30)
        assert len(rows) == 1

        # ~2s into the (now 60s) wait of whichever sharer holds the queue:
        # lower the OTHER sharer's delay to 1s. Under the old code that
        # channel was stuck on the folder lock; the queue must drain soon.
        await asyncio.sleep(2.0)
        saved_at = loop.time()
        resp = await _put_channel(
            monkeypatch, Session, b_id, {"inter_upload_delay_seconds": 1}
        )
        assert resp.status_code == 200, resp.text

        rows = await _wait_uploads(Session, expected=4, timeout=20)
        waited = loop.time() - saved_at
        assert len(rows) == 4, (
            f"only {len(rows)} of 4 videos uploaded {waited:.1f}s after "
            "lowering the blocked sharer's delay; the shared-folder queue "
            "was still stuck behind the lock"
        )
        assert waited < 15, (
            f"queue drained only {waited:.1f}s after the save; the lowered "
            "delay did not interrupt the shared-folder wait"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_last_video_in_limit_uploads_despite_slow_sharer(
    tmp_path, monkeypatch
):
    """Bug 2: limit allows N uploads, the Nth must still happen.

    A slow sharer (30s delay) used to CLAIM files and park them behind its
    window while the limited channel still had quota; the Nth video waited
    out the slow channel's delay for no reason. A channel whose window is
    open must not claim files: the ready sharer takes them.
    """
    spans: list = []
    settings, Session, (a_id, b_id), engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=5, a_delay=2, b_delay=30,
        a_limit_enabled=True, a_limit_count=5,
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        # A allows 5 per 24h; all five files must complete well before B's
        # 30s window could ever elapse.
        rows = await _wait_uploads(Session, expected=5, timeout=40)
        assert len(rows) == 5, (
            f"only {len(rows)} of 5 allowed videos uploaded; the last one "
            "was parked behind the slow sharer's delay window"
        )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_sharers_pace_independently_under_transfer_lock(
    tmp_path, monkeypatch
):
    """Two sharers with no completion history upload without waiting for
    each other (pacing is per channel), while the transfer lock still
    serializes their batches so no file uploads twice.
    """
    spans: list = []
    settings, Session, (a_id, b_id), engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=4, a_delay=4, b_delay=4,
        a_limit_enabled=False,
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        rows = await _wait_uploads(Session, expected=4, timeout=60)
        assert len(rows) == 4

        starts = sorted(t for kind, t, _ in spans if kind == "upload_start")
        # First two uploads (one per sharer, no own history): close together.
        assert starts[1] - starts[0] < 2.0, (
            "a sharer with no completion history waited for the other "
            "sharer's delay window; pacing must be per channel"
        )

        # Each sharer's own window paces ITS next upload.
        by_sharer: dict[str, list[float]] = {}
        for kind, t, who in spans:
            if kind == "upload_start":
                by_sharer.setdefault(who, []).append(t)
        for who, ts in by_sharer.items():
            for prev, nxt in zip(ts, ts[1:]):
                assert nxt - prev >= 4.0 - 0.5, (
                    f"sharer {who} uploaded twice {nxt - prev:.2f}s apart; "
                    "its own inter_upload_delay was not respected"
                )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_proxy_imported_for_route_ready_check(tmp_path, monkeypatch):
    """A mid-upload proxy failure must not fail with NameError.

    ``_proxy_route_ready`` resolves the assigned proxy row via
    ``session.get(Proxy, ...)``; ``Proxy`` must be importable in the
    coordinator module (a NameError used to fail the retried upload with
    "Upload failed: name 'Proxy' is not defined").
    """
    from app.models.proxy import Proxy

    assert coord_mod.Proxy is Proxy

    # And the failing path end-to-end: one retryable network error, the
    # switch clears recovery, the retry must complete the task.
    from app.core import security

    enc = lambda v: security._fernet.encrypt(v.encode()).decode()  # noqa: E731
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        channel_scan_interval_seconds=1,
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
        proxy = Proxy(url_enc=enc("socks5://u:p@10.0.0.1:1080"), is_active=True)
        session.add(proxy)
        await session.commit()
        await session.refresh(proxy)
        session.add(Channel(
            id=channel_id, youtube_channel_id="yc", title="t",
            project_id=project.id,
            refresh_token_enc="x", access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path=f"videos/{channel_id}",
            max_batch_size=10, min_batch_size=1,
            inter_upload_delay_seconds=600,
            proxy_id=proxy.id, proxy_auto_switch=True,
        ))
        await session.commit()

    class FakeProxyService:
        async def switch_channel(self, channel_id, **kwargs):
            async with Session() as session:
                channel = await session.get(Channel, channel_id)
                channel.proxy_recovery_pending = False
                await session.commit()
            return None

    monkeypatch.setattr(coord_mod, "ProxyService", FakeProxyService)

    attempts = {"n": 0}

    async def fake_upload(
        session_url, file_path, access_token, progress_callback=None,
        proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs,
    ):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise coord_mod.YouTubeUploadError(
                "Proxy connection reset", reason="network_error",
                is_retryable=True,
            )
        return {"id": Path(file_path).name}

    async def fake_create_session(
        project, channel, access_token, file_path, title, description,
        proxy_url=None,
    ):
        return f"https://upload.example.com/{Path(file_path).name}"

    async def fake_ensure_token(self, channel, *, request_reauth=True):
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

    coordinator = UploadCoordinator(NotificationService())
    try:
        await coordinator.wake_channel(channel_id)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        task = None
        while loop.time() < deadline:
            await asyncio.sleep(0.1)
            async with Session() as session:
                task = (
                    await session.exec(
                        select(UploadTask).where(
                            UploadTask.channel_id == channel_id
                        )
                    )
                ).scalars().first()
                if task and task.status in ("completed", "failed"):
                    break
        assert task is not None, "task was never created"
        assert task.status == "completed", (
            f"retry after proxy switch failed: {task.error_message}"
        )
        assert attempts["n"] == 2
    finally:
        await coordinator.stop_channel_worker(channel_id)
        await engine.dispose()
