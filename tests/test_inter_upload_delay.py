"""Inter-upload delay pacing between batches, including with upload limits.

``inter_upload_delay_seconds`` must space consecutive upload batches: the
next batch's uploads start no earlier than
``last_completed_upload + inter_upload_delay_seconds``. This must also hold
when the configurable upload limit trims the batch size, and after the
delay is changed via ``PUT /channels/bulk-settings``.
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
import app.services.proxy_service as proxy_mod
import app.core.db as db_mod


def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
    """Build an isolated coordinator for this test's event loop.

    The module-level ``coordinator`` (and its Events/Locks) binds to the
    first event loop that uses it; pytest gives every test a new loop, so
    reusing it across tests raises "bound to a different event loop" or
    worse, leaks workers between tests. A fresh instance per test avoids all
    of that. The routes and services are patched to point at it.
    """
    import app.api.routes.channels as routes_mod

    notes = NotificationService()
    coord = UploadCoordinator(notes)
    monkeypatch.setattr(coord_mod, "coordinator", coord)
    monkeypatch.setattr(routes_mod, "coordinator", coord)
    # channel_service broadcasts through the module-level notifications.
    monkeypatch.setattr(cs_mod, "notifications", notes)
    return coord


async def _make_world(
    tmp_path,
    monkeypatch,
    *,
    n_files=4,
    max_batch_size=2,
    min_batch_size=1,
    inter_upload_delay_seconds=3,
    upload_limit_enabled=False,
    upload_limit_count=10,
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
    monkeypatch.setattr(proxy_mod, "settings", settings)
    monkeypatch.setattr(proxy_mod, "async_session_maker", Session)
    monkeypatch.setattr(db_mod, "async_session_maker", Session)

    channel_id = uuid4()
    folder = tmp_path / "videos" / str(channel_id)
    folder.mkdir(parents=True)
    # Distinct content per file: identical bytes would hash the same and the
    # second batch would be skipped as "already uploaded".
    for i in range(n_files):
        (folder / f"v{i}.mp4").write_bytes(f"video-data-{i}".encode())

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
                max_batch_size=max_batch_size,
                min_batch_size=min_batch_size,
                inter_upload_delay_seconds=inter_upload_delay_seconds,
                upload_limit_enabled=upload_limit_enabled,
                upload_limit_count=upload_limit_count,
                upload_limit_period_hours=24,
            )
        )
        await session.commit()

    return settings, Session, channel_id, engine


def _patch_transport(monkeypatch):
    """Fake transport recording each upload's start/end monotonic times."""
    spans: list[tuple[float, float]] = []

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
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.sleep(0.05)
        spans.append((start, loop.time()))
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

    async def fake_refresh_snippet(self, channel, access_token):
        return False

    monkeypatch.setattr(
        cs_mod.ChannelService, "refresh_channel_snippet", fake_refresh_snippet
    )
    return spans


async def _wait_uploads(Session, channel_id, expected, timeout=60):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
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


def _batch_gaps(spans: list[tuple[float, float]], batch_size: int):
    """Group upload spans into batches and return (last_end, next_start) gaps."""
    spans = sorted(spans)
    gaps = []
    for i in range(batch_size, len(spans), batch_size):
        prev = spans[i - batch_size : i]
        gaps.append((max(end for _, end in prev), spans[i][0]))
    return gaps


@pytest.mark.asyncio
async def test_inter_upload_delay_between_batches(tmp_path, monkeypatch):
    """Next batch waits out inter_upload_delay since the last completion."""
    settings, Session, channel_id, engine = await _make_world(
        tmp_path,
        monkeypatch,
        n_files=4,
        max_batch_size=2,
        inter_upload_delay_seconds=3,
    )
    spans = _patch_transport(monkeypatch)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=4)
        # Let any premature next batch surface.
        await asyncio.sleep(1)

        assert len(spans) == 4, f"expected 4 uploads, got {len(spans)}"
        gaps = _batch_gaps(spans, batch_size=2)
        assert gaps, "uploads did not run in two batches"
        # Start-to-start (see the limit test for why completion-based gaps
        # flake under load).
        starts_sorted = sorted(start for start, _ in spans)
        for prev_start, next_start in zip(starts_sorted, starts_sorted[2:]):
            actual = next_start - prev_start
            assert actual >= 3.0 - 0.3, (
                f"inter-batch gap {actual:.2f}s < inter_upload_delay 3s"
            )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_inter_upload_delay_with_upload_limit(tmp_path, monkeypatch):
    """Delay also holds when the limit trims batches (2 + 1 of 3 allowed)."""
    settings, Session, channel_id, engine = await _make_world(
        tmp_path,
        monkeypatch,
        n_files=4,
        max_batch_size=2,
        inter_upload_delay_seconds=3,
        upload_limit_enabled=True,
        upload_limit_count=3,
    )
    spans = _patch_transport(monkeypatch)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=3)

        assert len(spans) == 3, f"expected 3 uploads, got {len(spans)}"
        # Batch 1 = 2 uploads, batch 2 = 1 upload (limit remaining).
        # Start-to-start between the batches: an upload "end" span fires
        # before ``last_upload_at`` commits in the result loop, which flakes
        # under a full-suite load.
        batch1_last_start = max(start for start, _ in spans[:2])
        batch2_start = min(start for start, _ in spans[2:])
        actual = batch2_start - batch1_last_start
        assert actual >= 3.0 - 0.3, (
            f"inter-batch gap with limit {actual:.2f}s < inter_upload_delay 3s"
        )

        # The 4th file must not upload: the channel gets paused by the limit.
        # The pause is committed right after the 3rd upload completes, so
        # poll for it instead of asserting immediately.
        async def _wait_paused(timeout=10):
            loop = asyncio.get_event_loop()
            deadline = loop.time() + timeout
            while loop.time() < deadline:
                async with Session() as session:
                    channel = await session.get(Channel, channel_id)
                    if channel.status == "paused":
                        return channel
                await asyncio.sleep(0.1)
            return channel

        channel = await _wait_paused()
        assert channel.status == "paused", (
            f"channel not paused by upload limit: {channel.status}"
        )
        assert channel.pause_reason == "upload_limit"
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_bulk_settings_delay_applies_to_worker(tmp_path, monkeypatch):
    """A delay changed via bulk-settings takes effect for the next batches."""
    from httpx import ASGITransport, AsyncClient

    from app.api.deps import get_session
    from app.core.config import settings as global_settings
    from app.main import app

    # Start at 5s so the second batch cannot begin before the bulk update
    # lands; bulk then lowers it to 3s, and the measured gap must follow the
    # NEW value (≈3s), not the old one (≈5s).
    settings, Session, channel_id, engine = await _make_world(
        tmp_path,
        monkeypatch,
        n_files=4,
        max_batch_size=2,
        inter_upload_delay_seconds=5,
    )
    spans = _patch_transport(monkeypatch)
    coordinator = _fresh_coordinator(monkeypatch)

    async def override_get_session():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        await coordinator.start()
        await _wait_uploads(Session, channel_id, expected=2)

        # Bulk-change the delay to 3s and the batch size, exactly like the
        # client's bulk settings panel does.
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": f"Bearer {global_settings.api_token}"}
            resp = await client.put(
                "/channels/bulk-settings",
                headers=headers,
                json={
                    "channel_ids": [str(channel_id)],
                    "inter_upload_delay_seconds": 3,
                    "max_batch_size": 2,
                    "min_batch_size": 1,
                    "shuffle_upload_order": False,
                },
            )
            assert resp.status_code == 200, resp.text
            updated = resp.json()["updated"]
            assert updated[0]["inter_upload_delay_seconds"] == 3

        await _wait_uploads(Session, channel_id, expected=4)
        await asyncio.sleep(1)

        assert len(spans) == 4
        # Start-to-start between the batches (completion-based gaps flake
        # under load — see the limit test).
        starts_sorted = sorted(start for start, _ in spans)
        actual = starts_sorted[2] - starts_sorted[1]
        # New value (3s) must apply: ~3s gap. If the bulk-saved value were
        # ignored, the gap would be ~5s (the pre-bulk delay).
        assert 2.5 <= actual <= 4.6, (
            f"post-bulk inter-batch gap {actual:.2f}s does not match the "
            "bulk-saved inter_upload_delay_seconds=3"
        )
    finally:
        app.dependency_overrides.clear()
        await coordinator.stop()
        await engine.dispose()
