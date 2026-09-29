"""Channels sharing a video folder form one upload queue.

A channel whose video folder references another channel's folder (see
``folder_from_channel_id``) shares the folder — and each sharer has its own
worker. Two bugs made every file in a shared folder upload once PER SHARING
channel (the user saw "two videos uploaded in a row" exactly on channels
whose folder references another channel):

1. uploaded-hash bookkeeping was filtered to the collecting channel's id, so
   a sharer never saw another sharer's completed uploads;
2. two sharers' workers collecting in the same scan pass each claimed the
   same files — the "already claimed by an active task" filter only sees
   committed rows, which land after both collectors ran.

Fixed behavior pinned here:

* each file uploads exactly once, no matter which sharer's worker gets it;
* the uploaded hash is recorded for every sharer, so no sharer re-uploads;
* active tasks of other sharers exclude a file from collection;
* inter-upload pacing is PER CHANNEL: a sharer's own delay window anchors
  to its own last upload; one sharer waiting out its window does not hold
  the other sharer back (only the transfer itself is serialized per
  shared folder, so files never upload twice);
* a channel with its own dedicated folder is unaffected.
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
from app.models.uploaded_hash import UploadedVideoHash
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
import app.services.coordinator as coord_mod
import app.services.channel_service as cs_mod
import app.core.db as db_mod


DELAY = 4.0


def _patch_transport(monkeypatch, spans):
    async def fake_create_session(
        project, channel, access_token, file_path, title, description, proxy_url=None
    ):
        return f"https://u/{channel.title}/{Path(file_path).name}"

    async def fake_upload(
        session_url, file_path, access_token, progress_callback=None,
        proxy_url=None, proxy_resolver=None, start_offset=0, **kwargs,
    ):
        loop = asyncio.get_running_loop()
        # URL shape: https://u/{channel_title}/{file_name}
        parts = [p for p in session_url.split("/") if p]
        who, name = parts[-2], parts[-1]
        spans.append(("upload_start", loop.time(), f"{who}:{name}"))
        await asyncio.sleep(0.05)
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


async def _make_shared_world(tmp_path, monkeypatch, *, n_files=2, n_sharers=2):
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

        channel_ids = []
        for idx in range(n_sharers):
            ch = Channel(
                youtube_channel_id=f"yt-{uuid4().hex[:8]}",
                title=f"ch{idx}",
                project_id=project.id,
                refresh_token_enc="x",
                access_token_enc="x",
                access_token_expires_at=datetime.now(timezone.utc)
                + timedelta(hours=1),
                folder_path="videos/shared",
                max_batch_size=1,
                min_batch_size=1,
                inter_upload_delay_seconds=int(DELAY),
            )
            session.add(ch)
            channel_ids.append(ch.id)
        await session.commit()

    return settings, Session, channel_ids, engine


def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
    import app.api.routes.channels as routes_mod

    notes = NotificationService()
    coord = UploadCoordinator(notes)
    monkeypatch.setattr(coord_mod, "coordinator", coord)
    monkeypatch.setattr(routes_mod, "coordinator", coord)
    monkeypatch.setattr(cs_mod, "notifications", notes)
    return coord


async def _wait_completed(Session, expected, timeout=60):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    rows = []
    while loop.time() < deadline:
        async with Session() as session:
            rows = (
                await session.exec(
                    select(UploadTask).where(
                        UploadTask.status == "completed"
                    )
                )
            ).scalars().all()
            if len(rows) >= expected:
                return rows
        await asyncio.sleep(0.1)
    raise AssertionError(f"only {len(rows)} uploads completed, expected {expected}")


@pytest.mark.asyncio
async def test_shared_folder_each_file_uploads_once(tmp_path, monkeypatch):
    """Two channels share one folder: each file goes to YouTube exactly once."""
    spans: list = []
    settings, Session, channel_ids, engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=3, n_sharers=2
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_completed(Session, expected=3)
        await asyncio.sleep(0.5)

        uploaded_files = [
            who_name.split(":")[1]
            for kind, _, who_name in spans
            if kind == "upload_start"
        ]
        assert len(uploaded_files) == 3, (
            f"expected exactly 3 uploads in a shared folder, got "
            f"{len(uploaded_files)}: {uploaded_files}"
        )
        assert sorted(uploaded_files) == ["v0.mp4", "v1.mp4", "v2.mp4"]

        # The hash is recorded for BOTH sharers: neither will re-upload.
        async with Session() as session:
            rows = (
                await session.exec(select(UploadedVideoHash))
            ).scalars().all()
            per_channel = {}
            for row in rows:
                per_channel.setdefault(str(row.channel_id), set()).add(
                    row.file_hash
                )
            assert len(per_channel) == 2, (
                "uploaded hashes must be recorded for every folder sharer"
            )
            hashes_by_channel = list(per_channel.values())
            assert hashes_by_channel[0] == hashes_by_channel[1] == set(
                h for h in hashes_by_channel[0]
            )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_shared_folder_pacing_is_per_channel(tmp_path, monkeypatch):
    """Pacing anchors to each channel's OWN last upload, not the folder's.

    Both sharers' first uploads (no completion history) go up promptly:
    sharer B must NOT wait out sharer A's delay window just because A
    uploaded a moment ago. Each channel's own window then paces ITS next
    upload.
    """
    spans: list = []
    settings, Session, channel_ids, engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=4, n_sharers=2
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_completed(Session, expected=4)

        who_starts = [who for kind, _, who_name in spans
                      if kind == "upload_start" for who in [who_name.split(":")[0]]]
        starts = sorted(t for kind, t, _ in spans if kind == "upload_start")
        assert len(starts) == 4

        # Independence: the first two uploads (one per sharer, no own
        # history yet) start close together — B did not wait A's window.
        first_pair_gap = starts[1] - starts[0]
        assert first_pair_gap < DELAY - 0.5, (
            f"second sharer waited {first_pair_gap:.2f}s after the first; "
            "pacing must be per channel, not per shared folder"
        )

        # Each channel's OWN window still paces its next upload: the 3rd
        # upload (by whichever sharer uploaded 1st) waits out that sharer's
        # full delay since its own previous upload.
        by_sharer: dict[str, list[float]] = {}
        for kind, t, who_name in spans:
            if kind == "upload_start":
                who = who_name.split(":")[0]
                by_sharer.setdefault(who, []).append(t)
        for who, ts in by_sharer.items():
            for prev, nxt in zip(ts, ts[1:]):
                gap = nxt - prev
                assert gap >= DELAY - 0.5, (
                    f"sharer {who} uploaded twice {gap:.2f}s apart; its own "
                    f"inter_upload_delay of {DELAY}s was not respected"
                )
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_active_task_of_sharer_excludes_file(tmp_path, monkeypatch):
    """A file claimed by one sharer's active task is not collected by another.

    This pins the collection filter directly: worker A has a pending task
    for v0.mp4; worker B's collection must skip it (and must not duplicate
    v0 into its own batch).
    """
    from app.services.channel_service import ChannelService

    spans: list = []
    settings, Session, channel_ids, engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=2, n_sharers=2
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        a_id, b_id = channel_ids
        async with Session() as session:
            # Sharer A already has a pending task for v0.mp4.
            session.add(
                UploadTask(
                    channel_id=a_id,
                    file_path="videos/shared/v0.mp4",
                    status="pending",
                    file_hash="hash-v0",
                )
            )
            await session.commit()

            channel_b = await session.get(Channel, b_id)
            collected, limit = await coordinator._collect_new_files(
                session, channel_b, ChannelService(session)
            )

        collected_paths = [p for p, _ in collected]
        assert "videos/shared/v0.mp4" not in collected_paths, (
            "a file claimed by another sharer's active task must not be "
            "collected again"
        )
        assert "videos/shared/v1.mp4" in collected_paths
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_shared_folder_shuffle_prefers_least_used_other_channel_videos(
    tmp_path, monkeypatch
):
    """Shuffle distributes a shared folder before reusing another sharer's video."""
    from app.services.channel_service import ChannelService

    spans: list = []
    settings, Session, channel_ids, engine = await _make_shared_world(
        tmp_path, monkeypatch, n_files=3, n_sharers=2
    )
    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        a_id, b_id = channel_ids
        async with Session() as session:
            channel_b = await session.get(Channel, b_id)
            channel_b.shuffle_upload_order = True
            await session.commit()

            # Get the real content hashes used by collection, then give v0 a
            # completed upload on the other channel. v1/v2 were never used,
            # so one of them must be first regardless of random tie-breaking.
            collected, _ = await coordinator._collect_new_files(
                session, channel_b, ChannelService(session)
            )
            hashes = {Path(path).name: file_hash for path, file_hash in collected}
            session.add(
                UploadTask(
                    channel_id=a_id,
                    file_path="videos/shared/v0.mp4",
                    file_hash=hashes["v0.mp4"],
                    status="completed",
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()

            collected, _ = await coordinator._collect_new_files(
                session, channel_b, ChannelService(session)
            )
            assert Path(collected[0][0]).name in {"v1.mp4", "v2.mp4"}

            # Once every file has been used, randomisation is restricted to
            # the least-used tier rather than choosing the most-used v1.
            for _ in range(2):
                session.add(
                    UploadTask(
                        channel_id=a_id,
                        file_path="videos/shared/v1.mp4",
                        file_hash=hashes["v1.mp4"],
                        status="completed",
                        completed_at=datetime.now(timezone.utc),
                    )
                )
            session.add(
                UploadTask(
                    channel_id=a_id,
                    file_path="videos/shared/v2.mp4",
                    file_hash=hashes["v2.mp4"],
                    status="completed",
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()

            collected, _ = await coordinator._collect_new_files(
                session, channel_b, ChannelService(session)
            )
            assert Path(collected[0][0]).name in {"v0.mp4", "v2.mp4"}
    finally:
        await coordinator.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_dedicated_folders_unaffected(tmp_path, monkeypatch):
    """Channels with separate folders keep fully independent queues."""
    spans: list = []
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

    ids = []
    for idx in range(2):
        folder = tmp_path / "videos" / f"ch{idx}"
        folder.mkdir(parents=True)
        (folder / "same-hash.mp4").write_bytes(b"identical-content")
        ids.append((folder, f"videos/ch{idx}"))

    async with Session() as session:
        project = GCPProject(
            name="p", client_id="x", client_secret_enc="x",
            redirect_uri="http://localhost", scope="x", token_uri="x", auth_uri="x",
            api_upload="https://www.googleapis.com/upload/youtube/v3/videos",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)
        for idx, (_, rel) in enumerate(ids):
            session.add(
                Channel(
                    youtube_channel_id=f"yt-{uuid4().hex[:8]}",
                    title=f"ded{idx}",
                    project_id=project.id,
                    refresh_token_enc="x",
                    access_token_enc="x",
                    access_token_expires_at=datetime.now(timezone.utc)
                    + timedelta(hours=1),
                    folder_path=rel,
                    max_batch_size=1,
                    min_batch_size=1,
                    inter_upload_delay_seconds=1,
                )
            )
        await session.commit()

    _patch_transport(monkeypatch, spans)
    coordinator = _fresh_coordinator(monkeypatch)

    try:
        await coordinator.start()
        await _wait_completed(Session, expected=2)
        await asyncio.sleep(0.5)
        # Same content in separate folders: each channel uploads its own copy.
        starts = [w for kind, _, w in spans if kind == "upload_start"]
        assert len(starts) == 2, spans
    finally:
        await coordinator.stop()
        await engine.dispose()
