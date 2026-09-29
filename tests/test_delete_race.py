"""DELETE /channels/{id} must close the scan-loop worker-resurrection race.

The scan loop wakes a worker for every active channel every
``channel_scan_interval_seconds``. The DELETE endpoint used to stop the
worker first and delete the DB row last; under SQLite write contention that
window stretches to seconds, and the scan loop resurrects a worker for the
channel being deleted. The resurrected worker loads the still-existing row,
starts a new batch, uploads videos and broadcasts ``channel.updated`` with
the channel payload — so the client resurrects the just-deleted channel in
the UI ("the channel does not get deleted").
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models  # noqa: F401
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
import app.services.coordinator as coord_mod
import app.services.channel_service as cs_mod
import app.services.proxy_service as proxy_mod
import app.core.db as db_mod


def _fresh_coordinator(monkeypatch) -> UploadCoordinator:
    """Build an isolated coordinator for this test's event loop.

    The module-level ``coordinator`` binds its Events/Locks to the first
    event loop that uses it; pytest gives every test a new loop, so a fresh
    instance per test avoids cross-loop state.
    """
    import app.api.routes.channels as routes_mod

    notes = NotificationService()
    coord = UploadCoordinator(notes)
    monkeypatch.setattr(coord_mod, "coordinator", coord)
    monkeypatch.setattr(routes_mod, "coordinator", coord)
    monkeypatch.setattr(cs_mod, "notifications", notes)
    return coord


async def _make_world(tmp_path, monkeypatch, *, n_files=3):
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
    for i in range(n_files):
        (folder / f"v{i}.mp4").write_bytes(b"video-data")

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
                max_batch_size=10,
                min_batch_size=1,
                inter_upload_delay_seconds=0,
                upload_limit_enabled=False,
            )
        )
        await session.commit()

    return settings, Session, channel_id, engine


def _patch_transport(monkeypatch, upload_gate: asyncio.Event, started: asyncio.Event):
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
        started.set()
        await upload_gate.wait()
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


@pytest.mark.asyncio
async def test_delete_closes_worker_resurrection_race(tmp_path, monkeypatch):
    settings, Session, channel_id, engine = await _make_world(tmp_path, monkeypatch)

    upload_gate = asyncio.Event()
    upload_started = asyncio.Event()
    _patch_transport(monkeypatch, upload_gate, upload_started)

    from httpx import ASGITransport, AsyncClient

    from app.api.deps import get_session
    from app.core.config import settings as global_settings
    from app.main import app

    async def override_get_session():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        coordinator = _fresh_coordinator(monkeypatch)
        await coordinator.start()

        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://test", timeout=5.0)
        headers = {"Authorization": f"Bearer {global_settings.api_token}"}
        events: list[tuple[str, dict]] = []
        _patch_broadcast(monkeypatch, events)

        await asyncio.wait_for(upload_started.wait(), timeout=30)

        # Deterministically reproduce the scan-loop race: the scan fires
        # wake_channel for the channel right after the DELETE endpoint has
        # stopped the previous worker but before the DB row is gone. The
        # sleep models the real-world delay where the endpoint's writes queue
        # behind SQLite write contention — long enough for the resurrected
        # worker to load the still-existing row and start a batch.
        original_stop = coordinator.stop_channel_worker

        async def racing_stop(cid, **kwargs):
            result = await original_stop(cid, **kwargs)
            if cid == channel_id:
                await coordinator.wake_channel(cid)
                # Give the resurrected worker time to load the still-existing
                # row and enter its (blocked) batch before the endpoint
                # deletes the row.
                await asyncio.sleep(1.5)
            return result

        monkeypatch.setattr(coordinator, "stop_channel_worker", racing_stop)

        events.clear()
        resp = await client.delete(f"/channels/{channel_id}", headers=headers)
        assert resp.status_code == 204

        # Release the uploads and give a resurrected worker time to run a
        # whole batch (uploads complete -> channel.updated broadcast).
        upload_gate.set()
        await asyncio.sleep(4)

        zombie_events = [
            (name, payload)
            for name, payload in events
            if _mentions_channel(payload, channel_id) and name != "channel.deleted"
        ]
        assert not zombie_events, (
            "the deleted channel kept broadcasting events (client would "
            "resurrect it in the UI): "
            f"{[(n, json.dumps(p)[:150]) for n, p in zombie_events]}"
        )

        async with coordinator._lock:
            assert channel_id not in coordinator._workers, (
                "a worker is still alive for the deleted channel"
            )

        await client.aclose()
    finally:
        app.dependency_overrides.clear()
        await coordinator.stop()
        await engine.dispose()


def _patch_broadcast(monkeypatch, events: list) -> None:
    """Record every notification instead of streaming it over SSE."""
    from app.services.notification_service import notifications

    original = notifications.broadcast

    async def recording_broadcast(event_type, payload):
        events.append((event_type, payload))
        return await original(event_type, payload)

    monkeypatch.setattr(notifications, "broadcast", recording_broadcast)


@pytest.mark.asyncio
async def test_delete_with_mutual_and_shared_folders(tmp_path, monkeypatch):
    """DELETE stays fast and folder removal is correct even when channels
    reference each other's folders (A -> B's folder, B -> A's folder) or
    share one folder. Reproduces the reported "circular folder dependency"
    scenario around channel deletion.
    """
    from httpx import ASGITransport, AsyncClient

    from app.api.deps import get_session
    from app.core.config import settings as global_settings
    from app.main import app

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

    a_id, b_id, c_id, d_id = uuid4(), uuid4(), uuid4(), uuid4()

    def _mk_channel(cid, folder):
        return Channel(
            id=cid,
            youtube_channel_id=f"yt-{cid.hex[:8]}",
            title=f"ch-{cid.hex[:4]}",
            refresh_token_enc="x",
            access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc)
            + timedelta(hours=1),
            folder_path=folder,
            project_id=project_id,
            # "expired" keeps workers/metadata loops from touching the network
            # with the fake tokens; the DELETE flow does not care.
            session_status="expired",
            max_batch_size=1,
            min_batch_size=1,
            inter_upload_delay_seconds=0,
            upload_limit_enabled=False,
        )

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
        project_id = project.id

        # Mutual references: A uses B's dedicated folder, B uses A's.
        session.add(_mk_channel(a_id, f"videos/{b_id}"))
        session.add(_mk_channel(b_id, f"videos/{a_id}"))
        # Shared folder: C and D both use videos/shared.
        session.add(_mk_channel(c_id, "videos/shared"))
        session.add(_mk_channel(d_id, "videos/shared"))
        await session.commit()

    videos = tmp_path / "videos"
    for sub in (str(b_id), str(a_id), "shared"):
        (videos / sub).mkdir(parents=True, exist_ok=True)

    coordinator = _fresh_coordinator(monkeypatch)

    async def override_get_session():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        await coordinator.start()
        await asyncio.sleep(1.5)  # let the scan loop spawn workers

        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test", timeout=15.0
        ) as client:
            headers = {"Authorization": f"Bearer {global_settings.api_token}"}

            async def _delete(cid):
                loop = asyncio.get_event_loop()
                started = loop.time()
                resp = await client.delete(f"/channels/{cid}", headers=headers)
                elapsed = loop.time() - started
                assert resp.status_code == 204, resp.text
                assert elapsed < 10, f"DELETE hung for {elapsed:.1f}s"
                return elapsed

            # Mutual references: delete A -> its assigned folder (B's
            # dedicated) goes away, B and its own folder survive.
            await _delete(a_id)
            await _wait_folder_gone(videos / str(b_id))
            assert (videos / str(a_id)).exists(), "B's folder must survive A"
            async with Session() as session:
                assert await session.get(Channel, b_id) is not None

            # Shared folder: deleting C must NOT remove it while D uses it.
            await _delete(c_id)
            await asyncio.sleep(1.0)
            assert (videos / "shared").exists(), (
                "shared folder removed while channel D still uses it"
            )

            # Last user deleted -> folder goes away.
            await _delete(d_id)
            await _wait_folder_gone(videos / "shared")

            # B remains deletable too.
            await _delete(b_id)
            await _wait_folder_gone(videos / str(a_id))
    finally:
        app.dependency_overrides.clear()
        await coordinator.stop()
        await engine.dispose()


async def _wait_folder_gone(folder, timeout=10):
    """Folder removal is backgrounded; poll until it disappears."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if not folder.exists():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"folder was not removed: {folder}")


def _mentions_channel(payload: dict, channel_id) -> bool:
    cid = str(channel_id)
    if payload.get("channel_id") == cid:
        return True
    channel = payload.get("channel")
    if isinstance(channel, dict) and channel.get("id") == cid:
        return True
    tasks = payload.get("tasks")
    if isinstance(tasks, list):
        for t in tasks:
            if isinstance(t, dict) and t.get("channel_id") == cid:
                return True
    task = payload.get("task")
    if isinstance(task, dict) and task.get("channel_id") == cid:
        return True
    return False
