"""Tests for PUT /channels/{channel_id} folder semantics and GET /tasks limit."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import app.api.routes.channels as routes
import app.api.routes.tasks as tasks_routes
from app.models.channel import Channel
from app.models.upload_task import UploadTask


def make_channel(**overrides) -> Channel:
    base = dict(
        youtube_channel_id=f"yt-{uuid4().hex[:8]}",
        title="Test channel",
        refresh_token_enc="r",
        access_token_enc="a",
        access_token_expires_at=datetime.now(timezone.utc),
    )
    base.update(overrides)
    return Channel(**base)


@pytest.fixture
async def app_client(tmp_path, monkeypatch):
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    import app.models.channel  # noqa: F401
    import app.models.channel_project_auth  # noqa: F401
    import app.models.oauth_state  # noqa: F401
    import app.models.project  # noqa: F401
    import app.models.proxy  # noqa: F401
    import app.models.rejected_hash  # noqa: F401
    import app.models.upload_task  # noqa: F401
    import app.models.uploaded_hash  # noqa: F401
    from app.api.deps import get_session
    from app.core.config import settings
    from app.main import app

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'folder.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with TestSession() as s:
        ch1, ch2 = make_channel(), make_channel()
        ch1.folder_path = "videos/ch1"
        ch2.folder_path = "videos/ch2"
        s.add(ch1)
        s.add(ch2)
        await s.commit()
        await s.refresh(ch1)
        await s.refresh(ch2)
        ids = (ch1.id, ch2.id)

        # A handful of tasks with distinct creation times for the limit test.
        for i in range(7):
            s.add(
                UploadTask(
                    channel_id=ch1.id,
                    file_path=f"video{i}.mp4",
                    status="completed",
                    created_at=datetime(2026, 1, 1, 0, i, tzinfo=timezone.utc),
                )
            )
        await s.commit()

    async def override_get_session():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    monkeypatch.setattr(routes.coordinator, "wake_scan", AsyncMock())

    headers = {"Authorization": f"Bearer {settings.api_token}"}

    class _Ctx:
        pass

    ctx = _Ctx()
    ctx.ids = ids
    ctx.headers = headers
    ctx.client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    ctx.TestSession = TestSession
    yield ctx
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_folder_switch_and_dedicated_reset(app_client):
    id1, id2 = app_client.ids
    client = app_client.client

    # Absent key: folder untouched.
    resp = await client.put(
        f"/channels/{id1}", headers=app_client.headers, json={"title": "x"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["folder_path"] == "videos/ch1"

    # Explicit id: switch to the referenced channel's folder.
    resp = await client.put(
        f"/channels/{id1}",
        headers=app_client.headers,
        json={"folder_from_channel_id": str(id2)},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["folder_path"] == "videos/ch2"

    # Explicit null: back to the dedicated folder under VIDEO_BASE_DIR.
    resp = await client.put(
        f"/channels/{id1}",
        headers=app_client.headers,
        json={"folder_from_channel_id": None},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["folder_path"].endswith(str(id1))

    # Unknown source channel still 404s.
    resp = await client.put(
        f"/channels/{id1}",
        headers=app_client.headers,
        json={"folder_from_channel_id": str(uuid4())},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_tasks_limit_returns_newest_first(app_client):
    client = app_client.client
    id1, _ = app_client.ids

    resp = await client.get(
        "/tasks", headers=app_client.headers, params={"channel_id": str(id1), "limit": 3}
    )
    assert resp.status_code == 200, resp.text
    tasks = resp.json()
    assert len(tasks) == 3
    created = [t["created_at"] for t in tasks]
    assert created == sorted(created, reverse=True), "newest first"
    assert all(t["file_path"] != "video0.mp4" for t in tasks), "oldest dropped"

    # No limit keeps full history (backward compatible).
    resp = await client.get(
        "/tasks", headers=app_client.headers, params={"channel_id": str(id1)}
    )
    assert len(resp.json()) == 7

    # Limit bounds are validated.
    resp = await client.get(
        "/tasks", headers=app_client.headers, params={"limit": 0}
    )
    assert resp.status_code == 422
    resp = await client.get(
        "/tasks", headers=app_client.headers, params={"limit": 1001}
    )
    assert resp.status_code == 422
