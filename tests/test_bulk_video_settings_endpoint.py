"""Tests for PUT /channels/bulk-video-settings (video metadata defaults)."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

import app.api.routes.channels as routes
from app.api.routes.channels import bulk_update_channel_video_settings
from app.models.channel import Channel
from app.schemas.channel import ChannelBulkVideoSettingsUpdate


def make_channel(**overrides) -> Channel:
    base = dict(
        youtube_channel_id=f"yt-{uuid4().hex[:8]}",
        title="Test channel",
        refresh_token_enc="r",
        access_token_enc="a",
        access_token_expires_at=datetime.now(timezone.utc),
        title_template="{filename}",
        description_template="",
        privacy_status="public",
        default_category_id="",
    )
    base.update(overrides)
    return Channel(**base)


class StubService:
    def __init__(self, channels):
        self.channels = {c.id: c for c in channels}
        self.update_calls = []

    async def get(self, cid):
        return self.channels.get(cid)

    async def update(self, channel, *, commit: bool = True, **kwargs):
        self.update_calls.append((channel.id, dict(kwargs), commit))
        for k, v in kwargs.items():
            setattr(channel, k, v)
        return channel


class DummySession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def refresh(self, channel):
        return None


@pytest.mark.asyncio
async def test_bulk_video_applies_settings_to_all_channels(monkeypatch):
    channels = [make_channel(), make_channel(), make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[c.id for c in channels],
        title_template="Video {filename}",
        description_template="Uploaded on {date}",
        privacy_status="unlisted",
        default_category_id="22",
    )
    result = await bulk_update_channel_video_settings(data, DummySession())

    assert result.not_found == []
    assert len(result.updated) == 3
    for c in channels:
        assert c.title == "Test channel"  # channel title is not a bulk field
        assert c.title_template == "Video {filename}"
        assert c.description_template == "Uploaded on {date}"
        assert c.privacy_status == "unlisted"
        assert c.default_category_id == "22"


@pytest.mark.asyncio
async def test_bulk_video_applies_only_provided_fields(monkeypatch):
    channels = [make_channel(privacy_status="private", title="Keep me")]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[c.id for c in channels],
        description_template="Uploaded on {date}",
    )
    result = await bulk_update_channel_video_settings(data, DummySession())

    assert stub.update_calls == [
        (channels[0].id, {"description_template": "Uploaded on {date}"}, False)
    ]
    assert channels[0].privacy_status == "private"  # untouched
    assert channels[0].title == "Keep me"  # untouched


@pytest.mark.asyncio
async def test_bulk_video_ignores_explicit_nulls(monkeypatch):
    channels = [make_channel(default_category_id="22")]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[c.id for c in channels],
        title_template=None,
        description_template=None,
        default_category_id=None,
    )
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_video_settings(data, DummySession())
    assert exc.value.status_code == 400
    assert stub.update_calls == []


@pytest.mark.asyncio
async def test_bulk_video_no_settings_returns_400(monkeypatch):
    channels = [make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(channel_ids=[c.id for c in channels])
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_video_settings(data, DummySession())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_bulk_video_reports_not_found_and_skips_deleted(monkeypatch):
    active = make_channel()
    deleted = make_channel(status="deleted")
    missing_id = uuid4()
    stub = StubService([active, deleted])
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[active.id, missing_id, deleted.id],
        privacy_status="private",
    )
    result = await bulk_update_channel_video_settings(data, DummySession())

    assert set(result.not_found) == {missing_id, deleted.id}
    assert len(result.updated) == 1
    assert active.privacy_status == "private"
    assert deleted.privacy_status == "public"


@pytest.mark.asyncio
async def test_bulk_video_all_not_found_returns_404(monkeypatch):
    stub = StubService([])
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(channel_ids=[uuid4()], privacy_status="private")
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_video_settings(data, DummySession())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_bulk_video_invalid_privacy_rejected_by_schema():
    with pytest.raises(ValueError):
        ChannelBulkVideoSettingsUpdate(
            channel_ids=[uuid4()],
            privacy_status="secret",
        )


@pytest.mark.asyncio
async def test_bulk_video_rejects_bad_description_template(monkeypatch):
    # A template with an unknown placeholder must fail validation before
    # anything is applied (all-or-nothing).
    channels = [make_channel(), make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[c.id for c in channels],
        description_template="{no_such_variable}",
    )
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_video_settings(data, DummySession())
    assert exc.value.status_code == 400
    assert stub.update_calls == []


@pytest.mark.asyncio
async def test_bulk_video_rejects_bad_title_template(monkeypatch):
    # A title template with an unknown placeholder must fail validation before
    # anything is applied (all-or-nothing).
    channels = [make_channel(), make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[c.id for c in channels],
        title_template="{no_such_variable}",
    )
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_video_settings(data, DummySession())
    assert exc.value.status_code == 400
    assert stub.update_calls == []


@pytest.mark.asyncio
async def test_bulk_video_deduplicates_channel_ids(monkeypatch):
    channels = [make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkVideoSettingsUpdate(
        channel_ids=[channels[0].id, channels[0].id],
        privacy_status="private",
    )
    result = await bulk_update_channel_video_settings(data, DummySession())

    assert len(result.updated) == 1
    assert len(stub.update_calls) == 1


@pytest.mark.asyncio
async def test_bulk_video_http_routing_and_persistence(tmp_path, monkeypatch):
    """End-to-end over ASGI: proves /bulk-video-settings is not captured by
    /{channel_id} and that changes are persisted to the database."""
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
    from app.models.channel import Channel

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'bulk.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with TestSession() as s:
        ch1, ch2 = make_channel(), make_channel()
        s.add(ch1)
        s.add(ch2)
        await s.commit()
        await s.refresh(ch1)
        await s.refresh(ch2)
        id1, id2 = ch1.id, ch2.id

    async def override_get_session():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    monkeypatch.setattr(routes.coordinator, "wake_scan", AsyncMock())

    headers = {"Authorization": f"Bearer {settings.api_token}"}
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.put(
                "/channels/bulk-video-settings",
                headers=headers,
                json={
                    "channel_ids": [str(id1), str(id2)],
                    "title_template": "Video {filename}",
                    "description_template": "Uploaded {date}",
                    "privacy_status": "unlisted",
                    "default_category_id": "22",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["not_found"] == []
    assert len(body["updated"]) == 2
    for ch in body["updated"]:
        assert ch["title"] == "Test channel"  # channel title is not a bulk field
        assert ch["title_template"] == "Video {filename}"
        assert ch["description_template"] == "Uploaded {date}"
        assert ch["privacy_status"] == "unlisted"
        assert ch["default_category_id"] == "22"

    # Verify persistence by reading the channels back from the DB.
    async with TestSession() as s:
        from sqlmodel import select

        rows = (await s.exec(select(Channel))).all()
        assert len(rows) == 2
        for c in rows:
            assert c.title == "Test channel"  # channel title is not a bulk field
            assert c.title_template == "Video {filename}"
            assert c.description_template == "Uploaded {date}"
            assert c.privacy_status == "unlisted"
            assert c.default_category_id == "22"

    await engine.dispose()
