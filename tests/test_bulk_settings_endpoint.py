"""Tests for PUT /channels/bulk-settings (apply settings to many channels)."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

import app.api.routes.channels as routes
from app.api.routes.channels import bulk_update_channel_settings
from app.models.channel import Channel
from app.schemas.channel import ChannelBulkSettingsUpdate


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


@pytest.fixture
def patch_coordinator(monkeypatch):
    monkeypatch.setattr(routes.coordinator, "wake_scan", AsyncMock())


@pytest.mark.asyncio
async def test_bulk_applies_settings_to_all_channels(patch_coordinator, monkeypatch):
    channels = [make_channel(), make_channel(), make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        max_batch_size=10,
        min_batch_size=2,
        inter_upload_delay_seconds=7,
        shuffle_upload_order=True,
        upload_limit_enabled=True,
        upload_limit_count=30,
        upload_limit_period_hours=12,
        proxy_auto_switch=True,
        skip_blocked_videos=True,
    )
    result = await bulk_update_channel_settings(data, DummySession())

    assert result.not_found == []
    assert len(result.updated) == 3
    for c in channels:
        assert c.max_batch_size == 10
        assert c.min_batch_size == 2
        assert c.inter_upload_delay_seconds == 7
        assert c.shuffle_upload_order is True
        assert c.upload_limit_enabled is True
        assert c.upload_limit_count == 30
        assert c.upload_limit_period_hours == 12
        assert c.proxy_auto_switch is True
        assert c.skip_blocked_videos is True


@pytest.mark.asyncio
async def test_bulk_reports_not_found(patch_coordinator, monkeypatch):
    existing = make_channel()
    missing_id = uuid4()
    stub = StubService([existing])
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[existing.id, missing_id],
        max_batch_size=5,
    )
    result = await bulk_update_channel_settings(data, DummySession())

    assert result.not_found == [missing_id]
    assert len(result.updated) == 1
    assert existing.max_batch_size == 5


@pytest.mark.asyncio
async def test_bulk_skips_deleted_channels(patch_coordinator, monkeypatch):
    active = make_channel()
    deleted = make_channel(status="deleted")
    stub = StubService([active, deleted])
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[active.id, deleted.id],
        shuffle_upload_order=True,
    )
    result = await bulk_update_channel_settings(data, DummySession())

    assert result.not_found == [deleted.id]
    assert len(result.updated) == 1
    assert active.shuffle_upload_order is True
    assert deleted.shuffle_upload_order is False


@pytest.mark.asyncio
async def test_bulk_rejects_min_greater_than_max(patch_coordinator, monkeypatch):
    channels = [make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        min_batch_size=50,
        max_batch_size=10,
    )
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_settings(data, DummySession())
    assert exc.value.status_code == 400
    # Nothing applied on validation failure.
    assert channels[0].min_batch_size == 1
    assert channels[0].max_batch_size == 200
    assert stub.update_calls == []


@pytest.mark.asyncio
async def test_bulk_min_validated_against_existing_max(patch_coordinator, monkeypatch):
    # Channel has a small existing max; a larger incoming min must be rejected.
    channels = [make_channel(max_batch_size=5)]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        min_batch_size=10,
    )
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_settings(data, DummySession())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_bulk_no_settings_returns_400(patch_coordinator, monkeypatch):
    channels = [make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(channel_ids=[c.id for c in channels])
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_settings(data, DummySession())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_bulk_all_not_found_returns_404(patch_coordinator, monkeypatch):
    stub = StubService([])
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(channel_ids=[uuid4()], max_batch_size=5)
    with pytest.raises(HTTPException) as exc:
        await bulk_update_channel_settings(data, DummySession())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_bulk_applies_ai_settings(patch_coordinator, monkeypatch):
    channels = [make_channel(), make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        ai_model="google_gemini",
        ai_generate_description=True,
        ai_use_cached=True,
    )
    result = await bulk_update_channel_settings(data, DummySession())

    assert result.not_found == []
    assert len(result.updated) == 2
    for c in channels:
        assert c.ai_model == "google_gemini"
        assert c.ai_generate_description is True
        assert c.ai_use_cached is True


@pytest.mark.asyncio
async def test_bulk_ai_model_null_disables_ai(patch_coordinator, monkeypatch):
    channels = [make_channel(ai_model="google_gemini", ai_generate_description=True)]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        ai_model=None,
    )
    await bulk_update_channel_settings(data, DummySession())

    # An explicit null disables AI generation instead of being ignored.
    assert channels[0].ai_model is None
    # Unrelated AI flags keep their values.
    assert channels[0].ai_generate_description is True
    assert stub.update_calls == [
        (channels[0].id, {"ai_model": None}, False)
    ]


@pytest.mark.asyncio
async def test_bulk_invalid_ai_model_returns_422():
    with pytest.raises(Exception) as exc:
        ChannelBulkSettingsUpdate(
            channel_ids=[uuid4()],
            ai_model="not-a-type",
        )
    assert "ai_model" in str(exc.value)


@pytest.mark.asyncio
async def test_bulk_applies_only_provided_fields(patch_coordinator, monkeypatch):
    channels = [make_channel(max_batch_size=200, shuffle_upload_order=False)]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[c.id for c in channels],
        shuffle_upload_order=True,
    )
    await bulk_update_channel_settings(data, DummySession())

    # Only shuffle_upload_order should have been passed to update(), with the
    # transaction left to the endpoint's single commit.
    assert stub.update_calls == [
        (channels[0].id, {"shuffle_upload_order": True}, False)
    ]
    assert channels[0].shuffle_upload_order is True
    assert channels[0].max_batch_size == 200  # untouched


@pytest.mark.asyncio
async def test_bulk_deduplicates_channel_ids(patch_coordinator, monkeypatch):
    channels = [make_channel()]
    stub = StubService(channels)
    monkeypatch.setattr(routes, "ChannelService", lambda session: stub)

    data = ChannelBulkSettingsUpdate(
        channel_ids=[channels[0].id, channels[0].id, channels[0].id],
        max_batch_size=9,
    )
    result = await bulk_update_channel_settings(data, DummySession())

    assert len(result.updated) == 1
    assert len(stub.update_calls) == 1
    assert channels[0].max_batch_size == 9


@pytest.mark.asyncio
async def test_bulk_settings_http_routing_and_persistence(tmp_path, monkeypatch):
    """End-to-end over ASGI: proves /bulk-settings is not captured by /{channel_id}
    and that changes are persisted to the database."""
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
                "/channels/bulk-settings",
                headers=headers,
                json={
                    "channel_ids": [str(id1), str(id2)],
                    "max_batch_size": 42,
                    "shuffle_upload_order": True,
                    "proxy_auto_switch": True,
                    "upload_limit_enabled": True,
                    "upload_limit_count": 33,
                    "upload_limit_period_hours": 6,
                    "ai_model": "google_gemini",
                    "ai_generate_description": True,
                    "ai_use_cached": True,
                    "skip_blocked_videos": True,
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["not_found"] == []
    assert len(body["updated"]) == 2
    for ch in body["updated"]:
        assert ch["max_batch_size"] == 42
        assert ch["shuffle_upload_order"] is True
        assert ch["proxy_auto_switch"] is True
        assert ch["upload_limit_enabled"] is True
        assert ch["upload_limit_count"] == 33
        assert ch["upload_limit_period_hours"] == 6
        assert ch["ai_model"] == "google_gemini"
        assert ch["ai_generate_description"] is True
        assert ch["ai_use_cached"] is True
        assert ch["skip_blocked_videos"] is True

    # Verify persistence by reading the channels back from the DB.
    async with TestSession() as s:
        from sqlmodel import select

        rows = (await s.exec(select(Channel))).all()
        assert len(rows) == 2
        for c in rows:
            assert c.max_batch_size == 42
            assert c.shuffle_upload_order is True
            assert c.proxy_auto_switch is True
            assert c.upload_limit_enabled is True
            assert c.upload_limit_count == 33
            assert c.ai_model == "google_gemini"
            assert c.ai_generate_description is True
            assert c.ai_use_cached is True
            assert c.skip_blocked_videos is True

    await engine.dispose()
