"""Tests for channel AI model settings (PUT /channels/{id} ai fields)."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.api.deps import get_session
from app.core.config import settings
from app.main import app
from app.models.channel import Channel


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
async def client(tmp_path):
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_channel.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_session():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    headers = {"Authorization": f"Bearer {settings.api_token}"}
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c, headers, TestSession
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()


async def _create_channel(TestSession) -> Channel:
    async with TestSession() as s:
        channel = make_channel()
        s.add(channel)
        await s.commit()
        await s.refresh(channel)
        return channel


@pytest.mark.asyncio
async def test_channel_defaults(client):
    _, _, TestSession = client
    channel = await _create_channel(TestSession)
    async with TestSession() as s:
        stored = await s.get(Channel, channel.id)
        assert stored.ai_model is None
        assert stored.ai_generate_description is False
        assert stored.ai_use_cached is False


@pytest.mark.asyncio
async def test_set_ai_model_auto(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}",
        headers=headers,
        json={"ai_model": "auto", "ai_generate_description": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ai_model"] == "auto"
    assert body["ai_generate_description"] is True
    assert body["ai_use_cached"] is False

    async with TestSession() as s:
        stored = await s.get(Channel, channel.id)
        assert stored.ai_model == "auto"
        assert stored.ai_generate_description is True
        assert stored.ai_use_cached is False


@pytest.mark.asyncio
async def test_set_specific_ai_type(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    # A supported type is accepted even before any settings are saved for
    # it; generation then falls back to the templates.
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": "google_gemini"}
    )
    assert resp.status_code == 200
    assert resp.json()["ai_model"] == "google_gemini"

    async with TestSession() as s:
        stored = await s.get(Channel, channel.id)
        assert stored.ai_model == "google_gemini"


@pytest.mark.asyncio
async def test_unsupported_ai_type_returns_422(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": "openai_gpt"}
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_invalid_ai_model_value_returns_422(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": "not-a-type"}
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_disable_ai_model_with_null(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    await c.put(
        f"/channels/{channel.id}",
        headers=headers,
        json={"ai_model": "google_gemini", "ai_generate_description": True},
    )
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": None}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ai_model"] is None
    # Unrelated flags keep their values.
    assert body["ai_generate_description"] is True


@pytest.mark.asyncio
async def test_set_ai_use_cached_flag(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}",
        headers=headers,
        json={"ai_model": "auto", "ai_use_cached": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ai_use_cached"] is True

    async with TestSession() as s:
        stored = await s.get(Channel, channel.id)
        assert stored.ai_use_cached is True

    # And back to false.
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_use_cached": False}
    )
    assert resp.status_code == 200
    assert resp.json()["ai_use_cached"] is False


async def _create_custom_provider(TestSession, name="My provider") -> str:
    """Create a custom provider row; returns its ``custom:<id>`` reference."""
    from app.models.ai_model import AICustomProvider

    async with TestSession() as s:
        provider = AICustomProvider(
            name=name,
            base_url="https://api.example.com/v1",
            models_json='["gpt-4o"]',
        )
        s.add(provider)
        await s.commit()
        await s.refresh(provider)
        return f"custom:{provider.id}"


@pytest.mark.asyncio
async def test_set_custom_provider(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    ref = await _create_custom_provider(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": ref}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["ai_model"] == ref

    async with TestSession() as s:
        stored = await s.get(Channel, channel.id)
        assert stored.ai_model == ref


@pytest.mark.asyncio
async def test_custom_provider_missing_returns_400(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}",
        headers=headers,
        json={"ai_model": f"custom:{uuid4()}"},
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_custom_provider_invalid_id_returns_422(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        f"/channels/{channel.id}", headers=headers, json={"ai_model": "custom:not-a-uuid"}
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_bulk_custom_provider(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    ref = await _create_custom_provider(TestSession)
    resp = await c.put(
        "/channels/bulk-settings",
        headers=headers,
        json={"channel_ids": [str(channel.id)], "ai_model": ref},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["updated"][0]["ai_model"] == ref


@pytest.mark.asyncio
async def test_bulk_custom_provider_missing_returns_400(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.put(
        "/channels/bulk-settings",
        headers=headers,
        json={"channel_ids": [str(channel.id)], "ai_model": f"custom:{uuid4()}"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_channel_exposes_ai_fields(client):
    c, headers, TestSession = client
    channel = await _create_channel(TestSession)
    resp = await c.get(f"/channels/{channel.id}", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "ai_model" in body and body["ai_model"] is None
    assert "ai_generate_description" in body and body["ai_generate_description"] is False
    assert "ai_use_cached" in body and body["ai_use_cached"] is False
