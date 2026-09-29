"""Tests for AI prompt storage endpoints (GET/PUT /ai-prompts)."""

import pytest

from app.api.deps import get_session
from app.core.config import settings
from app.main import app
from app.models.ai_model import AIPrompt


@pytest.fixture
async def client(tmp_path):
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_prompts.db'}")
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


@pytest.mark.asyncio
async def test_get_prompts_returns_defaults(client):
    c, headers, _ = client
    resp = await c.get("/ai-prompts", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["title_prompt"]  # non-empty default
    assert body["description_prompt"]
    assert body["title_prompt"] != body["description_prompt"]


@pytest.mark.asyncio
async def test_update_prompts(client):
    c, headers, TestSession = client
    resp = await c.put(
        "/ai-prompts",
        headers=headers,
        json={
            "title_prompt": "Generate a catchy title about cats.",
            "description_prompt": "Describe the video with timestamps.",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["title_prompt"] == "Generate a catchy title about cats."
    assert body["description_prompt"] == "Describe the video with timestamps."

    # Persisted as two separate prompt rows.
    async with TestSession() as s:
        from sqlmodel import select

        rows = {row.prompt_type: row.content for row in (await s.exec(select(AIPrompt))).all()}
        assert rows == {
            "title": "Generate a catchy title about cats.",
            "description": "Describe the video with timestamps.",
        }


@pytest.mark.asyncio
async def test_update_single_prompt_keeps_other(client):
    c, headers, _ = client
    await c.put(
        "/ai-prompts",
        headers=headers,
        json={"title_prompt": "T1", "description_prompt": "D1"},
    )
    resp = await c.put(
        "/ai-prompts", headers=headers, json={"title_prompt": "T2"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["title_prompt"] == "T2"
    assert body["description_prompt"] == "D1"


@pytest.mark.asyncio
async def test_update_empty_prompt_rejected(client):
    c, headers, _ = client
    resp = await c.put(
        "/ai-prompts", headers=headers, json={"title_prompt": "   "}
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_prompt_length_limit(client):
    c, headers, _ = client
    resp = await c.put(
        "/ai-prompts", headers=headers, json={"title_prompt": "a" * 20_001}
    )
    assert resp.status_code == 422
