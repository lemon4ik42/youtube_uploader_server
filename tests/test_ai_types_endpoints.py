"""Tests for the /ai-types endpoints (per-API-type settings)."""

import json
from uuid import uuid4

import pytest

import app.api.routes.ai_types as routes
from app.api.deps import get_session
from app.core.config import settings
from app.core.security import decrypt
from app.main import app
from app.models.ai_model import AIModelApiKey, AIModelConfig


@pytest.fixture
async def client(tmp_path, monkeypatch):
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_types.db'}")
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


SAVE_BODY = {
    "models": ["gemini-2.5-pro", "gemini-2.5-flash"],
    "api_keys": ["AIzaSyAAAABBBBBCCCCCDDDD", "AIzaSyEEEEFFFFFGGGGGHHHH"],
}


@pytest.mark.asyncio
async def test_list_types_unconfigured(client):
    c, headers, _ = client
    resp = await c.get("/ai-types", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert [t["api_type"] for t in body] == ["google_gemini"]
    assert body[0]["models"] == []
    assert body[0]["api_keys"] == []
    assert body[0]["is_active"] is False


@pytest.mark.asyncio
async def test_save_type_settings(client):
    c, headers, _ = client
    resp = await c.put(
        "/ai-types/google_gemini", headers=headers, json=SAVE_BODY
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["api_type"] == "google_gemini"
    assert body["models"] == ["gemini-2.5-pro", "gemini-2.5-flash"]
    # API keys are never returned in full; each key carries its status.
    assert [k["key"] for k in body["api_keys"]] == ["AIza...DDDD", "AIza...HHHH"]
    assert [k["status"] for k in body["api_keys"]] == ["unknown", "unknown"]
    assert all(k["last_checked_at"] is None for k in body["api_keys"])
    assert body["is_active"] is True


@pytest.mark.asyncio
async def test_save_persists_encrypted_keys(client):
    c, headers, TestSession = client
    resp = await c.put(
        "/ai-types/google_gemini", headers=headers, json=SAVE_BODY
    )
    assert resp.status_code == 200
    async with TestSession() as s:
        from sqlmodel import select

        config = (await s.exec(select(AIModelConfig))).one()
        assert config.api_type == "google_gemini"
        key_rows = (await s.exec(select(AIModelApiKey))).all()
        # Raw key material must not be stored in plaintext.
        for row in key_rows:
            assert "AIzaSy" not in row.key_enc
        keys = sorted(decrypt(row.key_enc) for row in key_rows)
        assert keys == sorted(SAVE_BODY["api_keys"])
        assert json.loads(config.models_json) == SAVE_BODY["models"]


@pytest.mark.asyncio
async def test_get_type(client):
    c, headers, _ = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    resp = await c.get("/ai-types/google_gemini", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["api_type"] == "google_gemini"
    assert len(body["api_keys"]) == 2


@pytest.mark.asyncio
async def test_get_unsupported_type_returns_404(client):
    c, headers, _ = client
    resp = await c.get("/ai-types/openai_gpt", headers=headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_put_unsupported_type_returns_400(client):
    c, headers, _ = client
    resp = await c.put(
        "/ai-types/openai_gpt", headers=headers, json=SAVE_BODY
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_put_replaces_lists(client):
    c, headers, TestSession = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    resp = await c.put(
        "/ai-types/google_gemini",
        headers=headers,
        json={
            "models": ["gemini-2.5-flash"],
            "api_keys": ["AIzaSyNEWNEWNEWNEWNEWNEW"],
            "is_active": False,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["models"] == ["gemini-2.5-flash"]
    assert [k["key"] for k in body["api_keys"]] == ["AIza...WNEW"]
    assert [k["status"] for k in body["api_keys"]] == ["unknown"]
    assert body["is_active"] is False

    async with TestSession() as s:
        from sqlmodel import select

        key_rows = (await s.exec(select(AIModelApiKey))).all()
        # Old key rows were replaced by the new list.
        assert len(key_rows) == 1
        assert decrypt(key_rows[0].key_enc) == "AIzaSyNEWNEWNEWNEWNEWNEW"


@pytest.mark.asyncio
async def test_put_partial_keeps_other_list(client):
    c, headers, _ = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    resp = await c.put(
        "/ai-types/google_gemini", headers=headers, json={"is_active": False}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_active"] is False
    assert body["models"] == SAVE_BODY["models"]
    assert len(body["api_keys"]) == 2


@pytest.mark.asyncio
async def test_put_replacing_keys_resets_status(client):
    c, headers, TestSession = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    # Simulate live status updates on the stored keys.
    async with TestSession() as s:
        from sqlmodel import select

        rows = (await s.exec(select(AIModelApiKey))).all()
        for row in rows:
            row.status = "quota_exceeded"
        s.add_all(rows)
        await s.commit()

    resp = await c.put(
        "/ai-types/google_gemini",
        headers=headers,
        json={"api_keys": ["AIzaSyFRESHFRESHFRESHFRESH1"]},
    )
    assert resp.status_code == 200
    statuses = [k["status"] for k in resp.json()["api_keys"]]
    # New key material starts as unknown regardless of the old statuses.
    assert statuses == ["unknown"]


@pytest.mark.asyncio
async def test_put_same_keys_again_is_allowed(client):
    c, headers, _ = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    # Re-submitting the same keys for the same type must not trip the
    # duplicate check (it excludes the type's own rows).
    resp = await c.put(
        "/ai-types/google_gemini", headers=headers, json=SAVE_BODY
    )
    assert resp.status_code == 200
    assert len(resp.json()["api_keys"]) == 2


@pytest.mark.asyncio
async def test_put_rejects_empty_lists(client):
    c, headers, _ = client
    resp = await c.put(
        "/ai-types/google_gemini", headers=headers, json={"models": []}
    )
    assert resp.status_code == 422
    resp = await c.put(
        "/ai-types/google_gemini",
        headers=headers,
        json={"api_keys": ["  "]},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_delete_type(client):
    c, headers, TestSession = client
    await c.put("/ai-types/google_gemini", headers=headers, json=SAVE_BODY)
    resp = await c.delete("/ai-types/google_gemini", headers=headers)
    assert resp.status_code == 204
    resp = await c.get("/ai-types/google_gemini", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["api_keys"] == []
    assert resp.json()["models"] == []

    async with TestSession() as s:
        from sqlmodel import select

        assert (await s.exec(select(AIModelConfig))).all() == []
        # Key rows are removed together with the settings.
        assert (await s.exec(select(AIModelApiKey))).all() == []


@pytest.mark.asyncio
async def test_delete_unconfigured_returns_404(client):
    c, headers, _ = client
    resp = await c.delete("/ai-types/google_gemini", headers=headers)
    assert resp.status_code == 404
    resp = await c.delete("/ai-types/openai_gpt", headers=headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_mask_api_key_short():
    assert routes.mask_api_key("short") == "****"
    assert routes.mask_api_key("12345678") == "****"
    assert routes.mask_api_key("123456789") == "1234...6789"
