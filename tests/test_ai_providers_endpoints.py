"""Tests for the /ai-providers endpoints (custom OpenAI-compatible providers)."""

import json
from uuid import uuid4

import pytest

from app.api.deps import get_session
from app.core.config import settings
from app.core.security import decrypt, encrypt
from app.main import app
from app.models.ai_model import (
    AICustomProvider,
    AICustomProviderApiKey,
    AIModelApiKey,
    AIModelConfig,
)


@pytest.fixture
async def client(tmp_path):
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_prov.db'}")
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


CREATE_BODY = {
    "name": "My OpenRouter",
    "base_url": "https://openrouter.ai/api/v1/",
    "models": ["gpt-4o", "gpt-4o-mini"],
    "api_keys": ["sk-AAAABBBBBCCCCCDDDD", "sk-EEEEFFFFFGGGGGHHHH"],
}


@pytest.mark.asyncio
async def test_create_provider(client):
    c, headers, _ = client
    resp = await c.post("/ai-providers", headers=headers, json=CREATE_BODY)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "My OpenRouter"
    # Trailing slash is normalized away.
    assert body["base_url"] == "https://openrouter.ai/api/v1"
    assert body["models"] == ["gpt-4o", "gpt-4o-mini"]
    assert [k["key"] for k in body["api_keys"]] == ["sk-A...DDDD", "sk-E...HHHH"]
    assert [k["status"] for k in body["api_keys"]] == ["unknown", "unknown"]
    assert body["is_active"] is True


@pytest.mark.asyncio
async def test_create_persists_encrypted_keys(client):
    c, headers, TestSession = client
    resp = await c.post("/ai-providers", headers=headers, json=CREATE_BODY)
    assert resp.status_code == 201
    async with TestSession() as s:
        from sqlmodel import select

        provider = (await s.exec(select(AICustomProvider))).one()
        key_rows = (await s.exec(select(AICustomProviderApiKey))).all()
        for row in key_rows:
            assert "sk-" not in row.key_enc
        keys = sorted(decrypt(row.key_enc) for row in key_rows)
        assert keys == sorted(CREATE_BODY["api_keys"])
        assert json.loads(provider.models_json) == CREATE_BODY["models"]
        assert all(row.provider_id == provider.id for row in key_rows)


@pytest.mark.asyncio
async def test_list_and_get_provider(client):
    c, headers, _ = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    other = dict(CREATE_BODY, name="Other", base_url="http://localhost:8000/v1", api_keys=["sk-KEYKEYKEYKEYKEYKEY01"])
    await c.post("/ai-providers", headers=headers, json=other)

    resp = await c.get("/ai-providers", headers=headers)
    assert resp.status_code == 200
    assert [p["name"] for p in resp.json()] == ["My OpenRouter", "Other"]

    resp = await c.get(f"/ai-providers/{created['id']}", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]


@pytest.mark.asyncio
async def test_get_missing_provider_returns_404(client):
    c, headers, _ = client
    resp = await c.get(f"/ai-providers/{uuid4()}", headers=headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_name_returns_409(client):
    c, headers, _ = client
    await c.post("/ai-providers", headers=headers, json=CREATE_BODY)
    duplicate = dict(CREATE_BODY, api_keys=["sk-KEYKEYKEYKEYKEYKEY02"])
    resp = await c.post("/ai-providers", headers=headers, json=duplicate)
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_duplicate_key_across_providers_returns_409(client):
    c, headers, _ = client
    await c.post("/ai-providers", headers=headers, json=CREATE_BODY)
    duplicate = dict(CREATE_BODY, name="Other")
    resp = await c.post("/ai-providers", headers=headers, json=duplicate)
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_duplicate_key_against_api_type_returns_409(client):
    """A key stored on a built-in API type cannot be reused on a provider."""
    c, headers, TestSession = client
    async with TestSession() as s:
        config = AIModelConfig(
            api_type="google_gemini", models_json='["gemini-2.5-pro"]'
        )
        s.add(config)
        await s.flush()
        s.add(AIModelApiKey(config_id=config.id, key_enc=encrypt("sk-SHAREDSHAREDSHARED")))
        await s.commit()

    body = dict(CREATE_BODY, api_keys=["sk-SHAREDSHAREDSHARED"])
    resp = await c.post("/ai-providers", headers=headers, json=body)
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_api_type_rejects_provider_key(client):
    """And vice versa: a provider's key cannot be saved on an API type."""
    c, headers, _ = client
    await c.post("/ai-providers", headers=headers, json=CREATE_BODY)
    resp = await c.put(
        "/ai-types/google_gemini",
        headers=headers,
        json={"api_keys": [CREATE_BODY["api_keys"][0]]},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_invalid_base_url_returns_422(client):
    c, headers, _ = client
    for bad in ("ftp://x", "not-a-url", ""):
        resp = await c.post(
            "/ai-providers",
            headers=headers,
            json=dict(CREATE_BODY, base_url=bad),
        )
        assert resp.status_code == 422, bad


@pytest.mark.asyncio
async def test_update_provider(client):
    c, headers, TestSession = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    resp = await c.put(
        f"/ai-providers/{created['id']}",
        headers=headers,
        json={
            "name": "Renamed",
            "base_url": "https://api.deepseek.com/v1",
            "models": ["deepseek-chat"],
            "api_keys": ["sk-NEWNEWNEWNEWNEWNEWNEW"],
            "is_active": False,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "Renamed"
    assert body["base_url"] == "https://api.deepseek.com/v1"
    assert body["models"] == ["deepseek-chat"]
    assert [k["key"] for k in body["api_keys"]] == ["sk-N...WNEW"]
    assert [k["status"] for k in body["api_keys"]] == ["unknown"]
    assert body["is_active"] is False

    async with TestSession() as s:
        from sqlmodel import select

        rows = (await s.exec(select(AICustomProviderApiKey))).all()
        assert len(rows) == 1  # old keys replaced


@pytest.mark.asyncio
async def test_update_partial_keeps_fields(client):
    c, headers, _ = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    resp = await c.put(
        f"/ai-providers/{created['id']}", headers=headers, json={"is_active": False}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_active"] is False
    assert body["name"] == CREATE_BODY["name"]
    assert body["base_url"] == "https://openrouter.ai/api/v1"
    assert len(body["api_keys"]) == 2


@pytest.mark.asyncio
async def test_update_replacing_keys_resets_status(client):
    c, headers, TestSession = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    async with TestSession() as s:
        from sqlmodel import select

        rows = (await s.exec(select(AICustomProviderApiKey))).all()
        for row in rows:
            row.status = "quota_exceeded"
        s.add_all(rows)
        await s.commit()

    resp = await c.put(
        f"/ai-providers/{created['id']}",
        headers=headers,
        json={"api_keys": ["sk-FRESHFRESHFRESHFRESH1"]},
    )
    assert resp.status_code == 200
    assert [k["status"] for k in resp.json()["api_keys"]] == ["unknown"]


@pytest.mark.asyncio
async def test_update_own_keys_again_is_allowed(client):
    c, headers, _ = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    resp = await c.put(
        f"/ai-providers/{created['id']}", headers=headers, json=CREATE_BODY
    )
    assert resp.status_code == 200
    assert len(resp.json()["api_keys"]) == 2


@pytest.mark.asyncio
async def test_delete_provider(client):
    c, headers, TestSession = client
    created = (await c.post("/ai-providers", headers=headers, json=CREATE_BODY)).json()
    resp = await c.delete(f"/ai-providers/{created['id']}", headers=headers)
    assert resp.status_code == 204
    resp = await c.get(f"/ai-providers/{created['id']}", headers=headers)
    assert resp.status_code == 404

    async with TestSession() as s:
        from sqlmodel import select

        assert (await s.exec(select(AICustomProvider))).all() == []
        assert (await s.exec(select(AICustomProviderApiKey))).all() == []


@pytest.mark.asyncio
async def test_delete_missing_returns_404(client):
    c, headers, _ = client
    resp = await c.delete(f"/ai-providers/{uuid4()}", headers=headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_rejects_empty_lists(client):
    c, headers, _ = client
    resp = await c.post(
        "/ai-providers", headers=headers, json=dict(CREATE_BODY, models=[])
    )
    assert resp.status_code == 422
    resp = await c.post(
        "/ai-providers", headers=headers, json=dict(CREATE_BODY, api_keys=["  "])
    )
    assert resp.status_code == 422
