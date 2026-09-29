from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session
from app.core.config import settings
from app.core.migrations import upgrade_database
from app.main import app
from app.models.channel import Channel
from app.models.tag import ChannelTag
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject
from app.services.channel_service import ChannelService
from app.services.notification_service import notifications


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["new", "replace", "omit", "null", "clear", "unknown", "failed"])
async def test_tags_at_authorization_completion(client, monkeypatch, tmp_path, mode):
    from app.api.routes import channels as routes

    c, ids, sessions, _ = client
    old = (await c.post("/tags", json={"name": "Old"})).json()
    new = (await c.post("/tags", json={"name": "New"})).json()
    await c.put(f"/channels/{ids[0]}/tags/{old['id']}")
    async with sessions() as session:
        channel = await session.get(Channel, UUID(ids[0]))
        youtube_id = channel.youtube_channel_id if mode != "new" else "new-channel"
        project = GCPProject(name="Test", client_id="id", client_secret_enc="secret",
                             redirect_uri="http://localhost/callback")
        session.add(project)
        await session.commit()
        session.add(OAuthState(
            state="test-state", project_id=project.id,
            channel_id=channel.id if mode != "new" else None,
            code_verifier="verifier", redirect_uri=project.redirect_uri,
            channel_title="Test", expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        ))
        await session.commit()
    exchange = AsyncMock(return_value={"access_token": "a", "refresh_token": "r"})
    if mode == "failed":
        exchange.side_effect = RuntimeError("exchange failed")
    monkeypatch.setattr(routes, "exchange_code_for_tokens", exchange)
    monkeypatch.setattr(routes, "fetch_youtube_channel_info", AsyncMock(return_value={"id": youtube_id, "title": "Test"}))
    monkeypatch.setattr(routes.coordinator, "wake_channel", AsyncMock())
    monkeypatch.setattr(routes.coordinator, "start_channel_metadata", AsyncMock())
    monkeypatch.setattr(settings, "video_base_dir", str(tmp_path / "videos"))
    body = {"state": "test-state", "code": "code"}
    if mode != "omit":
        body["tag_ids"] = (None if mode == "null" else [] if mode == "clear"
                           else [str(uuid4())] if mode == "unknown" else [new["id"], new["id"]])
    response = await c.post("/channels/auth/callback", json=body)
    if mode in ("unknown", "failed"):
        assert response.status_code == (404 if mode == "unknown" else 400), response.text
        assert (await c.get(f"/channels/{ids[0]}")).json()["tags"] == [old]
        if mode == "unknown":
            exchange.assert_not_awaited()
        return
    assert response.status_code == 200, response.text
    expected = [old] if mode in ("omit", "null") else [] if mode == "clear" else [new]
    assert response.json()["tags"] == expected
    assert (await c.get(f"/channels/{response.json()['id']}")).json()["tags"] == expected
    connected = [call for call in notifications.broadcast.await_args_list if call.args[0] == "channel.connected"]
    assert connected[-1].args[1]["channel"]["tags"] == expected


@pytest.fixture
async def client(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tags.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    sessions = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async def get_test_session():
        async with sessions() as session:
            yield session
    app.dependency_overrides[get_session] = get_test_session
    monkeypatch.setattr(notifications, "broadcast", AsyncMock())
    async with sessions() as session:
        channels = [Channel(
            youtube_channel_id=str(uuid4()), title="Test", refresh_token_enc="r",
            access_token_enc="a", access_token_expires_at=datetime.now(timezone.utc),
            created_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        ) for _ in range(2)]
        session.add_all(channels)
        await session.commit()
        ids = [str(channel.id) for channel in channels]
    try:
        async with AsyncClient(transport=ASGITransport(app), base_url="http://test",
                               headers={"Authorization": f"Bearer {settings.api_token}"}) as c:
            yield c, ids, sessions, engine
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()


@pytest.mark.asyncio
async def test_shared_tags_persist_and_preserve_authorization_date(client):
    c, ids, sessions, _ = client
    before = (await c.get(f"/channels/{ids[0]}")).json()
    assert before["tags"] == []
    tag = (await c.post("/tags", json={"name": " Gaming "})).json()
    assert tag["name"] == "Gaming"
    assert (await c.post("/tags", json={"name": "Gaming"})).json() == tag
    for channel_id in ids:
        for _ in range(2):
            response = await c.put(f"/channels/{channel_id}/tags/{tag['id']}")
            assert response.status_code == 200, response.text
            assert response.json()["tags"] == [tag]
    async with sessions() as session:
        assert len((await session.exec(select(ChannelTag))).scalars().all()) == 2
        channel = await session.get(Channel, UUID(ids[0]))
        # Reauthorization updates credentials using the same service method.
        await ChannelService(session).update(channel, access_token_enc="new", session_status="active")
    listed = (await c.get("/channels")).json()
    assert all(item["tags"] == [tag] for item in listed)
    for _ in range(2):
        response = await c.delete(f"/channels/{ids[0]}/tags/{tag['id']}")
        assert response.status_code == 200
        assert response.json()["tags"] == []
        assert response.json()["created_at"] == before["created_at"]
    assert (await c.get(f"/channels/{ids[1]}")).json()["tags"] == [tag]
    assert (await c.get("/tags")).json() == [tag]
    assert notifications.broadcast.await_args.args[0] == "channel.updated"
    assert notifications.broadcast.await_args.args[1]["channel"]["tags"] == []
    async with sessions() as session:
        await ChannelService(session).delete(await session.get(Channel, UUID(ids[1])))
        assert (await session.exec(select(ChannelTag))).scalars().all() == []
    assert (await c.get("/tags")).json() == [tag]


@pytest.mark.asyncio
async def test_validation_and_missing_records(client):
    c, ids, _, _ = client
    for name in ["", "   ", "x" * 101]:
        assert (await c.post("/tags", json={"name": name})).status_code == 422
    tag = (await c.post("/tags", json={"name": "Test"})).json()
    for method in (c.put, c.delete):
        assert (await method(f"/channels/{uuid4()}/tags/{tag['id']}")).status_code == 404
        assert (await method(f"/channels/{ids[0]}/tags/{uuid4()}")).status_code == 404
        assert (await method(f"/channels/{ids[0]}/tags/bad")).status_code == 422
    assert (await c.get("/tags", headers={"Authorization": "Bearer invalid"})).status_code in (401, 403)


@pytest.mark.asyncio
async def test_existing_database_startup_creates_tag_tables(client):
    c, ids, _, engine = client
    before = (await c.get(f"/channels/{ids[0]}")).json()["created_at"]
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE channel_tag"))
        await conn.execute(text("DROP TABLE tag"))
        for _ in range(2):
            await conn.run_sync(upgrade_database)
            await conn.run_sync(SQLModel.metadata.create_all)
    response = await c.get(f"/channels/{ids[0]}")
    assert response.status_code == 200, response.text
    assert response.json()["tags"] == []
    assert response.json()["created_at"] == before
    assert (await c.post("/tags", json={"name": "Migrated"})).status_code == 200
