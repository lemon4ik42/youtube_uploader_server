"""PUT /channels/{channel_id} must stay fast while videos are uploading.

The upload loop resolves the channel's proxy URL before every 5 MiB chunk
(``upload_file_to_session`` → ``get_current_channel_proxy_url``). That helper
used to open a brand-new database session per call: with a full batch of 200
concurrent uploads it opened hundreds of connections per second for the whole
batch, and the resulting SQLite lock traffic starved unrelated endpoints —
users measured channel-settings saves taking from seconds up to minutes while
videos were uploading.

The helper now serves the resolved URL from a short-lived cache (invalidated
explicitly whenever the proxy assignment or recovery state changes). This test
reproduces the storm against a real SQLite database (WAL, busy timeout, same
engine settings as production) and asserts the save completes quickly.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.services.channel_service as cs_mod
import app.services.proxy_service as ps
from app.core import security
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.proxy import Proxy
from app.main import app  # noqa: E402


FERNET = security._fernet


def _enc(value: str) -> str:
    return FERNET.encrypt(value.encode()).decode()


@pytest.fixture
def sqlite_env(monkeypatch, tmp_path):
    """Point the shared engine at a throwaway SQLite file and create the tables."""
    db_path = tmp_path / "test.db"
    url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setattr(Settings, "database_url", url, raising=False)

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool
    from sqlalchemy.orm import sessionmaker
    from sqlmodel.ext.asyncio.session import AsyncSession
    import app.core.db as db

    engine = create_async_engine(url, echo=False, future=True, connect_args={"timeout": 30}, poolclass=NullPool)

    @event.listens_for(engine.sync_engine, "connect")
    def _configure(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA cache_size=-2000")
            cursor.execute("PRAGMA temp_store=FILE")
            cursor.execute("PRAGMA wal_autocheckpoint=256")
        finally:
            cursor.close()

    test_session_maker = sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )

    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "async_session_maker", test_session_maker)

    # Modules that bound the session maker at import time (``from ... import``)
    # hold their own reference and must be pointed at the test database too.
    import app.api.deps as deps
    monkeypatch.setattr(deps, "async_session_maker", test_session_maker)
    monkeypatch.setattr(ps, "async_session_maker", test_session_maker)

    # The per-channel proxy cache is module state; keep tests independent.
    monkeypatch.setattr(cs_mod, "_proxy_url_cache", {})

    async def init_tables():
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    return engine, test_session_maker, init_tables


@pytest.mark.asyncio
async def test_channel_save_is_fast_during_upload_resolver_storm(
    sqlite_env, monkeypatch, tmp_path
):
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    async with session_maker() as session:
        project = GCPProject(
            name="p",
            client_id="cid",
            client_secret_enc=_enc("cs"),
            auth_uri="https://example.com/auth",
            token_uri="https://example.com/token",
            redirect_uri="https://example.com/redirect",
            scope="scope",
            api_upload="https://example.com/upload",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)

        proxy = Proxy(url_enc=_enc("socks5://u:p@10.0.0.1:1080"), is_active=True)
        session.add(proxy)
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            project_id=project.id,
            refresh_token_enc="r",
            access_token_enc="a",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
            proxy_id=proxy.id,
        )
        session.add(channel)
        await session.commit()
        await session.refresh(channel)
        channel_id = channel.id
        proxy_url = "socks5://u:p@10.0.0.1:1080"

    stop = asyncio.Event()

    async def fake_upload():
        """One uploading video: resolve the proxy before every 5 MiB chunk."""
        while not stop.is_set():
            await cs_mod.get_current_channel_proxy_url(channel_id)
            await ps.record_proxy_url_usage(proxy_url, True)
            await asyncio.sleep(0.02)  # network time per chunk

    # A full batch is up to 200 concurrent uploads.
    uploads = [asyncio.create_task(fake_upload()) for _ in range(200)]
    try:
        await asyncio.sleep(1.0)  # let the storm reach full concurrency

        token = "test-token"
        monkeypatch.setattr(
            "app.api.deps.settings",
            Settings(
                base_dir=str(tmp_path),
                app_secret_key="test-secret",
                api_token=token,
            ),
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            started = time.perf_counter()
            response = await client.put(
                f"/channels/{channel_id}",
                json={"max_batch_size": 12, "inter_upload_delay_seconds": 3},
            )
            elapsed = time.perf_counter() - started

        assert response.status_code == 200, response.text
        assert response.json()["max_batch_size"] == 12
        assert elapsed < 1.5, f"channel save took {elapsed:.2f}s during uploads"
    finally:
        stop.set()
        for task in uploads:
            task.cancel()
        await asyncio.gather(*uploads, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
async def test_proxy_url_cache_hits_database_once_within_ttl(
    sqlite_env, monkeypatch
):
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    async with session_maker() as session:
        proxy = Proxy(url_enc=_enc("socks5://u:p@10.0.0.9:1080"), is_active=True)
        session.add(proxy)
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            refresh_token_enc="r",
            access_token_enc="a",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
            proxy_id=proxy.id,
        )
        session.add(channel)
        await session.commit()
        await session.refresh(channel)
        channel_id = channel.id

    import app.core.db as db_mod

    real_maker = db_mod.async_session_maker
    sessions_opened = []

    def counting_maker():
        sessions_opened.append(1)
        return real_maker()

    monkeypatch.setattr(db_mod, "async_session_maker", counting_maker)

    first = await cs_mod.get_current_channel_proxy_url(channel_id)
    assert first == "socks5://u:p@10.0.0.9:1080"
    for _ in range(50):
        assert (
            await cs_mod.get_current_channel_proxy_url(channel_id) == first
        )
    assert len(sessions_opened) == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_proxy_url_cache_expires_and_rereads(sqlite_env, monkeypatch):
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    async with session_maker() as session:
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            refresh_token_enc="r",
            access_token_enc="a",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
        )
        session.add(channel)
        await session.commit()
        await session.refresh(channel)
        channel_id = channel.id

    monkeypatch.setattr(cs_mod, "_PROXY_URL_CACHE_TTL_SECONDS", 0.05)

    assert await cs_mod.get_current_channel_proxy_url(channel_id) is None
    # A proxy assigned after the cached answer still appears once the TTL
    # expires: the cache must never pin a stale route forever.
    async with session_maker() as session:
        proxy = Proxy(url_enc=_enc("socks5://u:p@10.0.0.7:1080"), is_active=True)
        session.add(proxy)
        await session.commit()
        await session.refresh(proxy)
        channel = await session.get(Channel, channel_id)
        channel.proxy_id = proxy.id
        await session.commit()

    await asyncio.sleep(0.06)
    assert (
        await cs_mod.get_current_channel_proxy_url(channel_id)
        == "socks5://u:p@10.0.0.7:1080"
    )
    await engine.dispose()


@pytest.mark.asyncio
async def test_proxy_url_cache_invalidated_on_proxy_change(sqlite_env):
    """Saving a new proxy takes effect on the very next resolver call."""
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    async with session_maker() as session:
        proxy_a = Proxy(url_enc=_enc("socks5://u:p@10.0.0.1:1080"), is_active=True)
        proxy_b = Proxy(url_enc=_enc("socks5://u:p@10.0.0.2:1080"), is_active=True)
        session.add(proxy_a)
        session.add(proxy_b)
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            refresh_token_enc="r",
            access_token_enc="a",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
            proxy_id=proxy_a.id,
        )
        session.add(channel)
        await session.commit()
        await session.refresh(proxy_a)
        await session.refresh(proxy_b)
        await session.refresh(channel)
        channel_id = channel.id
        proxy_b_id = proxy_b.id

    assert (
        await cs_mod.get_current_channel_proxy_url(channel_id)
        == "socks5://u:p@10.0.0.1:1080"
    )

    # The PUT /channels/{id} funnel: ChannelService.update with a new proxy.
    async with session_maker() as session:
        channel = await session.get(Channel, channel_id)
        await cs_mod.ChannelService(session).update(channel, proxy_id=proxy_b_id)

    assert (
        await cs_mod.get_current_channel_proxy_url(channel_id)
        == "socks5://u:p@10.0.0.2:1080"
    )
    await engine.dispose()


@pytest.mark.asyncio
async def test_proxy_url_cache_invalidated_by_recovery_scheduling(
    sqlite_env, monkeypatch
):
    """Scheduling recovery drops cached routes so uploads stop using them."""
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    async with session_maker() as session:
        proxy = Proxy(url_enc=_enc("socks5://u:p@10.0.0.3:1080"), is_active=True)
        session.add(proxy)
        channel = Channel(
            youtube_channel_id=f"yt-{uuid4().hex[:8]}",
            title="ch",
            refresh_token_enc="r",
            access_token_enc="a",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            session_status="active",
            proxy_id=proxy.id,
            proxy_auto_switch=True,
        )
        session.add(channel)
        await session.commit()
        await session.refresh(proxy)
        await session.refresh(channel)
        channel_id = channel.id
        proxy_id = proxy.id

    assert (
        await cs_mod.get_current_channel_proxy_url(channel_id)
        == "socks5://u:p@10.0.0.3:1080"
    )

    # Recovery scheduling spawns a loop; keep this test focused on the cache.
    async def noop_start(channel_id):
        return None

    monkeypatch.setattr(ps, "start_channel_proxy_recovery", noop_start)

    await ps.start_proxy_recovery_for_proxy(proxy_id)

    # Recovery pending: the resolver must not serve the cached dead route.
    # It polls once per second, so the call returns only once recovery is
    # observed; here recovery never resolves, so give it a bounded window.
    try:
        await asyncio.wait_for(
            cs_mod.get_current_channel_proxy_url(channel_id), timeout=0.5
        )
        raise AssertionError("resolver must wait while recovery is pending")
    except asyncio.TimeoutError:
        pass  # still waiting: the cached URL was not served

    # Once recovery is cleared, the route resolves again (fresh read).
    async with session_maker() as session:
        channel = await session.get(Channel, channel_id)
        channel.proxy_recovery_pending = False
        await session.commit()

    assert (
        await cs_mod.get_current_channel_proxy_url(channel_id)
        == "socks5://u:p@10.0.0.3:1080"
    )
    await engine.dispose()
