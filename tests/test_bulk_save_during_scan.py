"""PUT /channels/bulk-settings must stay fast while a proxy scan is writing.

A "check all" over hundreds of proxies writes its results in per-wave
transactions, and every write queues on SQLite's single writer. The bulk save
used to commit once per channel, so with 15 channels it waited behind 15
separate transactions per wave — users measured ~15-second saves. This test
reproduces that contention against a real SQLite database (WAL, busy timeout,
same engine settings as production) and asserts the endpoint completes quickly
while a scan-shaped writer keeps committing in the background.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select

import app.services.proxy_service as ps
from app.core.config import Settings
from app.core.db import async_session_maker
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.proxy import Proxy
from app.core import security
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

    # Rebuild the engine and session maker against the test database.
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool
    from sqlalchemy.orm import sessionmaker
    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlmodel import SQLModel
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
    import app.services.proxy_service as ps
    monkeypatch.setattr(deps, "async_session_maker", test_session_maker)
    monkeypatch.setattr(ps, "async_session_maker", test_session_maker)

    async def init_tables():
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    return engine, test_session_maker, init_tables


@pytest.mark.asyncio
async def test_bulk_save_is_fast_during_proxy_scan_writes(
    sqlite_env, monkeypatch, tmp_path
):
    engine, session_maker, init_tables = sqlite_env
    await init_tables()

    # Seed one project, 15 channels and 300 proxies.
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

        channels = []
        for _ in range(15):
            channel = Channel(
                youtube_channel_id=f"yt-{uuid4().hex[:8]}",
                title="ch",
                project_id=project.id,
                refresh_token_enc="r",
                access_token_enc="a",
                access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                status="active",
                session_status="active",
            )
            session.add(channel)
            channels.append(channel)

        proxies = []
        for i in range(300):
            proxy = Proxy(url_enc=_enc(f"socks5://u:p@10.0.0.{i % 250}:1080"))
            session.add(proxy)
            proxies.append(proxy)
        await session.commit()
        channel_ids = [str(c.id) for c in channels]

    # Stand-in for the scan's write phase: per-wave transactions updating
    # proxy rows, exactly like ``_record_check_results`` does during check-all.
    async def scan_writer():
        for _ in range(40):
            async with session_maker() as session:
                result = await session.exec(select(Proxy))
                rows = list(result.scalars().all())
                now = datetime.now(timezone.utc)
                for row in rows:
                    row.last_checked_at = now
                    row.status = "working"
                await session.commit()
            await asyncio.sleep(0.01)

    writer = asyncio.create_task(scan_writer())
    try:
        await asyncio.sleep(0.1)  # let the writer get going

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
                "/channels/bulk-settings",
                json={
                    "channel_ids": channel_ids,
                    "max_batch_size": 12,
                    "inter_upload_delay_seconds": 3,
                },
            )
            elapsed = time.perf_counter() - started

        assert response.status_code == 200, response.text
        assert len(response.json()["updated"]) == 15
        # The whole point of the fix: one short transaction, not 15 queued ones.
        assert elapsed < 1.5, f"bulk save took {elapsed:.2f}s during a proxy scan"
    finally:
        writer.cancel()
        try:
            await writer
        except asyncio.CancelledError:
            pass
        await engine.dispose()
