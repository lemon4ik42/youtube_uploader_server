"""Tests for the AI generation cache (app/services/ai_cache.py)."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.models.ai_model import AIGenerationCache
from app.services.ai_cache import (
    MAX_CACHED_ENTRIES_PER_VIDEO,
    pick_cached_metadata,
    save_cached_metadata,
)


@pytest.fixture
async def session(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    import app.models  # noqa: F401

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_cache.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with TestSession() as s:
        yield s
    await engine.dispose()


async def _add(session, file_hash, title, description="", count=1):
    for i in range(count):
        session.add(
            AIGenerationCache(
                id=uuid4(),
                file_hash=file_hash,
                title=f"{title}-{i}" if count > 1 else title,
                description=description,
                created_at=datetime.now(timezone.utc),
            )
        )
    await session.commit()


@pytest.mark.asyncio
async def test_pick_below_maximum_returns_none(session):
    await _add(session, "hash1", "Title", "Desc", count=MAX_CACHED_ENTRIES_PER_VIDEO - 1)
    assert await pick_cached_metadata(session, "hash1") is None


@pytest.mark.asyncio
async def test_pick_at_maximum_returns_random_entry(session):
    titles = [f"Title-{i}" for i in range(MAX_CACHED_ENTRIES_PER_VIDEO)]
    for title in titles:
        session.add(
            AIGenerationCache(
                id=uuid4(), file_hash="hash1", title=title, description="D"
            )
        )
    await session.commit()

    picks = [await pick_cached_metadata(session, "hash1") for _ in range(50)]
    assert all(p is not None for p in picks)
    assert {p["title"] for p in picks} <= set(titles)
    assert {p["description"] for p in picks} == {"D"}


@pytest.mark.asyncio
async def test_pick_is_per_video_hash(session):
    await _add(session, "hash1", "Title", count=MAX_CACHED_ENTRIES_PER_VIDEO)
    assert await pick_cached_metadata(session, "other-hash") is None


@pytest.mark.asyncio
async def test_pick_without_hash_returns_none(session):
    assert await pick_cached_metadata(session, None) is None
    assert await pick_cached_metadata(session, "") is None


@pytest.mark.asyncio
async def test_save_stores_new_pair(session):
    assert await save_cached_metadata(session, "hash1", "T1", "D1") is True
    await session.commit()
    result = await pick_cached_metadata(session, "hash1")
    assert result is None  # below the maximum
    from sqlmodel import select

    rows = (await session.exec(select(AIGenerationCache))).all()
    assert len(rows) == 1
    assert rows[0].title == "T1"
    assert rows[0].description == "D1"


@pytest.mark.asyncio
async def test_save_skips_duplicate_pairs(session):
    assert await save_cached_metadata(session, "hash1", "T1", "D1") is True
    assert await save_cached_metadata(session, "hash1", "T1", "D1") is False
    await session.commit()
    from sqlmodel import select

    assert len((await session.exec(select(AIGenerationCache))).all()) == 1


@pytest.mark.asyncio
async def test_save_never_exceeds_maximum(session):
    for i in range(MAX_CACHED_ENTRIES_PER_VIDEO + 5):
        await save_cached_metadata(session, "hash1", f"T{i}", f"D{i}")
    await session.commit()
    from sqlmodel import select

    rows = (await session.exec(select(AIGenerationCache))).all()
    assert len(rows) == MAX_CACHED_ENTRIES_PER_VIDEO


@pytest.mark.asyncio
async def test_save_trims_race_overflow(session, monkeypatch):
    """Rows past the maximum appearing between the count check and the insert
    (a concurrent writer race) are trimmed back to the maximum."""
    from app.services import ai_cache

    rows = [
        AIGenerationCache(
            id=uuid4(),
            file_hash="hash1",
            title=f"T{i}",
            description="D",
            created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
        for i in range(11)
    ]
    session.add_all(rows)
    await session.commit()

    real = ai_cache._entries_for
    calls = {"n": 0}

    async def fake_entries(sess, file_hash):
        calls["n"] += 1
        result = await real(sess, file_hash)
        # First call (the count check) misses two concurrently inserted rows.
        return result[:9] if calls["n"] == 1 else result

    monkeypatch.setattr(ai_cache, "_entries_for", fake_entries)

    assert await save_cached_metadata(session, "hash1", "New", "D") is True
    await session.commit()
    from sqlmodel import select

    remaining = (await session.exec(select(AIGenerationCache))).all()
    assert len(remaining) == MAX_CACHED_ENTRIES_PER_VIDEO


@pytest.mark.asyncio
async def test_save_without_hash_is_noop(session):
    assert await save_cached_metadata(session, None, "T", "D") is False
    assert await save_cached_metadata(session, "", "T", "D") is False


@pytest.mark.asyncio
async def test_save_empty_description_stored_as_empty_string(session):
    assert await save_cached_metadata(session, "hash1", "T", None) is True
    await session.commit()
    from sqlmodel import select

    row = (await session.exec(select(AIGenerationCache))).one()
    assert row.description == ""
