"""Persistent cache of AI-generated video metadata, keyed by file hash.

Channels with ``ai_use_cached`` enabled pick a random cached entry once the
maximum number of unique entries is stored for the video; below the maximum
new metadata is generated (and stored) instead. Channels without the flag
always generate new metadata; successful generations are still stored so the
cache fills up.

The caller owns the transaction: functions only add rows to the session.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select as sa_select

from app.core.logging import logger
from app.models.ai_model import AIGenerationCache

MAX_CACHED_ENTRIES_PER_VIDEO = 10


def _sort_key(entry: AIGenerationCache) -> tuple:
    # SQLite returns naive datetimes; normalize before comparing.
    created_at = entry.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return (created_at, str(entry.id))


async def _entries_for(session, file_hash: str) -> list[AIGenerationCache]:
    result = await session.exec(
        sa_select(AIGenerationCache).where(AIGenerationCache.file_hash == file_hash)
    )
    return list(result.scalars().all())


async def pick_cached_metadata(session, file_hash: str | None) -> dict | None:
    """Return a random cached entry for the video.

    Only once the maximum number of unique entries (10) is saved for this
    hash — below the maximum the caller should generate and store new
    metadata instead. Returns None when there is nothing to reuse.
    """
    if not file_hash:
        return None
    entries = await _entries_for(session, file_hash)
    if len(entries) < MAX_CACHED_ENTRIES_PER_VIDEO:
        return None
    entry = random.choice(entries)
    return {"title": entry.title, "description": entry.description}


async def save_cached_metadata(
    session, file_hash: str | None, title: str, description: str
) -> bool:
    """Store a generated (title, description) pair for the video hash.

    Skips duplicate pairs (the cache holds unique entries only) and never
    grows past the maximum of 10 entries per hash. The caller commits.
    """
    if not file_hash:
        return False
    description = description or ""
    entries = await _entries_for(session, file_hash)
    if any(
        e.title == title and e.description == description for e in entries
    ):
        return False
    if len(entries) >= MAX_CACHED_ENTRIES_PER_VIDEO:
        return False
    session.add(
        AIGenerationCache(
            id=uuid4(),
            file_hash=file_hash,
            title=title,
            description=description,
        )
    )
    # Autoflush makes the new row visible to subsequent picks in the same
    # batch; trim defensively in case a concurrent insert pushed past the cap.
    await session.flush()
    entries = await _entries_for(session, file_hash)
    if len(entries) > MAX_CACHED_ENTRIES_PER_VIDEO:
        entries.sort(key=_sort_key)
        for extra in entries[MAX_CACHED_ENTRIES_PER_VIDEO:]:
            await session.delete(extra)
        await session.flush()
        logger.debug(
            "Trimmed AI generation cache for hash {} to {} entries",
            file_hash,
            MAX_CACHED_ENTRIES_PER_VIDEO,
        )
    return True
