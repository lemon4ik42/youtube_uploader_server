"""The scan loop must not re-read the whole video library while idle.

``_collect_new_files`` runs every ``channel_scan_interval_seconds`` for every
channel and hashes each file in the folder. Re-hashing unchanged files kept the
disk busy and churned a fresh buffer per chunk on an otherwise idle server, so
digests are cached against (size, mtime).
"""

from __future__ import annotations

import pytest

from app.core.config import Settings
from app.services import coordinator as coord


@pytest.fixture
def video_file(monkeypatch, tmp_path):
    settings = Settings(base_dir=str(tmp_path), app_secret_key="x", api_token="x")
    monkeypatch.setattr(coord, "settings", settings)
    coord._hash_cache.clear()
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"video-bytes" * 1000)
    return path


@pytest.mark.asyncio
async def test_unchanged_file_is_hashed_once(video_file, monkeypatch):
    import builtins

    reads = 0
    real_open = builtins.open
    target = str(video_file)

    def counting_open(file, *args, **kwargs):
        nonlocal reads
        if str(file) == target:
            reads += 1
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", counting_open)

    first = await coord._compute_file_hash(target)
    for _ in range(10):
        assert await coord._compute_file_hash(target) == first
    assert reads == 1, "an unchanged file must be read from disk only once"


@pytest.mark.asyncio
async def test_modified_file_is_rehashed(video_file):
    first = await coord._compute_file_hash(str(video_file))

    # Rewrite with different content and a distinct mtime.
    import os

    video_file.write_bytes(b"different-content" * 1000)
    stat = video_file.stat()
    os.utime(video_file, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert await coord._compute_file_hash(str(video_file)) != first


@pytest.mark.asyncio
async def test_cache_is_bounded(video_file, monkeypatch):
    monkeypatch.setattr(coord, "_HASH_CACHE_MAX_ENTRIES", 5)
    folder = video_file.parent
    for i in range(20):
        candidate = folder / f"v{i}.mp4"
        candidate.write_bytes(f"body{i}".encode())
        await coord._compute_file_hash(str(candidate))
    assert len(coord._hash_cache) <= 5


@pytest.mark.asyncio
async def test_deleting_a_file_forgets_its_digest(video_file):
    await coord._compute_file_hash(str(video_file))
    assert coord._hash_cache

    coord.forget_cached_hash(str(video_file))
    assert str(video_file) not in coord._hash_cache


@pytest.mark.asyncio
async def test_seeded_digest_avoids_reading_the_file(video_file, monkeypatch):
    """The upload endpoint hashes while streaming; the scan loop reuses that."""
    import builtins

    coord.remember_cached_hash(str(video_file), "seeded-digest")

    real_open = builtins.open
    target = str(video_file)

    def fail_open(file, *args, **kwargs):
        if str(file) == target:
            raise AssertionError("file must not be re-read after seeding the digest")
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fail_open)
    assert await coord._compute_file_hash(str(video_file)) == "seeded-digest"
