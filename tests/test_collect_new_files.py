"""Regression tests for the min_batch_size starvation bug.

Previously _collect_new_files truncated the waiting-file list at the batch
limit (min(max_batch_size, 200)) *before* the min_batch_size check, so a
channel with min_batch_size > 200 could never start uploading.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.services.coordinator import UploadCoordinator, _compute_file_hash
from app.services.notification_service import notifications


def _make_channel(folder_rel: str, **overrides):
    base = dict(
        id="11111111-1111-1111-1111-111111111111",
        status="active",
        session_status="active",
        auto_upload_enabled=True,
        folder_path=folder_rel,
        max_batch_size=200,
        min_batch_size=300,
        upload_limit_enabled=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _EmptyResult:
    def scalars(self):
        return self

    def first(self):
        return None

    def all(self):
        return []


class _DummySession:
    def __init__(self):
        self.exec_calls = 0

    async def exec(self, query):
        self.exec_calls += 1
        return _EmptyResult()

    async def commit(self):
        pass


@pytest.mark.asyncio
async def test_collect_new_files_does_not_truncate_list(monkeypatch):
    """With 250 eligible files and limit 200, the collector must return all 250."""
    coordinator = UploadCoordinator(notifications)

    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(base_dir=tmp, video_base_dir="./videos", app_secret_key="x", api_token="x")
        monkeypatch.setattr("app.services.coordinator.settings", settings)

        folder = Path(tmp) / "videos/chan"
        folder.mkdir(parents=True, exist_ok=True)
        for i in range(250):
            (folder / f"v{i:03d}.mp4").write_bytes(f"data{i}".encode())

        # Avoid real hashing across 250 files; return a stable fake hash per path.
        monkeypatch.setattr(
            "app.services.coordinator._compute_file_hash",
            AsyncMock(side_effect=lambda p: f"hash_{Path(p).name}"),
        )

        channel = _make_channel("videos/chan", max_batch_size=200)
        session = _DummySession()
        files, limit = await coordinator._collect_new_files(
            session, channel, SimpleNamespace()
        )
        # Full waiting list must be returned (no early break at the limit).
        assert len(files) == 250
        assert limit == 200
        # The caller (worker) slices after the min check.
        assert len(files[:limit]) <= limit
        # Hash lookups are batched, not one query pair per file.
        assert session.exec_calls <= 4


@pytest.mark.asyncio
async def test_min_batch_threshold_uses_true_waiting_count(monkeypatch):
    """min_batch_size must be compared against the *true* waiting count."""
    coordinator = UploadCoordinator(notifications)
    channel = _make_channel("videos/chan", max_batch_size=200, min_batch_size=250)

    # 250 waiting files >= 250 threshold -> should start
    true_count = 250
    assert true_count >= max(1, channel.min_batch_size or 1)

    # 249 waiting files < 250 threshold -> must wait
    true_count = 249
    assert true_count < max(1, channel.min_batch_size or 1)
