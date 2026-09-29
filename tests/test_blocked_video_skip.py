"""Copyright-blocked video filtering and YouTube status classification."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import notifications
from app.services.youtube_uploader import (
    delete_video,
    is_copyright_blocked_everywhere,
    wait_for_video_processing,
)


class _Result:
    def __init__(self, values=()):
        self._values = list(values)

    def scalars(self):
        return self

    def all(self):
        return self._values


class _Session:
    def __init__(self, blocked_hash: str):
        self.blocked_hash = blocked_hash

    async def commit(self):
        pass

    async def exec(self, query):
        # The collector only needs the queried hashes here. Returning the
        # marked hash for the BlockedVideoHash lookup exercises the filter;
        # every other lookup is intentionally empty.
        if "blocked_video_hash" in str(query):
            return _Result([self.blocked_hash])
        return _Result()


@pytest.mark.asyncio
async def test_collect_skips_globally_blocked_hash_only_when_enabled(tmp_path, monkeypatch):
    import app.services.coordinator as coord_mod

    settings = Settings(
        base_dir=str(tmp_path), video_base_dir="./videos", app_secret_key="x", api_token="x"
    )
    monkeypatch.setattr(coord_mod, "settings", settings)
    folder = tmp_path / "videos" / "channel"
    folder.mkdir(parents=True)
    (folder / "blocked.mp4").write_bytes(b"blocked")
    (folder / "new.mp4").write_bytes(b"new")

    hashes = {
        str(folder / "blocked.mp4"): "blocked-hash",
        str(folder / "new.mp4"): "new-hash",
    }
    monkeypatch.setattr(
        coord_mod,
        "_compute_file_hash",
        AsyncMock(side_effect=lambda path: hashes[str(Path(path))]),
    )

    channel = SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        folder_path="videos/channel",
        max_batch_size=200,
        upload_limit_enabled=False,
        shuffle_upload_order=False,
        skip_blocked_videos=True,
        status="active",
    )
    coordinator = UploadCoordinator(notifications)
    files, _ = await coordinator._collect_new_files(
        _Session("blocked-hash"), channel, SimpleNamespace()
    )
    assert files == [("videos/channel/new.mp4", "new-hash")]

    channel.skip_blocked_videos = False
    files, _ = await coordinator._collect_new_files(
        _Session("blocked-hash"), channel, SimpleNamespace()
    )
    assert {file_hash for _, file_hash in files} == {"blocked-hash", "new-hash"}


def test_detects_only_copyright_blocks_that_are_global():
    assert is_copyright_blocked_everywhere(
        {
            "contentDetails": {
                "licensedContent": True,
                "regionRestriction": {"allowed": []},
            }
        }
    )
    assert not is_copyright_blocked_everywhere(
        {
            "contentDetails": {
                "licensedContent": True,
                "regionRestriction": {"allowed": ["US"]},
            }
        }
    )
    assert not is_copyright_blocked_everywhere(
        {
            "contentDetails": {
                "licensedContent": False,
                "regionRestriction": {"allowed": []},
            }
        }
    )
    assert is_copyright_blocked_everywhere(
        {"status": {"uploadStatus": "rejected", "rejectionReason": "copyright"}}
    )


@pytest.mark.asyncio
async def test_wait_for_processing_polls_until_terminal_status(monkeypatch):
    import app.services.youtube_uploader as uploader

    get_status = AsyncMock(
        side_effect=[
            {"processingDetails": {"processingStatus": "processing"}},
            {
                "processingDetails": {"processingStatus": "succeeded"},
                "contentDetails": {
                    "licensedContent": True,
                    "regionRestriction": {"allowed": []},
                },
            },
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(uploader, "get_uploaded_video_status", get_status)
    monkeypatch.setattr(uploader.asyncio, "sleep", sleep)

    status = await wait_for_video_processing("video-id", "token", poll_seconds=0.1)

    assert get_status.await_count == 2
    sleep.assert_awaited_once_with(0.1)
    assert is_copyright_blocked_everywhere(status)


@pytest.mark.asyncio
async def test_processed_upload_status_is_terminal_when_processing_details_missing(monkeypatch):
    import app.services.youtube_uploader as uploader

    get_status = AsyncMock(return_value={"status": {"uploadStatus": "processed"}})
    monkeypatch.setattr(uploader, "get_uploaded_video_status", get_status)

    await wait_for_video_processing("video-id", "token", poll_seconds=0.1)

    get_status.assert_awaited_once()


@pytest.mark.asyncio
async def test_processing_waits_for_multiple_videos_concurrently(monkeypatch):
    import asyncio
    import app.services.youtube_uploader as uploader

    responses = {
        "first": [
            {"processingDetails": {"processingStatus": "processing"}},
            {"processingDetails": {"processingStatus": "succeeded"}},
        ],
        "second": [
            {"processingDetails": {"processingStatus": "processing"}},
            {"processingDetails": {"processingStatus": "succeeded"}},
        ],
    }

    async def get_status(video_id, *_args, **_kwargs):
        return responses[video_id].pop(0)

    real_sleep = asyncio.sleep

    async def yield_control(_delay):
        await real_sleep(0)

    monkeypatch.setattr(uploader, "get_uploaded_video_status", get_status)
    monkeypatch.setattr(uploader.asyncio, "sleep", yield_control)

    results = await asyncio.gather(
        wait_for_video_processing("first", "token", poll_seconds=0.1),
        wait_for_video_processing("second", "token", poll_seconds=0.1),
    )

    assert [item["processingDetails"]["processingStatus"] for item in results] == [
        "succeeded",
        "succeeded",
    ]
    assert responses == {"first": [], "second": []}


@pytest.mark.asyncio
async def test_delete_uses_live_proxy_resolver(monkeypatch):
    import app.services.youtube_uploader as uploader

    class Response:
        status_code = 204

    request = AsyncMock(return_value=Response())
    resolver = AsyncMock(return_value="socks5://proxy.example:1080")
    monkeypatch.setattr(uploader, "proxy_request", request)

    await delete_video("video-id", "token", proxy_resolver=resolver)

    resolver.assert_awaited_once()
    assert request.await_args.kwargs["proxy_url"] == "socks5://proxy.example:1080"
    assert request.await_args.kwargs["params"] == {"id": "video-id"}
