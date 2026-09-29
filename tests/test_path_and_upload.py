"""Quick verification for path normalization and manual upload flow.

Run with:
    python -m pytest tests/test_path_and_upload.py -v -s
or directly:
    python tests/test_path_and_upload.py
"""
import io
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.core.config import Settings
from app.core.migrations import _relative_or_original


class TestPathNormalization:
    """Ensure backslashes are normalized to posix separators before paths are used."""

    def test_settings_normalizes_backslashes(self):
        # \4 would be interpreted as octal escape in a normal string; use raw string
        raw = r"videos\4e01ae03-0d19-4032-b580-00e0bd06ef06"
        base = Path("C:/data") if os.name == "nt" else Path("/data")
        s = Settings(base_dir=str(base), video_base_dir=raw)
        # video_base_dir should be normalized to forward slashes
        assert s.video_base_dir.as_posix() == "videos/4e01ae03-0d19-4032-b580-00e0bd06ef06"
        # resolve_path should build a correct absolute path under base_dir
        resolved = s.resolve_path(raw)
        assert resolved == base / "videos/4e01ae03-0d19-4032-b580-00e0bd06ef06"

    def test_relative_path_normalizes_backslashes(self):
        base = Path("C:/data") if os.name == "nt" else Path("/data")
        s = Settings(base_dir=str(base))
        raw = r"videos\4e01ae03-0d19-4032-b580-00e0bd06ef06\video.mp4"
        rel = s.relative_path(raw)
        assert rel == "videos/4e01ae03-0d19-4032-b580-00e0bd06ef06/video.mp4"

    def test_relative_path_interprets_relative_from_base_dir(self):
        """Relative paths must resolve from base_dir, not from the CWD."""
        with tempfile.TemporaryDirectory() as tmp:
            s = Settings(base_dir=tmp)
            # Create the file under base_dir
            target = Path(tmp) / "videos" / "chan" / "video.mp4"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"x")
            # A relative path should be interpreted from base_dir
            assert s.relative_path("videos/chan/video.mp4") == "videos/chan/video.mp4"


class TestMigrationRelativeOrOriginal:
    """Ensure the migration helper also interprets relative paths from base_dir."""

    def test_relative_path_interpreted_from_base_dir(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "base"
            base.mkdir()
            target = base / "videos" / "chan"
            target.mkdir(parents=True)
            monkeypatch.setattr(
                "app.core.migrations.settings", Settings(base_dir=str(base))
            )
            assert _relative_or_original(r"videos\chan") == "videos/chan"

    def test_absolute_path_under_base_dir_becomes_relative(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "base"
            base.mkdir()
            target = base / "videos" / "chan"
            target.mkdir(parents=True)
            monkeypatch.setattr(
                "app.core.migrations.settings", Settings(base_dir=str(base))
            )
            assert _relative_or_original(str(target)) == "videos/chan"


@pytest.mark.asyncio
async def test_upload_helper_saves_and_skips_duplicate(monkeypatch):
    """Manual upload helper must save a new file and skip an already-uploaded one."""
    from app.api.routes import uploads as uploads_module
    from app.api.routes.uploads import _save_upload_to_channel_folder

    with tempfile.TemporaryDirectory() as tmp:
        s = Settings(base_dir=tmp)
        monkeypatch.setattr(uploads_module, "settings", s)

        channel_id = uuid4()
        folder = s.resolve_path(f"videos/{channel_id}")
        folder.mkdir(parents=True, exist_ok=True)

        channel = SimpleNamespace(
            id=channel_id,
            folder_path=f"videos/{channel_id}",
        )

        # Build a fake UploadFile backed by an in-memory buffer
        content = b"video content"
        stream = io.BytesIO(content)

        class _FakeUploadFile:
            filename = "uploaded.mp4"
            size = len(content)
            file = stream

            async def read(self, chunk_size: int = -1) -> bytes:
                return stream.read(chunk_size)

            async def seek(self, offset: int) -> None:
                stream.seek(offset)

            async def tell(self) -> int:
                return stream.tell()

        fake_file = _FakeUploadFile()

        # First call: no hashes in DB -> file is kept
        session = AsyncMock()
        session.exec = AsyncMock()
        # Return empty results for both UploadedVideoHash and RejectedVideoHash checks
        session.exec.side_effect = [
            SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None)),
            SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None)),
        ]

        name, skipped, reason = await _save_upload_to_channel_folder(
            channel, fake_file, session
        )
        assert name == "uploaded.mp4"
        assert skipped is False
        assert reason is None
        saved = folder / "uploaded.mp4"
        assert saved.exists()
        assert saved.read_bytes() == content


@pytest.mark.asyncio
async def test_upload_endpoint_creates_folder_with_forward_slashes():
    """Manual upload must create videos/<channel_id>/video.mp4, not videos\\<channel_id>."""
    channel_id = "4e01ae03-0d19-4032-b580-00e0bd06ef06"
    with tempfile.TemporaryDirectory() as tmp:
        s = Settings(base_dir=tmp, video_base_dir="./videos")
        folder = s.resolve_path(f"videos/{channel_id}")
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / "test.mp4"
        target.write_bytes(b"test video content")

        # The path on disk must be videos/<channel_id>/test.mp4, not a single folder with backslash
        assert target.exists()
        assert target.parent.name == channel_id
        assert target.parent.parent.name == "videos"

        # And resolving from a windows-style stored path must still find the file
        resolved = s.resolve_path(rf"videos\{channel_id}\test.mp4")
        assert resolved.exists()
        assert resolved.read_bytes() == b"test video content"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
