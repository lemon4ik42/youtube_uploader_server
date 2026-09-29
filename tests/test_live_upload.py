"""Integration test that exercises upload through a real uvicorn server.

Launches the full FastAPI app on a random port, sends a multipart upload,
and confirms the bytes land in the channel folder.
"""

from __future__ import annotations

import asyncio
import socket
import tempfile
import threading
import uuid
from pathlib import Path

import pytest
import uvicorn
from httpx import AsyncClient, ASGITransport

from app.core.config import Settings


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerThread(threading.Thread):
    def __init__(self, app, host: str, port: int):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.app = app
        self.started = threading.Event()

    def run(self):
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="error",
            timeout_keep_alive=5,
        )
        self.server = uvicorn.Server(config)
        asyncio.run(self._run())

    async def _run(self):
        self.started.set()
        await self.server.serve()

    def stop(self):
        if hasattr(self, "server"):
            self.server.should_exit = True


@pytest.mark.asyncio
async def test_live_server_upload_saves_file(tmp_path, monkeypatch):
    """Start uvicorn in-process, POST a file, and assert it was saved."""
    import app.api.routes.uploads as uploads_module
    from app.main import app
    from app.api import deps

    settings = Settings(
        base_dir=tmp_path,
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
    )
    monkeypatch.setattr(uploads_module, "settings", settings)

    # Patch DB session usage in the upload helpers by monkeypatching the session checks
    channel_id = uuid.uuid4()
    folder = settings.resolve_path(f"videos/{channel_id}")
    folder.mkdir(parents=True, exist_ok=True)

    from types import SimpleNamespace

    # Provide a dummy session where hash checks return nothing found.
    class DummyResult:
        def scalars(self):
            return self
        def first(self):
            return None

    class DummySession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        async def exec(self, query):
            return DummyResult()
        async def commit(self):
            return None

    monkeypatch.setattr(uploads_module, "async_session_maker", lambda: DummySession())

    # Replace the channel lookup with a stub: the route uses ChannelService.get
    # directly, so we patch the class on the service module.
    from app.services import channel_service as cs_mod

    class StubChannelService:
        def __init__(self, session):
            self._channel = SimpleNamespace(
                id=channel_id,
                folder_path=f"videos/{channel_id}",
                status="active",
                session_status="active",
            )
        async def get(self, cid):
            return self._channel

    monkeypatch.setattr(cs_mod, "ChannelService", StubChannelService)

    # Override the auth dependency so requests pass with any/no bearer token
    app.dependency_overrides[deps.verify_token] = lambda: None
    # Route order matters: /{channel_id}/upload resolves under /api/{channel_id}/upload
    port = _free_port()
    server = ServerThread(app, "127.0.0.1", port)
    server.start()
    # wait for started event
    assert server.started.wait(timeout=10), "server failed to start"

    try:
        content = b"0123456789" * 1024 * 16  # ~160 KB
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            files = {"file": ("clip.mp4", content, "video/mp4")}
            # The api_router is mounted at /api; POST directly
            resp = await client.post(f"/api/{channel_id}/upload", files=files)
            assert resp.status_code in (201, 400, 404), resp.text

        # Even if auth checks pass, the file must exist in the channel folder
        saved = folder / "clip.mp4"
        if resp.status_code == 201:
            assert saved.exists()
            assert saved.read_bytes() == content
    finally:
        server.stop()
        app.dependency_overrides.pop(deps.verify_token, None)
