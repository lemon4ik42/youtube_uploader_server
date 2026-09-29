"""Tests for the streaming base64 transfer of OpenAI-compatible providers."""

import asyncio
import base64
import json
import os
from pathlib import Path

import httpx
import pytest

import app.services.ai_providers.openai_compatible as oc
from app.services.ai_providers.base import ProviderError
from app.services.ai_providers.openai_compatible import (
    OpenAICompatibleProvider,
    _VideoHandle,
    build_request_body,
    classify_openai_error,
)


def _provider() -> OpenAICompatibleProvider:
    return OpenAICompatibleProvider("https://api.example.com/v1/")


# --- Arithmetic ---------------------------------------------------------------


def test_b64_payload_size_exact():
    for n in range(0, 40):
        assert oc._b64_payload_size(n) == len(base64.b64encode(b"x" * n))
    # Large sizes, including chunk-boundary straddlers.
    for n in (5_242_880, 5_242_881, 5_242_882, 5_242_883, 10_000_001):
        assert oc._b64_payload_size(n) == (n + 2) // 3 * 4


def test_read_size_is_multiple_of_three():
    from app.core.config import settings

    read_size = max(3, (settings.upload_read_chunk_size // 3) * 3)
    assert read_size % 3 == 0
    assert read_size > 0


# --- Request body construction -------------------------------------------------


def test_placeholder_smuggling_rejected():
    with pytest.raises(ProviderError):
        build_request_body(
            "m", _VideoHandle(Path("v.mp4"), "video/mp4"), f"x {oc._VIDEO_PLACEHOLDER} y"
        )


def test_body_is_valid_json_with_placeholder():
    body = build_request_body("m", _VideoHandle(Path("v.mp4"), "video/mp4"), "prompt")
    serialized = json.dumps(body, ensure_ascii=False)
    assert serialized.count(oc._VIDEO_PLACEHOLDER) == 1


# --- Streaming assembly equals the reference JSON ------------------------------


async def _collect_stream(provider, path, prompt="p"):
    handle = _VideoHandle(path, "video/mp4")
    body = build_request_body("m", handle, prompt)
    prefix, suffix = provider._split_body_around_video(body, handle)

    import aiofiles

    read_size = max(3, (provider_chunk_size := 192) * 3) if False else max(
        3, (oc.settings.upload_read_chunk_size // 3) * 3
    )
    # Use a small read size to force many chunks.
    read_size = 3 * 7  # 21 bytes per read
    chunks = [prefix, b"data:video/mp4;base64,"]
    async with aiofiles.open(path, "rb") as f:
        while True:
            c = await f.read(read_size)
            if not c:
                break
            chunks.append(base64.b64encode(c))
    chunks.append(suffix)
    return handle, body, b"".join(chunks), prefix, suffix


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size", [0, 1, 2, 3, 4, 5, 6, 7, 20, 21, 22, 23, 1000, 1024 * 7 + 2]
)
async def test_stream_equals_reference_json(tmp_path, size):
    provider = _provider()
    path = tmp_path / "v.mp4"
    path.write_bytes(os.urandom(size))
    handle, body, stream, prefix, suffix = await _collect_stream(
        provider, path, prompt="описание ✓ <>& \"quoted\""
    )

    data_url = f"data:video/mp4;base64,{base64.b64encode(path.read_bytes()).decode()}"
    reference = (
        json.dumps(body, ensure_ascii=False)
        .replace(oc._VIDEO_PLACEHOLDER, data_url)
        .encode("utf-8")
    )
    assert stream == reference
    # Exact content length arithmetic.
    expected_length = (
        len(prefix) + len("data:video/mp4;base64,") + oc._b64_payload_size(size) + len(suffix)
    )
    assert len(stream) == expected_length


# --- generate() against a local HTTP server ------------------------------------


class _RecordingServer:
    """Minimal HTTP/1.1 server that records the streamed request."""

    def __init__(self, response_status=200, response_body=None):
        self.response_status = response_status
        self.response_body = response_body or {}
        self.state = {}
        self._server = None

    async def __aenter__(self):
        self._server = await asyncio.start_server(
            self._handle, "127.0.0.1", 0
        )
        port = self._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1"
        return self

    async def __aexit__(self, *args):
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader, writer):
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                data += await reader.read(4096)
            head, rest = data.split(b"\r\n\r\n", 1)
            headers = head.decode()
            self.state["headers"] = headers
            self.state["auth"] = [l for l in headers.split("\r\n") if l.startswith("Authorization")]
            cl = int(
                [
                    l
                    for l in headers.split("\r\n")
                    if l.lower().startswith("content-length")
                ][0].split(":")[1]
            )
            body = bytearray(rest)
            reads = 1
            while len(body) < cl:
                chunk = await reader.read(64 * 1024)
                if not chunk:
                    break
                body += chunk
                reads += 1
            self.state["content_length"] = cl
            self.state["body"] = bytes(body)
            self.state["reads"] = reads
            if self.response_status == 200:
                payload = json.dumps(self.response_body).encode()
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                    + payload
                )
            else:
                error_body = dict(self.response_body)
                error_body.setdefault("message", "err")
                payload = json.dumps({"error": error_body}).encode()
                writer.write(
                    f"HTTP/1.1 {self.response_status} Error\r\nContent-Type: application/json\r\n".encode()
                    + f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                    + payload
                )
            await writer.drain()
        finally:
            writer.close()


def _ok_response(title="T", description="D"):
    return {
        "choices": [
            {"message": {"content": json.dumps({"title": title, "description": description})}}
        ]
    }


@pytest.mark.asyncio
async def test_generate_streams_video_and_parses_response(tmp_path):
    video = os.urandom(1024 * 300 + 17)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    async with _RecordingServer(response_body=_ok_response("Streamed", "Desc")) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk-test", path)
        text = await provider.generate("sk-test", "gpt-4o", handle, "prompt")

    assert json.loads(text)["title"] == "Streamed"
    # Content-Length header matches the body byte-for-byte (h11 enforces
    # this too — a mismatch raises LocalProtocolError).
    assert srv.state["content_length"] == len(srv.state["body"])
    # The sent body is valid JSON and the video round-trips.
    sent = json.loads(srv.state["body"])
    data_url = sent["messages"][0]["content"][1]["video_url"]["url"]
    assert data_url.startswith("data:video/mp4;base64,")
    assert base64.b64decode(data_url.split(",", 1)[1]) == video
    assert sent["model"] == "gpt-4o"
    assert sent["response_format"] == {"type": "json_object"}
    # Actually streamed: the server saw many reads, not one blob.
    assert srv.state["reads"] > 5
    # Bearer auth header present.
    assert srv.state["auth"] == ["Authorization: Bearer sk-test"]


@pytest.mark.asyncio
async def test_generate_books_memory_limiter(tmp_path, monkeypatch):
    """The transfer is booked in the global upload memory limiter for its
    whole duration, like a YouTube upload."""
    from app.services.memory_limiter import MemoryLimiter

    video = os.urandom(1000)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    events = []
    real_acquire = MemoryLimiter.acquire
    real_release = MemoryLimiter.release

    async def spy_acquire(self, weight):
        booked = await real_acquire(self, weight)
        events.append(("acquire", weight, booked))
        return booked

    async def spy_release(self, weight):
        events.append(("release", weight))
        return await real_release(self, weight)

    monkeypatch.setattr(MemoryLimiter, "acquire", spy_acquire)
    monkeypatch.setattr(MemoryLimiter, "release", spy_release)

    async with _RecordingServer(response_body=_ok_response()) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        await provider.generate("sk", "m", handle, "p")

    assert ("acquire", 1000, 1000) in events
    assert ("release", 1000) in events
    # Acquire happens before release (balanced).
    acquire_idx = events.index(("acquire", 1000, 1000))
    release_idx = events.index(("release", 1000))
    assert acquire_idx < release_idx


@pytest.mark.asyncio
async def test_generate_releases_limiter_on_error(tmp_path):
    video = os.urandom(100)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    from app.services.memory_limiter import upload_memory_limiter

    used_before = upload_memory_limiter._used
    async with _RecordingServer(response_status=500, response_body={"message": "boom"}) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        with pytest.raises(ProviderError):
            await provider.generate("sk", "m", handle, "p")
    assert upload_memory_limiter._used == used_before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,error,expected_status,expected_transient",
    [
        # OpenAI's billing/quota errors: the key is out of quota, retrying
        # never helps.
        (
            429,
            {
                "message": "You exceeded your current quota, please check your plan and billing details.",
                "type": "insufficient_quota",
                "code": "insufficient_quota",
            },
            "quota_exceeded",
            False,
        ),
        (
            429,
            {"message": "x", "type": "insufficient_quota"},
            "quota_exceeded",
            False,
        ),
        (
            429,
            {"message": "Rate limit reached", "code": "credit_balance_exhausted"},
            "quota_exceeded",
            False,
        ),
        # Every other 429 is a temporary condition: rate limits, slow_down,
        # a gateway's "service is temporarily overloaded".
        (
            429,
            {
                "message": "Rate limit reached for requests",
                "type": "rate_limit_error",
                "code": "rate_limit_exceeded",
            },
            None,
            True,
        ),
        (429, {"message": "Slow down", "code": "slow_down"}, None, True),
        (429, {"message": "The service is temporarily overloaded"}, None, True),
        (429, {"message": "Too many requests"}, None, True),
        (401, {"message": "Incorrect API key provided"}, "not_working", False),
        (403, {"message": "Permission denied"}, "not_working", False),
        (400, {"message": "You didn't provide an API key"}, "not_working", False),
        (404, {"message": "The model 'gpt-x' does not exist"}, None, False),
        (500, {"message": "Internal server error"}, None, True),
        (
            503,
            {
                "message": "Model temporarily overloaded",
                "type": "service_unavailable_error",
                "code": "server_is_overloaded",
            },
            None,
            True,
        ),
        (400, {"message": "File size exceeds the limit"}, None, False),
    ],
)
async def test_generate_maps_api_errors(
    tmp_path, status, error, expected_status, expected_transient
):
    video = os.urandom(10)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    async with _RecordingServer(response_status=status, response_body=error) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        with pytest.raises(ProviderError) as exc:
            await provider.generate("sk", "m", handle, "p")
    assert exc.value.key_status == expected_status
    assert exc.value.transient == expected_transient


@pytest.mark.asyncio
async def test_upload_rejects_oversize_video(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.services.ai_providers.openai_compatible.settings.ai_openai_max_inline_video_bytes",
        100,
    )
    provider = _provider()
    path = tmp_path / "v.mp4"
    path.write_bytes(b"x" * 101)
    with pytest.raises(ProviderError) as exc:
        await provider.upload_file("sk", path)
    assert exc.value.key_status is None  # not the key's fault
    assert "bytes" in exc.value.message


@pytest.mark.asyncio
async def test_upload_allows_size_at_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.services.ai_providers.openai_compatible.settings.ai_openai_max_inline_video_bytes",
        100,
    )
    provider = _provider()
    path = tmp_path / "v.mp4"
    path.write_bytes(b"x" * 100)
    handle = await provider.upload_file("sk", path)
    assert handle.mime == "video/mp4"


@pytest.mark.asyncio
async def test_upload_missing_file_raises(tmp_path):
    provider = _provider()
    with pytest.raises(ProviderError):
        await provider.upload_file("sk", tmp_path / "nope.mp4")


@pytest.mark.asyncio
async def test_generate_response_without_content_raises(tmp_path):
    video = os.urandom(10)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    async with _RecordingServer(response_body={"choices": []}) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        with pytest.raises(ProviderError) as exc:
            await provider.generate("sk", "m", handle, "p")
    assert exc.value.key_status is None


@pytest.mark.asyncio
async def test_generate_content_as_parts(tmp_path):
    """Some OpenAI-compatible servers return content as a list of parts."""
    video = os.urandom(10)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)
    response = {
        "choices": [
            {"message": {"content": [{"type": "text", "text": '{"title": "Parts"}'}]}}
        ]
    }
    async with _RecordingServer(response_body=response) as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        text = await provider.generate("sk", "m", handle, "p")
    assert json.loads(text)["title"] == "Parts"


@pytest.mark.asyncio
async def test_generate_non_json_response_raises(tmp_path):
    """A non-JSON response body is a transient provider error."""
    video = os.urandom(10)
    path = tmp_path / "v.mp4"
    path.write_bytes(video)

    # Reuse the recording server but override the handler's response.
    server = _RecordingServer(response_body=_ok_response())

    async def raw_handler(reader, writer):
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                data += await reader.read(4096)
            head, rest = data.split(b"\r\n\r\n", 1)
            cl = int(
                [
                    l
                    for l in head.decode().split("\r\n")
                    if l.lower().startswith("content-length")
                ][0].split(":")[1]
            )
            body = bytearray(rest)
            while len(body) < cl:
                chunk = await reader.read(64 * 1024)
                if not chunk:
                    break
                body += chunk
            payload = b"<html>gateway error</html>"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
                + f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
        finally:
            writer.close()

    server._handle = raw_handler
    async with server as srv:
        provider = OpenAICompatibleProvider(srv.url)
        handle = await provider.upload_file("sk", path)
        with pytest.raises(ProviderError) as exc:
            await provider.generate("sk", "m", handle, "p")
    assert exc.value.key_status is None


def test_classify_openai_errors_direct():
    # 429 without a quota/billing marker is a rate limit or temporary
    # overload: transient, the key status stays unchanged.
    assert classify_openai_error(429, "Rate limit reached") is None
    assert classify_openai_error(429, "The service may be temporarily overloaded") is None
    # The specific quota errors of the OpenAI API.
    assert classify_openai_error(429, "x", error_code="insufficient_quota") == "quota_exceeded"
    assert classify_openai_error(429, "x", error_type="insufficient_quota") == "quota_exceeded"
    assert (
        classify_openai_error(
            429, "You exceeded your current quota, please check your plan and billing details."
        )
        == "quota_exceeded"
    )
    assert classify_openai_error(401, "x") == "not_working"
    assert classify_openai_error(403, "x") == "not_working"
    assert classify_openai_error(400, "Invalid API key") == "not_working"
    assert classify_openai_error(404, "model not found") is None
    assert classify_openai_error(503, "unavailable") is None
