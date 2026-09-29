"""OpenAI-compatible provider with a user-defined base URL.

Structurally this mirrors the Gemini provider (sequential API keys with
live health statuses, model ordering by token cost for automatic
selection, one JSON answer containing both the title and the
description), but targets ``{base_url}/chat/completions`` of any
OpenAI-compatible API.

Transfer model: OpenAI-compatible chat completion APIs have no separate
resumable file upload like the Gemini Files API — the video must be
inlined into the single request body as a base64 data URL. The request
body is assembled and streamed chunk-by-chunk so the video is never
held in memory in full:

- ``upload_file`` performs the local checks only (file exists, size
  within the inline limit) and returns a lightweight handle; no bytes
  move yet.
- ``generate`` streams the JSON body through the shared HTTP client:
  the base64 payload is produced by an async generator that reads the
  file in ``upload_read_chunk_size`` chunks, encodes each chunk with
  ``base64.b64encode`` (C implementation, allocation ~= 4/3 of the
  chunk) and yields it immediately. The exact ``Content-Length`` is
  precomputed arithmetically (``4 * ceil(size / 3)`` for the payload),
  so httpx keeps streaming instead of buffering the body.
- The whole request is booked in the global upload memory limiter
  (weight = raw file size), the same accounting a YouTube or Gemini
  upload uses, so concurrent transfers share one memory budget.
- Unlike Gemini (one file upload per API key, then N model calls),
  every model attempt re-sends the whole video: that is a property of
  the protocol, and it is the main reason to keep the model list short
  and the inline size limit low.

Errors map like Gemini's: HTTP 429 is only ``quota_exceeded`` when the
error body is one of OpenAI's billing/quota errors (``insufficient_quota``,
``credit_balance_exhausted``, the ``*_spend_limit_exceeded`` /
``*_usage_limit_exceeded`` codes) — everything else about 429 (rate
limits, ``slow_down``, a gateway's "service is temporarily overloaded")
is transient and leaves the key status unchanged. 401/403 (or a 400 about
the API key) -> not_working, everything else (404 model not found, 5xx,
network) is transient and leaves the key status unchanged too.
"""

from __future__ import annotations

import asyncio
import base64
import json
import mimetypes
from pathlib import Path
from typing import Any, AsyncIterator

import aiofiles
import httpx

from app.core.config import settings
from app.core.logging import logger
from app.models.ai_model import (
    KEY_STATUS_NOT_WORKING,
    KEY_STATUS_QUOTA_EXCEEDED,
)
from app.services.ai_providers.base import AIProvider, ProviderError
from app.services.http_client import get_client
from app.services.memory_limiter import upload_memory_limiter


class _VideoHandle:
    """Local-only handle: the path and mime type of the prepared video."""

    __slots__ = ("path", "mime")

    def __init__(self, path: Path, mime: str):
        self.path = path
        self.mime = mime


# OpenAI 429 responses cover two very different conditions. The billing /
# quota ones mean the API key's plan limit is exhausted: retrying them
# never restores access (see the OpenAI error codes guide), so they map to
# quota_exceeded. OpenAI signals them through ``error.code`` (the broader
# ``error.type`` is ``insufficient_quota`` for all of them).
_QUOTA_ERROR_CODES = frozenset(
    {
        "insufficient_quota",  # "You exceeded your current quota, ..."
        "credit_balance_exhausted",
        "organization_spend_limit_exceeded",
        "project_spend_limit_exceeded",
        "organization_usage_limit_exceeded",
    }
)

# Message fragments that identify the same quota/billing condition in
# gateways that return a plain message without the structured fields
# ("You exceeded your current quota, please check your plan and billing
# details", "insufficient quota", "credit balance exhausted", ...).
_QUOTA_MESSAGE_MARKERS = (
    "quota",
    "billing",
    "spend limit",
    "usage limit",
    "credit",
)

# HTTP statuses whose failure a later retry can survive once the key
# itself is proven fine: payment required (some gateways use it for
# exhausted credits; without a quota marker it stays retryable), request
# timeout, conflict, too-early, rate limit and every server-side error.
_TRANSIENT_STATUS_CODES = frozenset({402, 408, 409, 425, 429})


def classify_openai_error(
    status_code: int,
    message: str,
    error_code: str | int | None = None,
    error_type: str | None = None,
) -> str | None:
    """Map an OpenAI-compatible API error to a key status (None = unchanged).

    ``error_code``/``error_type`` are the ``error.code``/``error.type``
    fields of the response body when the server sent them. The quota
    decision for 429 (and 402 Payment Required used by some gateways for
    exhausted credits) never relies on the status alone: without a billing
    code or message the error is treated as a rate limit / temporary
    overload, i.e. transient.
    """
    lowered = (message or "").lower()
    code = str(error_code or "").strip().lower()
    err_type = str(error_type or "").strip().lower()
    if status_code in (429, 402):
        if code in _QUOTA_ERROR_CODES or err_type == "insufficient_quota":
            return KEY_STATUS_QUOTA_EXCEEDED
        if any(marker in lowered for marker in _QUOTA_MESSAGE_MARKERS):
            return KEY_STATUS_QUOTA_EXCEEDED
        # Rate limit ("Rate limit reached...", slow_down) or temporary
        # overload ("The service is temporarily overloaded"): transient.
        return None
    if status_code in (401, 403):
        return KEY_STATUS_NOT_WORKING
    if status_code == 400 and (
        "api key" in lowered or "api_key" in lowered or "apikey" in lowered
    ):
        return KEY_STATUS_NOT_WORKING
    # 404 = model not found, 5xx/network = transient: not the key's fault.
    return None


def translate_api_error(exc: Exception) -> ProviderError:
    """Map an httpx/SDK exception to a ProviderError with a key status."""
    if isinstance(exc, ProviderError):
        return exc
    if isinstance(exc, asyncio.TimeoutError):
        return ProviderError(
            f"OpenAI-compatible request timed out: {exc}", None, transient=True
        )
    if isinstance(exc, (httpx.HTTPError, OSError)):
        return ProviderError(
            f"OpenAI-compatible network error: {exc}", None, transient=True
        )
    return ProviderError(
        f"OpenAI-compatible request failed: {exc}", None, transient=True
    )


def _detect_mime(file_path: Path) -> str:
    mime, _ = mimetypes.guess_type(file_path.name)
    return mime or "video/mp4"


def _b64_payload_size(raw_size: int) -> int:
    """Exact length of the base64 encoding of ``raw_size`` bytes.

    Base64 encodes 3 bytes into 4 characters; the trailing partial group
    is padded to 4.
    """
    return (raw_size + 2) // 3 * 4


def _encode_chunk(chunk: bytes) -> bytes:
    """Encode one chunk to base64 without interior padding.

    Only chunks whose length is a multiple of 3 can be encoded
    independently: any other boundary would emit ``=`` padding in the
    middle of the stream. The reader therefore reads in multiples of 3;
    only the final chunk may be shorter.
    """
    return base64.b64encode(chunk)


def _extract_error_fields(response: httpx.Response) -> tuple[str, str, str]:
    """(message, error.code, error.type) from an OpenAI-style error body.

    OpenAI-compatible servers answer with ``{"error": {"message",
    "type", "code"}}``; gateways often fill only the message (and ``code``
    may be an int). Flat bodies without the ``error`` wrapper and plain
    non-JSON bodies degrade to the raw text with empty code/type.
    """
    try:
        data = response.json()
    except Exception:
        return response.text, "", ""
    if not isinstance(data, dict):
        return response.text, "", ""
    error = data.get("error")
    if isinstance(error, dict):
        message = str(error.get("message") or response.text)
        return message, str(error.get("code") or ""), str(error.get("type") or "")
    if isinstance(error, str):
        return error, "", ""
    if data.get("message"):
        return (
            str(data.get("message")),
            str(data.get("code") or ""),
            str(data.get("type") or ""),
        )
    return response.text, "", ""


def _raise_api_error(response: httpx.Response, context: str) -> None:
    message, error_code, error_type = _extract_error_fields(response)
    key_status = classify_openai_error(
        response.status_code, message, error_code, error_type
    )
    transient = key_status is None and (
        response.status_code >= 500 or response.status_code in _TRANSIENT_STATUS_CODES
    )
    raise ProviderError(
        f"OpenAI-compatible {context} failed: "
        f"{response.status_code} {message[:300]}",
        key_status,
        transient=transient,
    )


# A sentinel string that cannot occur in the serialized request JSON: it is
# injected as the video URL value and json.dumps emits it verbatim (no
# characters that get escaped), while no model name or prompt can contain
# it because the placeholder is placed as a standalone value and the
# prefix/suffix are split on its exact serialized form.
_VIDEO_PLACEHOLDER = "@@__UPLOADER_VIDEO_DATA_URL__@@"

_MAX_PLACEHOLDER_OCCURRENCES = 16


def _assert_placeholder_safe(prompt: str) -> None:
    """Guard against a prompt trying to smuggle the placeholder in."""
    if _VIDEO_PLACEHOLDER in prompt:
        raise ProviderError("Prompt must not contain the video placeholder", None)


def build_request_body(
    model: str, handle: "_VideoHandle", prompt: str
) -> dict[str, Any]:
    """The request JSON with the video URL replaced by a placeholder.

    ``_VIDEO_PLACEHOLDER`` marks where the base64 data URL goes; the
    streaming encoder writes the JSON around it verbatim. The placeholder
    cannot collide with the surrounding JSON: it appears exactly once (the
    prompt is guarded), and it is compared in its serialized form.
    """
    _assert_placeholder_safe(prompt)
    return {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "video_url", "video_url": {"url": _VIDEO_PLACEHOLDER}},
                ],
            }
        ],
        "response_format": {"type": "json_object"},
    }


class OpenAICompatibleProvider(AIProvider):
    """One custom provider instance; the base URL comes from its DB row."""

    api_type = "openai_compatible"

    def __init__(self, base_url: str):
        self._base_url = (base_url or "").rstrip("/")

    @property
    def base_url(self) -> str:
        return self._base_url

    # --- AIProvider API ------------------------------------------------

    async def upload_file(self, api_key: str, file_path: Path) -> Any:
        """Local preparation: the file must exist and fit the inline limit.

        No bytes are transferred here — the actual streaming happens in
        ``generate`` (OpenAI-compatible APIs have no upload endpoint).
        """
        try:
            file_size = file_path.stat().st_size
        except OSError as exc:
            raise ProviderError(f"Cannot read video file: {exc}", None) from exc
        max_inline = settings.ai_openai_max_inline_video_bytes
        if max_inline and file_size > max_inline:
            raise ProviderError(
                f"Video is {file_size} bytes; OpenAI-compatible providers "
                f"inline the video in the request body and are limited to "
                f"{max_inline} bytes",
                None,
            )
        return _VideoHandle(path=file_path, mime=_detect_mime(file_path))

    async def wait_file_ready(self, api_key: str, handle: Any) -> Any:
        # No asynchronous processing step: the handle is ready immediately.
        return handle

    async def generate(self, api_key: str, model: str, handle: Any, prompt: str) -> str:
        """Stream ONE chat/completions request with the inlined video.

        The JSON body is produced chunk-by-chunk: the base64 payload of
        each file chunk is encoded and yielded immediately, so memory use
        is bounded by one chunk (~4/3 of ``upload_read_chunk_size``) no
        matter how large the video is. The exact Content-Length is
        computed arithmetically, so httpx streams the async iterator
        instead of buffering it. The transfer is booked in the global
        upload memory limiter like any other upload.
        """
        if not isinstance(handle, _VideoHandle):
            handle = _VideoHandle(
                path=Path(str(handle)), mime=_detect_mime(Path(str(handle)))
            )
        file_size = handle.path.stat().st_size
        body = build_request_body(model, handle, prompt)
        prefix, suffix = self._split_body_around_video(body, handle)

        data_url_prefix = f"data:{handle.mime};base64,"
        content_length = (
            len(prefix)
            + len(data_url_prefix)
            + _b64_payload_size(file_size)
            + len(suffix)
        )

        # Read multiples of 3 bytes so every chunk except the last encodes
        # without interior base64 padding (see _encode_chunk).
        read_size = max(3, (settings.upload_read_chunk_size // 3) * 3)

        async def _stream() -> AsyncIterator[bytes]:
            # Everything is UTF-7-safe ASCII here except the prompt; the
            # prefix/suffix are already encoded as whole units.
            yield prefix
            yield data_url_prefix.encode("utf-8")
            async with aiofiles.open(handle.path, "rb") as f:
                while True:
                    chunk = await f.read(read_size)
                    if not chunk:
                        break
                    yield _encode_chunk(chunk)
            yield suffix

        client = get_client(None)
        booked = await upload_memory_limiter.acquire(file_size)
        try:
            response = await asyncio.wait_for(
                client.post(
                    f"{self._base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                        "Content-Length": str(content_length),
                        "Accept": "application/json",
                    },
                    content=_stream(),
                    timeout=httpx.Timeout(settings.ai_file_upload_timeout_seconds),
                ),
                timeout=settings.ai_file_upload_timeout_seconds,
            )
        except ProviderError:
            raise
        except Exception as exc:
            raise translate_api_error(exc) from exc
        finally:
            await upload_memory_limiter.release(booked)

        if response.status_code != 200:
            _raise_api_error(response, f"model {model}")

        return self._response_text(response)

    def _split_body_around_video(
        self, body: dict[str, Any], handle: _VideoHandle
    ) -> tuple[bytes, bytes]:
        """Serialize the request JSON with the video replaced by a
        placeholder, then split it into (prefix, suffix) around the
        placeholder."""
        serialized = json.dumps(body, ensure_ascii=False)
        before, placeholder, after = serialized.partition(_VIDEO_PLACEHOLDER)
        if not placeholder:
            # Impossible: the placeholder is our own sentinel.
            raise ProviderError("Internal error: video placeholder missing", None)
        return (
            before.encode("utf-8"),
            after.encode("utf-8"),
        )
    @staticmethod
    def _response_text(response: httpx.Response) -> str:
        """Concatenate the message content of a chat completion response."""
        try:
            data = response.json()
        except Exception as exc:
            raise ProviderError(
                f"OpenAI-compatible response is not JSON: {response.text[:300]}",
                None,
            ) from exc
        choices = data.get("choices") or []
        for choice in choices:
            message = choice.get("message") or {}
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content
            # Some servers return content as a list of parts.
            if isinstance(content, list):
                text = "".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict)
                )
                if text.strip():
                    return text
        raise ProviderError(
            "OpenAI-compatible response has no message content", None
        )

    # --- automatic model ordering ----------------------------------------

    def order_models_for_auto(self, models: list[str]) -> list[str]:
        """Order models by token cost, most costly first (heuristic).

        Custom providers may expose arbitrary model names, so the tiers are
        keyword-based: names with premium markers (``pro``, ``ultra``,
        ``max``, ``o1``, ``o3``) first, then unknown names, then cheap
        markers (``mini``, ``nano``, ``lite``, ``small``, ``flash``). The
        sort is stable, so models of one tier keep their stored order.
        """

        def rank(name: str) -> int:
            lowered = name.lower()
            if any(m in lowered for m in ("pro", "ultra", "max", "o1", "o3")):
                return 4
            if any(m in lowered for m in ("mini", "nano", "lite", "small", "flash")):
                return 1
            return 3

        return sorted(models, key=rank, reverse=True)
