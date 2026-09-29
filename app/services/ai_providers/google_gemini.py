"""Google Gemini provider implemented with the official ``google-genai`` SDK.

Every call goes through the SDK's asynchronous client (``genai.Client(...).aio``).
The video upload uses the SDK's resumable upload: the file is streamed to the
Gemini Files API in 8 MB chunks, and the transfer is booked in the global
upload memory limiter exactly like a YouTube upload, so concurrent AI and
YouTube transfers share one memory budget.

Error mapping (``google.genai.errors``):
- ``ClientError`` with code 429 (status ``RESOURCE_EXHAUSTED``) — quota
  exhausted for this API key -> ``quota_exceeded``.
- ``ClientError`` with code 400 and an "API key not valid" message (status
  ``INVALID_ARGUMENT``), or codes 401/403 (``UNAUTHENTICATED`` /
  ``PERMISSION_DENIED``) -> ``not_working``.
- ``ClientError`` 404 (model not found) says nothing about the key -> the
  stored key status is left unchanged. Any ``ServerError`` (5xx), network
  errors and timeouts are transient: the key status is left unchanged and
  the error is recorded in ``last_error`` (and retried later by the
  orchestrator).
"""

from __future__ import annotations

import asyncio
import mimetypes
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

import httpx
from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types

from app.core.config import settings
from app.core.logging import logger
from app.models.ai_model import (
    KEY_STATUS_NOT_WORKING,
    KEY_STATUS_QUOTA_EXCEEDED,
)
from app.services.ai_providers.base import AIProvider, ProviderError
from app.services.memory_limiter import upload_memory_limiter

# Cached async clients, one per API key, bounded like the proxy client cache.
_CLIENT_CACHE_CAP = 8


def translate_api_error(exc: Exception) -> ProviderError:
    """Map a google-genai SDK exception to a ProviderError.

    The returned ``key_status`` is set only for failures caused by the API
    key itself (quota, invalid key, permissions).
    """
    if isinstance(exc, genai_errors.ClientError):
        message = str(exc.message or exc)
        key_status: str | None = None
        if exc.code == 429:
            key_status = KEY_STATUS_QUOTA_EXCEEDED
        elif exc.code in (401, 403):
            key_status = KEY_STATUS_NOT_WORKING
        elif exc.code == 400 and "api key not valid" in message.lower():
            key_status = KEY_STATUS_NOT_WORKING
        return ProviderError(
            f"Gemini error {exc.code} {exc.status}: {message}", key_status
        )
    if isinstance(exc, genai_errors.APIError):
        # ServerError (5xx): retrying later can succeed.
        return ProviderError(
            f"Gemini error {exc.code} {exc.status}: {exc.message}",
            None,
            transient=True,
        )
    if isinstance(exc, asyncio.TimeoutError):
        return ProviderError(f"Gemini request timed out: {exc}", None, transient=True)
    if isinstance(exc, (httpx.HTTPError, OSError)):
        return ProviderError(f"Gemini network error: {exc}", None, transient=True)
    return ProviderError(f"Gemini request failed: {exc}", None, transient=True)


def _mime_type(file_path: Path) -> str:
    mime, _ = mimetypes.guess_type(file_path.name)
    return mime or "video/mp4"


def _file_state(file: Any) -> str:
    state = getattr(file, "state", None)
    value = getattr(state, "value", state)
    return str(value or "").upper()


class GoogleGeminiProvider(AIProvider):
    api_type = "google_gemini"

    def __init__(self) -> None:
        # One SDK client per API key. Created lazily, evicted
        # least-recently-used and closed via ``aio.aclose()``.
        self._clients: OrderedDict[str, genai.Client] = OrderedDict()

    async def _client(self, api_key: str) -> genai.Client:
        client = self._clients.get(api_key)
        if client is not None:
            self._clients.move_to_end(api_key)
            return client
        client = genai.Client(
            api_key=api_key,
            http_options=genai_types.HttpOptions(
                # Milliseconds; the upload is the longest request, everything
                # shorter is additionally bounded with asyncio.wait_for.
                timeout=int(settings.ai_file_upload_timeout_seconds * 1000)
            ),
        )
        self._clients[api_key] = client
        while len(self._clients) > _CLIENT_CACHE_CAP:
            _, evicted = self._clients.popitem(last=False)
            with _suppress_close_errors():
                await evicted.aio.aclose()
        return client

    async def close(self) -> None:
        """Close every cached SDK client (application shutdown)."""
        while self._clients:
            _, client = self._clients.popitem()
            with _suppress_close_errors():
                await client.aio.aclose()

    # --- AIProvider API ------------------------------------------------

    async def upload_file(self, api_key: str, file_path: Path) -> Any:
        """Upload the video to the Gemini Files API.

        The SDK streams the file in 8 MB chunks through a resumable upload
        session. The whole transfer is booked in the upload memory limiter
        first — the same accounting a YouTube upload uses — so the shared
        budget bounds how many large transfers run at once.
        """
        client = await self._client(api_key)
        file_size = file_path.stat().st_size
        booked = await upload_memory_limiter.acquire(file_size)
        try:
            return await asyncio.wait_for(
                client.aio.files.upload(
                    file=str(file_path),
                    config={
                        "mime_type": _mime_type(file_path),
                        "display_name": file_path.name,
                    },
                ),
                timeout=settings.ai_file_upload_timeout_seconds,
            )
        except (genai_errors.APIError, httpx.HTTPError, OSError, ValueError) as exc:
            raise translate_api_error(exc) from exc
        finally:
            await upload_memory_limiter.release(booked)

    async def wait_file_ready(self, api_key: str, handle: Any) -> Any:
        """Poll the Files API until the video finished processing."""
        client = await self._client(api_key)
        name = handle.name
        deadline = time.monotonic() + settings.ai_file_processing_timeout_seconds
        while True:
            try:
                file = await asyncio.wait_for(
                    client.aio.files.get(name=name),
                    timeout=settings.ai_request_timeout_seconds,
                )
            except (genai_errors.APIError, httpx.HTTPError, OSError) as exc:
                raise translate_api_error(exc) from exc
            state = _file_state(file)
            if state == "ACTIVE":
                return file
            if state == "FAILED":
                raise ProviderError(
                    "Gemini file processing failed (state FAILED)", None
                )
            if time.monotonic() >= deadline:
                raise ProviderError(
                    "Gemini file processing timed out", None
                )
            await asyncio.sleep(settings.ai_file_processing_poll_seconds)

    async def generate(self, api_key: str, model: str, handle: Any, prompt: str) -> str:
        """Run one model against the uploaded file; returns the raw text."""
        client = await self._client(api_key)
        file_uri = getattr(handle, "uri", None) or handle.name
        mime_type = getattr(handle, "mime_type", None) or "video/mp4"
        try:
            response = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=model,
                    contents=[
                        {
                            "role": "user",
                            "parts": [
                                {
                                    "file_data": {
                                        "file_uri": file_uri,
                                        "mime_type": mime_type,
                                    }
                                },
                                {"text": prompt},
                            ],
                        }
                    ],
                    config={"response_mime_type": "application/json"},
                ),
                timeout=settings.ai_request_timeout_seconds,
            )
        except (genai_errors.APIError, httpx.HTTPError, OSError) as exc:
            raise translate_api_error(exc) from exc

        return _response_text(response)

    # --- automatic model ordering ----------------------------------------

    def order_models_for_auto(self, models: list[str]) -> list[str]:
        """Order models by token cost, most costly first.

        Automatic selection looks for the first working model with the
        highest token expenditure. The tiers follow Gemini pricing:
        ``pro`` models are the most costly, then other/unknown premium
        names, then ``flash``, then ``flash-lite``. The sort is stable, so
        models of one tier keep their stored order.
        """
        def rank(name: str) -> int:
            lowered = name.lower()
            if "flash-lite" in lowered or "flash_lite" in lowered:
                return 1
            if "flash" in lowered:
                return 2
            if "pro" in lowered:
                return 4
            return 3

        return sorted(models, key=rank, reverse=True)


class _suppress_close_errors:
    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        if any(exc_info):
            logger.debug("Error while closing a Gemini SDK client: {}", exc_info)
        return True


def _response_text(response: Any) -> str:
    """Concatenate the text parts of a GenerateContentResponse."""
    parts = []
    for candidate in response.candidates or []:
        content = getattr(candidate, "content", None)
        for part in (getattr(content, "parts", None) or []):
            text = getattr(part, "text", None)
            if text:
                parts.append(text)
    return "".join(parts)
