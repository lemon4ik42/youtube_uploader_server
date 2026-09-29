"""Provider-neutral interface for AI metadata providers.

Every supported API type gets its own implementation in this package
(see ``google_gemini.py``). The orchestrator in
``app.services.ai_service`` picks the implementation by the stored
``api_type`` of an ``AIModelConfig``.
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Any

from app.models.ai_model import (
    KEY_STATUS_NOT_WORKING,
    KEY_STATUS_QUOTA_EXCEEDED,
)


class ProviderError(Exception):
    """A provider API call failed.

    ``key_status`` is the status the API key should get (None leaves the
    previous status unchanged — transient/model-level failures say nothing
    about the key itself).

    ``transient`` marks failures a later retry can survive: rate limits,
    temporary service overload, 5xx, network and timeout errors. Permanent
    model-level failures (model not found, unusable response) leave it
    False; key-level failures (quota/auth) are never transient.
    """

    def __init__(
        self, message: str, key_status: str | None = None, *, transient: bool = False
    ):
        super().__init__(message)
        self.message = message
        self.key_status = key_status
        self.transient = transient


class AIProvider(abc.ABC):
    """One AI API implementation (one ``api_type``)."""

    #: matches ``AIModelConfig.api_type`` values handled by this provider
    api_type: str = ""

    @abc.abstractmethod
    async def upload_file(self, api_key: str, file_path: Path) -> Any:
        """Upload the video and return a provider-specific file handle.

        Implementations must stream the file in chunks and book the
        transfer in the global upload memory limiter, like the YouTube
        uploader does.
        """

    @abc.abstractmethod
    async def wait_file_ready(self, api_key: str, handle: Any) -> Any:
        """Wait until the uploaded file is ready for generation.

        Returns the (possibly refreshed) handle. Raises ProviderError when
        processing fails or times out.
        """

    @abc.abstractmethod
    async def generate(self, api_key: str, model: str, handle: Any, prompt: str) -> str:
        """Run one model against the uploaded file.

        Returns the raw model text response. Raises ProviderError on API
        errors.
        """

    def order_models_for_auto(self, models: list[str]) -> list[str]:
        """Order models for automatic selection.

        The default keeps the stored order; providers with priced model
        tiers override this to try the most token-costly models first.
        """
        return list(models)


__all__ = [
    "AIProvider",
    "ProviderError",
    "KEY_STATUS_NOT_WORKING",
    "KEY_STATUS_QUOTA_EXCEEDED",
]
