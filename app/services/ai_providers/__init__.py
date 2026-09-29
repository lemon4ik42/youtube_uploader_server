"""Registry of AI provider implementations, one per supported API type."""

from __future__ import annotations

from app.services.ai_providers.base import AIProvider, ProviderError
from app.services.ai_providers.google_gemini import GoogleGeminiProvider

_PROVIDERS: dict[str, AIProvider] = {}

for _provider_cls in (GoogleGeminiProvider,):
    _provider = _provider_cls()
    _PROVIDERS[_provider.api_type] = _provider

#: every api_type a provider implementation exists for — the canonical list
#: of types the API and channel settings accept
SUPPORTED_API_TYPES: tuple[str, ...] = tuple(_PROVIDERS.keys())


def get_provider(api_type: str) -> AIProvider | None:
    """Return the provider implementation for ``api_type`` or None."""
    return _PROVIDERS.get(api_type)


async def close_ai_providers() -> None:
    """Release resources of every provider (application shutdown)."""
    for provider in _PROVIDERS.values():
        closer = getattr(provider, "close", None)
        if closer is not None:
            await closer()


__all__ = [
    "AIProvider",
    "ProviderError",
    "SUPPORTED_API_TYPES",
    "get_provider",
    "close_ai_providers",
]
