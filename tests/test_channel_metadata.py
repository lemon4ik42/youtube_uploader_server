from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_fetch_youtube_channel_info_includes_statistics(monkeypatch):
    import app.services.youtube_auth as youtube_auth

    payload = {
        "items": [
            {
                "id": "UC123",
                "snippet": {
                    "title": "Channel",
                    "customUrl": "@channel",
                    "thumbnails": {"high": {"url": "https://example/avatar.jpg"}},
                },
                "statistics": {
                    "subscriberCount": "12345",
                    "viewCount": "987654321",
                },
            }
        ]
    }
    response = SimpleNamespace(
        status_code=200,
        json=lambda: payload,
        raise_for_status=lambda: None,
    )
    request = AsyncMock(return_value=response)
    monkeypatch.setattr(youtube_auth, "proxy_request", request)

    info = await youtube_auth.fetch_youtube_channel_info("token")

    assert info["subscriber_count"] == 12345
    assert info["view_count"] == 987654321
    assert request.await_args.kwargs["params"]["part"] == "snippet,statistics"


@pytest.mark.asyncio
async def test_fetch_youtube_channel_info_allows_hidden_subscribers(monkeypatch):
    import app.services.youtube_auth as youtube_auth

    payload = {
        "items": [
            {
                "id": "UC123",
                "snippet": {"title": "Channel", "thumbnails": {}},
                "statistics": {"viewCount": "42", "hiddenSubscriberCount": True},
            }
        ]
    }
    response = SimpleNamespace(
        status_code=200,
        json=lambda: payload,
        raise_for_status=lambda: None,
    )
    monkeypatch.setattr(youtube_auth, "proxy_request", AsyncMock(return_value=response))

    info = await youtube_auth.fetch_youtube_channel_info("token")

    assert info["subscriber_count"] is None
    assert info["view_count"] == 42
