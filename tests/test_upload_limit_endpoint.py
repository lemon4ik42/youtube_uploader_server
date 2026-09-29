"""Regression test: PUT /channels/{id}/upload-limit must not fail with
"Channel object has no field 'enabled'"."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_upload_limit_endpoint_maps_fields_correctly():
    """Call the real endpoint function with a stub service and check the mapping."""
    from app.api.routes.channels import update_channel_upload_limit
    from app.models.channel import Channel
    from app.schemas.channel import ChannelUploadLimitUpdate

    channel_id = uuid4()
    channel = SimpleNamespace(
        id=channel_id,
        status="active",
        session_status="active",
        upload_limit_enabled=False,
        upload_limit_count=50,
        upload_limit_period_hours=24,
    )

    # Stub ChannelService so .get returns our channel and .update records kwargs
    recorded = {}

    class StubService:
        def __init__(self, session):
            pass

        async def get(self, cid):
            return channel

        async def update(self, ch, **kwargs):
            recorded.update(kwargs)
            for k, v in kwargs.items():
                setattr(ch, k, v)
            return ch

    import app.api.routes.channels as routes
    monkeypatched = pytest.MonkeyPatch()
    monkeypatched.setattr(routes, "ChannelService", StubService)

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async with DummySession() as session:
        data = ChannelUploadLimitUpdate(enabled=True, count=25, period_hours=12)
        result = await update_channel_upload_limit(channel_id, data, session)

    # The endpoint must pass channel-column names to update(), not the public ones
    assert "enabled" not in recorded
    assert "count" not in recorded
    assert "period_hours" not in recorded
    assert recorded["upload_limit_enabled"] is True
    assert recorded["upload_limit_count"] == 25
    assert recorded["upload_limit_period_hours"] == 12
    assert channel.upload_limit_enabled is True

    monkeypatched.undo()
