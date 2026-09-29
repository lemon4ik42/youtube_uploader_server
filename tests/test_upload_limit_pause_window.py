"""Regression coverage for a full-quota upload-limit pause."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models.channel  # noqa: F401 -- registers SQLModel tables
import app.models.upload_task  # noqa: F401 -- registers SQLModel tables
from app.models.channel import Channel
from app.models.upload_task import UploadTask
from app.core.datetime_utils import ensure_aware
from app.services.channel_service import ChannelService


@pytest.mark.asyncio
async def test_limit_pause_waits_for_full_quota_and_reconciles_setting_changes(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'limits.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(SQLModel.metadata.create_all)
    Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    now = datetime.now(timezone.utc)
    async with Session() as session:
        channel = Channel(
            youtube_channel_id="limit-pause-test",
            title="Limit pause test",
            refresh_token_enc="refresh",
            access_token_enc="access",
            access_token_expires_at=now + timedelta(hours=1),
            upload_limit_enabled=True,
            upload_limit_count=2,
            upload_limit_period_hours=24,
        )
        session.add(channel)
        await session.commit()
        await session.refresh(channel)
        session.add_all(
            [
                UploadTask(
                    channel_id=channel.id,
                    file_path="first.mp4",
                    status="completed",
                    completed_at=now - timedelta(minutes=20),
                ),
                UploadTask(
                    channel_id=channel.id,
                    file_path="second.mp4",
                    status="completed",
                    completed_at=now - timedelta(minutes=10),
                ),
            ]
        )
        await session.commit()

        service = ChannelService(session)
        limit = await service.check_upload_limit(channel)
        assert limit["is_limited"] is True
        # Resume only after the most recent item of the consumed quota leaves
        # the window, so the next run can upload a full batch of two.
        assert abs(
            (limit["reset_at"] - (now + timedelta(hours=23, minutes=50))).total_seconds()
        ) < 2

        await service.pause_for_upload_limit(channel, limit["reset_at"])

        # A stale deadline or a manual attempt to resume must not bypass an
        # exhausted limit: it is replaced with the freshly calculated one.
        channel.paused_until = now - timedelta(minutes=1)
        channel.status = "paused"
        channel.pause_reason = "upload_limit"
        await session.commit()
        await service.unpause(channel)
        assert channel.status == "paused"
        assert channel.pause_reason == "upload_limit"
        assert ensure_aware(channel.paused_until) > now

        await service.update(channel, upload_limit_period_hours=1)
        assert channel.status == "paused"
        assert channel.pause_reason == "upload_limit"
        assert abs(
            (
                ensure_aware(channel.paused_until)
                - (now + timedelta(minutes=50))
            ).total_seconds()
        ) < 2

        # Raising the count makes room immediately and clears only the
        # automatic limit pause.
        await service.update(channel, upload_limit_count=3)
        assert channel.status == "active"
        assert channel.pause_reason is None
        assert channel.paused_until is None

    await engine.dispose()
