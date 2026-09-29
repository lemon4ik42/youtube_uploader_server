"""inter_upload_delay_seconds bounds and the upload-limit batch trim.

* the delay is stored in seconds and accepts up to one week (604_800s);
  values above it are rejected by the schema on every settings endpoint
  (single-channel, batch-settings, bulk-settings);
* ``_load_active_tasks`` used ``max_tasks or base_limit``: an explicit
  ``max_tasks=0`` (upload limit exhausted) fell back to the unbounded base
  limit, so the limit trim was silently ignored when loading tasks.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import app.models  # noqa: F401
from app.core.config import Settings
from app.models.channel import Channel
from app.models.project import GCPProject
from app.models.upload_task import UploadTask
from app.services.coordinator import UploadCoordinator
from app.services.notification_service import NotificationService
from app.schemas.channel import (
    ChannelBatchSettingsUpdate,
    ChannelBulkSettingsUpdate,
    ChannelUpdate,
)


WEEK = 604_800


def test_delay_accepts_one_week_in_every_schema():
    assert ChannelUpdate(inter_upload_delay_seconds=WEEK)
    assert ChannelBatchSettingsUpdate(inter_upload_delay_seconds=WEEK)
    assert ChannelBulkSettingsUpdate(
        channel_ids=[uuid4()], inter_upload_delay_seconds=WEEK
    )


@pytest.mark.parametrize("schema", [ChannelUpdate, ChannelBatchSettingsUpdate])
def test_delay_above_one_week_rejected(schema):
    with pytest.raises(ValueError):
        schema(inter_upload_delay_seconds=WEEK + 1)


def test_bulk_delay_above_one_week_rejected():
    with pytest.raises(ValueError):
        ChannelBulkSettingsUpdate(
            channel_ids=[uuid4()], inter_upload_delay_seconds=WEEK + 1
        )


@pytest.mark.asyncio
async def test_load_active_tasks_zero_max_is_not_unbounded(tmp_path, monkeypatch):
    """max_tasks=0 must load ZERO tasks, not the full base limit."""
    settings = Settings(
        base_dir=str(tmp_path),
        video_base_dir="./videos",
        app_secret_key="x",
        api_token="x",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 't.db').as_posix()}",
    )
    engine = create_async_engine(settings.database_url, connect_args={"timeout": 30})
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    channel_id = uuid4()
    async with Session() as session:
        project = GCPProject(
            name="p", client_id="x", client_secret_enc="x",
            redirect_uri="http://localhost", scope="x", token_uri="x", auth_uri="x",
            api_upload="https://u",
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)
        channel = Channel(
            id=channel_id,
            youtube_channel_id="yc",
            title="t",
            refresh_token_enc="x",
            access_token_enc="x",
            access_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            folder_path=f"videos/{channel_id}",
            project_id=project.id,
            max_batch_size=10,
            min_batch_size=1,
            inter_upload_delay_seconds=0,
        )
        session.add(channel)
        for i in range(5):
            session.add(
                UploadTask(
                    channel_id=channel_id,
                    file_path=f"videos/{channel_id}/v{i}.mp4",
                    status="pending",
                )
            )
        await session.commit()
        await session.refresh(channel)

        coordinator = UploadCoordinator(NotificationService())

        # Limit exhausted: zero tasks must be loaded.
        tasks = await coordinator._load_active_tasks(
            session, channel, max_tasks=0
        )
        assert tasks == [], (
            "max_tasks=0 fell back to the base limit and loaded "
            f"{len(tasks)} task(s) although the upload limit is exhausted"
        )

        # Remaining=2: exactly two tasks.
        tasks = await coordinator._load_active_tasks(
            session, channel, max_tasks=2
        )
        assert len(tasks) == 2

        # None: the channel's batch size applies.
        tasks = await coordinator._load_active_tasks(session, channel)
        assert len(tasks) == 5

    await engine.dispose()
