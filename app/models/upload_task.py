from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

from app.models.channel import Channel
from app.models.project import utc_now


class UploadBatch(SQLModel, table=True):
    __tablename__ = "upload_batch"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    channel_id: UUID = Field(foreign_key="channel.id", index=True)
    status: str = Field(default="running", index=True)
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: Optional[datetime] = None
    pause_until: Optional[datetime] = None
    error_summary: Optional[str] = None

    tasks: List["UploadTask"] = Relationship(back_populates="batch")


class UploadTask(SQLModel, table=True):
    __tablename__ = "upload_task"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    batch_id: Optional[UUID] = Field(default=None, foreign_key="upload_batch.id", index=True)
    channel_id: UUID = Field(foreign_key="channel.id", index=True)
    file_path: str
    file_hash: Optional[str] = Field(default=None, index=True)
    title: Optional[str] = None
    description: Optional[str] = None
    status: str = Field(default="pending", index=True)
    youtube_video_id: Optional[str] = None
    session_url: Optional[str] = None
    progress_percent: int = Field(default=0)
    error_message: Optional[str] = None
    attempt: int = Field(default=0)
    completed_at: Optional[datetime] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column_kwargs={"onupdate": utc_now},
    )

    batch: Optional["UploadBatch"] = Relationship(back_populates="tasks")
    channel: "Channel" = Relationship(back_populates="tasks")
