from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel

from app.models.project import utc_now

if TYPE_CHECKING:
    from app.models.channel import Channel


class UploadedVideoHash(SQLModel, table=True):
    __tablename__ = "uploaded_video_hash"
    __table_args__ = (
        UniqueConstraint("channel_id", "file_hash", name="uq_channel_file_hash"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    channel_id: UUID = Field(foreign_key="channel.id", index=True)
    file_hash: str = Field(index=True)
    youtube_video_id: str
    file_path: str
    uploaded_at: datetime = Field(default_factory=utc_now)

    channel: "Channel" = Relationship(back_populates="uploaded_hashes")
