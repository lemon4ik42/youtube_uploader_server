from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel

from app.models.project import utc_now

if TYPE_CHECKING:
    from app.models.channel import Channel
    from app.models.project import GCPProject


class ChannelProjectAuth(SQLModel, table=True):
    __tablename__ = "channel_project_auth"
    __table_args__ = (
        UniqueConstraint("channel_id", "project_id", name="uq_channel_project"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    channel_id: UUID = Field(foreign_key="channel.id", index=True)
    project_id: UUID = Field(foreign_key="gcp_project.id", index=True)
    refresh_token_enc: str
    access_token_enc: str
    access_token_expires_at: datetime
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    channel: "Channel" = Relationship(back_populates="project_auths")
    project: "GCPProject" = Relationship(back_populates="channel_auths")
