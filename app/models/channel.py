from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import Index, text
from sqlmodel import Field, Relationship, SQLModel

from app.models.project import GCPProject, utc_now
from app.models.tag import ChannelTag, Tag

if TYPE_CHECKING:
    from app.models.channel_project_auth import ChannelProjectAuth
    from app.models.proxy import Proxy
    from app.models.upload_task import UploadBatch, UploadTask
    from app.models.uploaded_hash import UploadedVideoHash


class Channel(SQLModel, table=True):
    __tablename__ = "channel"
    __table_args__ = (
        Index(
            "uq_channel_youtube_id_active",
            "youtube_channel_id",
            unique=True,
            sqlite_where=text("status != 'deleted'"),
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    youtube_channel_id: str = Field(index=True)
    title: str
    project_id: Optional[UUID] = Field(
        default=None, foreign_key="gcp_project.id", index=True
    )
    refresh_token_enc: str
    access_token_enc: str
    access_token_expires_at: datetime
    folder_path: Optional[str] = Field(default=None)
    title_template: str = Field(default="{filename}")
    description_template: str = Field(default="")
    privacy_status: str = Field(default="public")
    default_category_id: Optional[str] = Field(default=None)
    auto_upload_enabled: bool = Field(default=True)
    status: str = Field(default="active", index=True)  # active, paused, error, deleted
    paused_until: Optional[datetime] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    last_upload_at: Optional[datetime] = Field(default=None)

    session_status: str = Field(default="active", index=True)  # active, expired, reauth_required
    session_status_changed_at: Optional[datetime] = Field(default=None)
    youtube_title: Optional[str] = Field(default=None)
    handle: Optional[str] = Field(default=None)
    thumbnail_url: Optional[str] = Field(default=None)
    subscriber_count: Optional[int] = Field(default=None)
    view_count: Optional[int] = Field(default=None)
    metadata_updated_at: Optional[datetime] = Field(default=None)
    pending_project_id: Optional[UUID] = Field(default=None, index=True)
    proxy_id: Optional[UUID] = Field(default=None, foreign_key="proxy.id", index=True)
    proxy_auto_switch: bool = Field(default=False)
    proxy_recovery_pending: bool = Field(default=False, index=True)

    # Configurable upload rate limits.
    upload_limit_enabled: bool = Field(default=False)
    upload_limit_count: int = Field(default=50)
    upload_limit_period_hours: int = Field(default=24)
    pause_reason: Optional[str] = Field(default=None, index=True)

    # Per-channel batch size and pacing.
    max_batch_size: int = Field(default=200)
    min_batch_size: int = Field(default=1)
    inter_upload_delay_seconds: int = Field(default=2)
    shuffle_upload_order: bool = Field(default=False)
    # When enabled, videos previously detected as copyright-blocked in every
    # country are excluded while collecting files for this channel.
    skip_blocked_videos: bool = Field(default=False)

    # AI metadata generation. ``ai_model`` is null (AI disabled), "auto"
    # (the server picks any active saved AI model configuration) or the id
    # of a saved ai_model_config row.
    ai_model: Optional[str] = Field(default=None)
    ai_generate_description: bool = Field(default=False)
    # Reuse cached generated metadata: once the maximum number of unique
    # entries is saved for the video hash, pick a random one instead of
    # generating new metadata.
    ai_use_cached: bool = Field(default=False)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    tags: List[Tag] = Relationship(
        link_model=ChannelTag,
        sa_relationship_kwargs={"lazy": "selectin", "order_by": "Tag.name"},
    )

    project: Optional["GCPProject"] = Relationship(back_populates="channels")
    tasks: List["UploadTask"] = Relationship(back_populates="channel")
    uploaded_hashes: List["UploadedVideoHash"] = Relationship(
        back_populates="channel"
    )
    project_auths: List["ChannelProjectAuth"] = Relationship(
        back_populates="channel"
    )
    proxy: Optional["Proxy"] = Relationship(back_populates="channels")
