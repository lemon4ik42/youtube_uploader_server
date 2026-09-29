from datetime import datetime, timezone
from typing import TYPE_CHECKING, List
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from app.models.channel import Channel
    from app.models.channel_project_auth import ChannelProjectAuth


DEFAULT_SCOPE = "https://www.googleapis.com/auth/youtube"
DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"
DEFAULT_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
DEFAULT_API_UPLOAD = "https://www.googleapis.com/upload/youtube/v3/videos"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class GCPProject(SQLModel, table=True):
    __tablename__ = "gcp_project"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str
    client_id: str
    client_secret_enc: str

    # OAuth / API endpoints configured per project.
    redirect_uri: str
    scope: str = Field(default=DEFAULT_SCOPE)
    token_uri: str = Field(default=DEFAULT_TOKEN_URI)
    auth_uri: str = Field(default=DEFAULT_AUTH_URI)
    api_upload: str = Field(default=DEFAULT_API_UPLOAD)

    quota_used: int = Field(default=0)
    quota_reset_at: datetime = Field(default_factory=utc_now)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    channels: List["Channel"] = Relationship(back_populates="project")
    channel_auths: List["ChannelProjectAuth"] = Relationship(
        back_populates="project"
    )
