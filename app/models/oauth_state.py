from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel

from app.models.project import utc_now


class OAuthState(SQLModel, table=True):
    __tablename__ = "oauth_state"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    state: str = Field(unique=True, index=True)
    project_id: Optional[UUID] = Field(
        default=None, foreign_key="gcp_project.id", index=True
    )
    channel_id: Optional[UUID] = Field(
        default=None, foreign_key="channel.id", index=True
    )
    code_verifier: str
    redirect_uri: str
    channel_title: str
    folder_from_channel_id: Optional[UUID] = Field(
        default=None, foreign_key="channel.id"
    )
    proxy_id: Optional[UUID] = Field(default=None, index=True)
    proxy_auto_switch: bool = Field(default=False)
    expires_at: datetime
    created_at: datetime = Field(default_factory=utc_now)
