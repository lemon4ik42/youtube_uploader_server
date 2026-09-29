from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

DEFAULT_SCOPE = "https://www.googleapis.com/auth/youtube"
DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"
DEFAULT_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
DEFAULT_API_UPLOAD = "https://www.googleapis.com/upload/youtube/v3/videos"


class ProjectCreate(BaseModel):
    name: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str = Field(default=DEFAULT_SCOPE)
    token_uri: str = Field(default=DEFAULT_TOKEN_URI)
    auth_uri: str = Field(default=DEFAULT_AUTH_URI)
    api_upload: str = Field(default=DEFAULT_API_UPLOAD)


class ProjectUpdate(BaseModel):
    name: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    redirect_uri: str | None = None
    scope: str | None = None
    token_uri: str | None = None
    auth_uri: str | None = None
    api_upload: str | None = None
    is_active: bool | None = None


class ProjectOut(BaseModel):
    id: UUID
    name: str
    client_id: str
    redirect_uri: str
    scope: str
    token_uri: str
    auth_uri: str
    api_upload: str
    quota_used: int
    quota_reset_at: datetime
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True
