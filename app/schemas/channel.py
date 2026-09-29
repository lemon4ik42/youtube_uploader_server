from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.models.ai_model import CUSTOM_PROVIDER_PREFIX
from app.schemas.ai_model import SUPPORTED_API_TYPES
from app.schemas.tag import TagOut

AI_MODEL_AUTO = "auto"


def validate_ai_model_selection(value: str | None) -> str | None:
    """Normalize channel AI model selection: null, "auto", an API type name
    or a custom provider reference ("custom:<provider_id>")."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.lower() == AI_MODEL_AUTO:
        return AI_MODEL_AUTO
    if value.startswith(CUSTOM_PROVIDER_PREFIX):
        provider_id = value[len(CUSTOM_PROVIDER_PREFIX):]
        try:
            UUID(provider_id)
        except (ValueError, AttributeError, TypeError) as exc:
            raise ValueError(
                'ai_model "custom:" reference must be followed by a provider id'
            ) from exc
        return value
    if value not in SUPPORTED_API_TYPES:
        raise ValueError(
            'ai_model must be null, "auto", a supported API type or '
            '"custom:<provider_id>" (supported types: '
            + ", ".join(SUPPORTED_API_TYPES)
            + ")"
        )
    return value


class ChannelAuthStart(BaseModel):
    channel_id: UUID | None = None
    project_id: UUID | None = None
    redirect_uri: str | None = None
    channel_title: str | None = None
    folder_from_channel_id: UUID | None = None
    proxy_id: UUID | None = None
    proxy_auto_switch: bool = False


class ChannelAuthCallback(BaseModel):
    code: str
    state: str
    tag_ids: list[UUID] | None = None


class ChannelUpdate(BaseModel):
    title: str | None = None
    folder_from_channel_id: UUID | None = None
    title_template: str | None = None
    description_template: str | None = None
    privacy_status: str | None = Field(default=None, pattern="^(public|unlisted|private)$")
    default_category_id: str | None = None
    auto_upload_enabled: bool | None = None
    status: str | None = Field(default=None, pattern="^(active|paused|error)$")
    upload_limit_enabled: bool | None = None
    upload_limit_count: int | None = Field(default=None, ge=1)
    upload_limit_period_hours: int | None = Field(default=None, ge=1)
    max_batch_size: int | None = Field(default=None, ge=1, le=200)
    min_batch_size: int | None = Field(default=None, ge=1, le=200)
    inter_upload_delay_seconds: int | None = Field(
        default=None, ge=0, le=604800
    )
    shuffle_upload_order: bool | None = None
    skip_blocked_videos: bool | None = None
    proxy_id: UUID | None = None
    proxy_auto_switch: bool | None = None
    ai_model: str | None = None
    ai_generate_description: bool | None = None
    ai_use_cached: bool | None = None

    _validate_ai_model = field_validator("ai_model")(validate_ai_model_selection)


class ChannelUploadLimit(BaseModel):
    enabled: bool
    count: int = Field(ge=1)
    period_hours: int = Field(ge=1)
    uploads_in_current_period: int
    period_started_at: datetime | None
    period_ends_at: datetime | None
    is_limited: bool
    remaining: int
    reset_at: datetime | None


class ChannelUploadLimitUpdate(BaseModel):
    enabled: bool | None = None
    count: int | None = Field(default=None, ge=1)
    period_hours: int | None = Field(default=None, ge=1)


class ChannelUploadedCount(BaseModel):
    total_uploaded: int
    uploads_in_current_period: int
    period_hours: int
    is_limited: bool
    remaining: int | None
    reset_at: datetime | None


class ChannelUploadedVideosCount(BaseModel):
    channel_id: UUID
    uploaded_count: int


class ChannelOut(BaseModel):
    tags: list[TagOut] = Field(default_factory=list)
    id: UUID
    youtube_channel_id: str
    title: str
    youtube_title: str | None
    handle: str | None
    thumbnail_url: str | None
    subscriber_count: int | None
    view_count: int | None
    metadata_updated_at: datetime | None
    project_id: UUID | None
    pending_project_id: UUID | None
    folder_path: str | None
    title_template: str
    description_template: str
    privacy_status: str
    default_category_id: str | None
    auto_upload_enabled: bool
    status: str
    session_status: str
    session_status_changed_at: datetime | None
    paused_until: datetime | None
    pause_reason: str | None
    error_message: str | None
    last_upload_at: datetime | None
    upload_limit_enabled: bool
    upload_limit_count: int
    upload_limit_period_hours: int
    max_batch_size: int
    min_batch_size: int
    inter_upload_delay_seconds: int
    shuffle_upload_order: bool
    skip_blocked_videos: bool
    proxy_id: UUID | None
    proxy_auto_switch: bool
    proxy_recovery_pending: bool
    ai_model: str | None
    ai_generate_description: bool
    ai_use_cached: bool
    created_at: datetime
    updated_at: datetime

    @field_validator("default_category_id", mode="before")
    @classmethod
    def _empty_category_to_none(cls, value: str | None) -> str | None:
        if value == "" or value is None:
            return None
        return value

    class Config:
        from_attributes = True


class ChannelSessionStatusOut(BaseModel):
    session_status: str
    pending_project_id: UUID | None
    message: str | None


class FolderVideoOut(BaseModel):
    channel_id: UUID
    file_name: str
    size_bytes: int
    modified_at: datetime


class ChannelAvatarOut(BaseModel):
    thumbnail_url: str | None


class ChannelBatchSettings(BaseModel):
    max_batch_size: int = Field(ge=1, le=200)
    min_batch_size: int = Field(ge=1, le=200)
    inter_upload_delay_seconds: int = Field(ge=0, le=604800)
    shuffle_upload_order: bool = False
    skip_blocked_videos: bool = False


class ChannelBatchSettingsUpdate(BaseModel):
    max_batch_size: int | None = Field(default=None, ge=1, le=200)
    min_batch_size: int | None = Field(default=None, ge=1, le=200)
    inter_upload_delay_seconds: int | None = Field(
        default=None, ge=0, le=604800
    )
    shuffle_upload_order: bool | None = None
    skip_blocked_videos: bool | None = None


class ChannelBulkSettingsUpdate(BaseModel):
    channel_ids: list[UUID] = Field(min_length=1)
    max_batch_size: int | None = Field(default=None, ge=1, le=200)
    min_batch_size: int | None = Field(default=None, ge=1, le=200)
    inter_upload_delay_seconds: int | None = Field(
        default=None, ge=0, le=604800
    )
    shuffle_upload_order: bool | None = None
    skip_blocked_videos: bool | None = None
    upload_limit_enabled: bool | None = None
    upload_limit_count: int | None = Field(default=None, ge=1)
    upload_limit_period_hours: int | None = Field(default=None, ge=1)
    proxy_auto_switch: bool | None = None
    ai_model: str | None = None
    ai_generate_description: bool | None = None
    ai_use_cached: bool | None = None

    _validate_ai_model = field_validator("ai_model")(validate_ai_model_selection)


class ChannelBulkVideoSettingsUpdate(BaseModel):
    channel_ids: list[UUID] = Field(min_length=1)
    title_template: str | None = None
    description_template: str | None = None
    privacy_status: str | None = Field(default=None, pattern="^(public|unlisted|private)$")
    default_category_id: str | None = None


class ChannelBulkFolderUpdate(BaseModel):
    """Switch the video folder of several channels at once.

    ``folder_from_channel_id`` is required but nullable: an explicit ``null``
    switches every listed channel back to its own dedicated folder under
    VIDEO_BASE_DIR — the same semantics as the ``folder_from_channel_id``
    field of ``PUT /channels/{channel_id}``.
    """

    channel_ids: list[UUID] = Field(min_length=1)
    folder_from_channel_id: UUID | None


class ChannelBulkUpdateResult(BaseModel):
    updated: list[ChannelOut]
    not_found: list[UUID]
