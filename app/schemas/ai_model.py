from datetime import datetime
from urllib.parse import urlparse
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.models.ai_model import CUSTOM_PROVIDER_PREFIX
from app.services.ai_providers import SUPPORTED_API_TYPES

KEY_STATUS_UNKNOWN = "unknown"
KEY_STATUS_WORKING = "working"
KEY_STATUS_NOT_WORKING = "not_working"
KEY_STATUS_QUOTA_EXCEEDED = "quota_exceeded"
KEY_STATUSES = (
    KEY_STATUS_UNKNOWN,
    KEY_STATUS_WORKING,
    KEY_STATUS_NOT_WORKING,
    KEY_STATUS_QUOTA_EXCEEDED,
)


def _strip_non_empty_list(value: list[str]) -> list[str]:
    cleaned = [item.strip() for item in value if item and item.strip()]
    if not cleaned:
        raise ValueError("List must contain at least one non-empty item")
    return list(dict.fromkeys(cleaned))


def _strip_non_empty_name(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("Name must not be empty")
    return value


def validate_base_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base_url must use http or https and include a host")
    return value.rstrip("/")


class AITypeUpdate(BaseModel):
    """Upsert the settings of one API type. Omitted fields keep their values."""

    models: list[str] | None = Field(default=None, min_length=1)
    api_keys: list[str] | None = Field(default=None, min_length=1)
    is_active: bool | None = None

    _clean_list = field_validator("models", "api_keys")(_strip_non_empty_list)


class AIModelKeyOut(BaseModel):
    id: UUID
    key: str
    status: str
    last_checked_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class AITypeOut(BaseModel):
    api_type: str
    models: list[str]
    api_keys: list[AIModelKeyOut]
    is_active: bool


class AICustomProviderCreate(BaseModel):
    name: str
    base_url: str
    models: list[str] = Field(min_length=1)
    api_keys: list[str] = Field(min_length=1)
    is_active: bool = True

    _clean_list = field_validator("models", "api_keys")(_strip_non_empty_list)
    _validate_name = field_validator("name")(_strip_non_empty_name)
    _validate_base_url = field_validator("base_url")(validate_base_url)


class AICustomProviderUpdate(BaseModel):
    name: str | None = None
    base_url: str | None = None
    models: list[str] | None = Field(default=None, min_length=1)
    api_keys: list[str] | None = Field(default=None, min_length=1)
    is_active: bool | None = None

    _clean_list = field_validator("models", "api_keys")(_strip_non_empty_list)
    _validate_name = field_validator("name")(_strip_non_empty_name)
    _validate_base_url = field_validator("base_url")(validate_base_url)


class AICustomProviderOut(BaseModel):
    id: UUID
    name: str
    base_url: str
    models: list[str]
    api_keys: list[AIModelKeyOut]
    is_active: bool
    created_at: datetime
    updated_at: datetime
