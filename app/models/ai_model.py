from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import Index
from sqlmodel import Field, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# API key health, refreshed automatically on every request made with the key.
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

# Saved generation prompts, identified by type.
PROMPT_TYPE_TITLE = "title"
PROMPT_TYPE_DESCRIPTION = "description"
PROMPT_TYPES = (PROMPT_TYPE_TITLE, PROMPT_TYPE_DESCRIPTION)

#: channel ``ai_model`` values referencing a custom provider use this prefix
#: followed by the provider id, e.g. ``custom:<uuid>``
CUSTOM_PROVIDER_PREFIX = "custom:"


class AIModelConfig(SQLModel, table=True):
    """Per-API-type settings: the models and API keys of one API type.

    There is exactly one row per supported ``api_type``; its key rows live
    in ``ai_model_api_key``.
    """

    __tablename__ = "ai_model_config"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    api_type: str = Field(unique=True)
    # JSON-encoded list of model names (not secret).
    models_json: str
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AIModelApiKey(SQLModel, table=True):
    """One stored API key with its live health status."""

    __tablename__ = "ai_model_api_key"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    config_id: UUID = Field(foreign_key="ai_model_config.id", index=True)
    key_enc: str
    status: str = Field(default=KEY_STATUS_UNKNOWN, index=True)
    last_checked_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AIPrompt(SQLModel, table=True):
    """Saved generation prompt (title or description)."""

    __tablename__ = "ai_prompt"

    prompt_type: str = Field(primary_key=True)
    content: str
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AICustomProvider(SQLModel, table=True):
    """A user-defined OpenAI-compatible provider with its own base URL.

    Unlike the built-in API types (one settings row per ``api_type``),
    custom providers are multi-instance: every provider has its own name,
    base URL, model list and API keys. Channels reference them as
    ``custom:<provider_id>``.
    """

    __tablename__ = "ai_custom_provider"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str = Field(unique=True)
    base_url: str
    # JSON-encoded list of model names (not secret).
    models_json: str
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AICustomProviderApiKey(SQLModel, table=True):
    """One API key of a custom provider, with its live health status."""

    __tablename__ = "ai_custom_provider_api_key"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    provider_id: UUID = Field(foreign_key="ai_custom_provider.id", index=True)
    key_enc: str
    status: str = Field(default=KEY_STATUS_UNKNOWN, index=True)
    last_checked_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AIGenerationCache(SQLModel, table=True):
    """AI-generated metadata cached per video hash.

    At most MAX entries (see app.services.ai_cache) of unique
    (title, description) pairs are stored per file hash; channels with
    ``ai_use_cached`` pick a random entry once the maximum is reached.
    """

    __tablename__ = "ai_generation_cache"
    __table_args__ = (
        Index(
            "uq_ai_generation_cache_entry",
            "file_hash",
            "title",
            "description",
            unique=True,
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    file_hash: str = Field(index=True)
    title: str
    description: str = ""
    created_at: datetime = Field(default_factory=utc_now)
