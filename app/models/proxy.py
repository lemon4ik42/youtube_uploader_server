from datetime import datetime, timezone
from typing import TYPE_CHECKING, List
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from app.models.channel import Channel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Proxy(SQLModel, table=True):
    __tablename__ = "proxy"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    url_enc: str
    is_active: bool = Field(default=True, index=True)
    last_checked_at: datetime | None = None
    last_check_ok: bool | None = None
    status: str = Field(default="unknown", index=True)
    failure_count: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    channels: List["Channel"] = Relationship(back_populates="proxy")
