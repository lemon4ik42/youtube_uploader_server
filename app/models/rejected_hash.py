from datetime import datetime
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel

from app.models.project import utc_now


class RejectedVideoHash(SQLModel, table=True):
    __tablename__ = "rejected_video_hash"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    file_hash: str = Field(unique=True, index=True)
    reason: str
    created_at: datetime = Field(default_factory=utc_now)
