from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel

from app.models.project import utc_now


class BlockedVideoHash(SQLModel, table=True):
    """A globally reusable record of a copyright-blocked video file.

    This intentionally differs from ``RejectedVideoHash``: the file uploaded
    successfully, but YouTube later made it unavailable everywhere because of
    a copyright claim.  Channels opt in to skipping these hashes.
    """

    __tablename__ = "blocked_video_hash"
    __table_args__ = (
        UniqueConstraint("file_hash", name="uq_blocked_video_hash_file_hash"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    file_hash: str = Field(index=True)
    youtube_video_id: str | None = Field(default=None)
    reason: str = Field(default="copyright_blocked_all_countries")
    blocked_at: datetime = Field(default_factory=utc_now)
