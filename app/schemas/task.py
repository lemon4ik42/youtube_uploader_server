from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class TaskOut(BaseModel):
    id: UUID
    batch_id: UUID | None
    channel_id: UUID
    file_path: str
    file_hash: str | None
    title: str | None
    description: str | None
    status: str
    youtube_video_id: str | None
    progress_percent: int
    error_message: str | None
    attempt: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
