from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.models.upload_task import UploadTask
from app.schemas.task import TaskOut

router = APIRouter(dependencies=[Depends(verify_token)])

_ACTIVE_STATUSES = {"pending", "session_created", "uploading"}


@router.get("", response_model=list[TaskOut])
async def list_tasks(
    channel_id: UUID | None = None,
    status: str | None = Query(
        None,
        description="Comma-separated statuses or 'active'",
    ),
    active_only: bool = Query(False, alias="active"),
    limit: int | None = Query(None, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
) -> list[TaskOut]:
    query = select(UploadTask)

    if channel_id:
        query = query.where(UploadTask.channel_id == channel_id)

    if active_only or status == "active":
        query = query.where(UploadTask.status.in_(_ACTIVE_STATUSES))
    elif status:
        statuses = [s.strip() for s in status.split(",") if s.strip()]
        query = query.where(UploadTask.status.in_(statuses))

    query = query.order_by(UploadTask.created_at.desc())
    # Without a limit the full task history (potentially thousands of rows,
    # megabytes of JSON) is serialized and transferred on every request.
    if limit is not None:
        query = query.limit(limit)
    result = await session.exec(query)
    return list(result.scalars().all())


@router.get("/{task_id}", response_model=TaskOut)
async def get_task(
    task_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> TaskOut:
    task = await session.get(UploadTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
