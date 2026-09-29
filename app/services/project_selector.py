from datetime import date, datetime, timedelta, timezone
from typing import Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings
from app.core.logging import logger
from app.models.project import GCPProject

_PACIFIC = ZoneInfo("America/Los_Angeles")


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _pacific_date(dt: datetime) -> date:
    return dt.astimezone(_PACIFIC).date()


def _maybe_reset_quota(project: GCPProject) -> None:
    reset_at = _ensure_aware(project.quota_reset_at)
    project.quota_reset_at = reset_at
    today = _pacific_date(datetime.now(timezone.utc))
    reset_day = _pacific_date(reset_at)
    if today > reset_day:
        project.quota_used = 0
        project.quota_reset_at = datetime.now(timezone.utc)
        logger.info(
            "Quota reset for project {}",
            project.id,
        )


def _has_remaining_quota(project: GCPProject) -> bool:
    return project.quota_used < settings.youtube_quota_limit


async def select_project(
    session: AsyncSession,
    preferred_project_id: Optional[UUID] = None,
    exclude_project_id: Optional[UUID] = None,
    require_quota: bool = True,
) -> Optional[GCPProject]:
    if preferred_project_id:
        project = await session.get(GCPProject, preferred_project_id)
        if project and project.id != exclude_project_id:
            _maybe_reset_quota(project)
            if project.is_active and (not require_quota or _has_remaining_quota(project)):
                return project
        # A stale/inactive preferred project must not make reauthorization
        # impossible when another active project is available.

    result = await session.exec(
        select(GCPProject).where(GCPProject.is_active == True)  # noqa: E712
    )
    projects = list(result.scalars().all())
    for project in projects:
        _maybe_reset_quota(project)

    if projects:
        await session.commit()

    candidates = [
        p for p in projects
        if p.id != exclude_project_id and (not require_quota or _has_remaining_quota(p))
    ]
    if not candidates:
        return None

    # Pick the project with the most remaining quota (least used).
    return min(
        candidates,
        key=lambda p: (p.quota_used, _ensure_aware(p.quota_reset_at)),
    )
