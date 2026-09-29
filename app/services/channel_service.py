import asyncio
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings
from app.core.datetime_utils import ensure_aware, iso_utc
from app.core.logging import logger
from app.core.security import decrypt, encrypt
from app.models.channel import Channel
from app.models.channel_project_auth import ChannelProjectAuth
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject
from app.models.proxy import Proxy
from app.models.upload_task import UploadTask
from app.models.uploaded_hash import UploadedVideoHash
from app.schemas.channel import ChannelOut
from app.schemas.task import TaskOut
from app.services.notification_service import notifications
from app.services.project_service import ProjectService, project_payload
from app.services.template_validation import validate_channel_templates
from app.services.youtube_auth import ChannelSessionExpiredError, refresh_access_token


def _channel_payload(channel: Channel) -> dict:
    return ChannelOut.model_validate(channel).model_dump(mode="json")


def _task_payload(task: UploadTask) -> dict:
    return TaskOut.model_validate(task).model_dump(mode="json")


def _dedicated_folder_path(channel_id: UUID) -> str:
    """Relative path of a channel's dedicated folder (VIDEO_BASE_DIR/<uuid>)."""
    return settings.relative_path(
        settings.resolve_path(settings.video_base_dir) / str(channel_id)
    )


async def find_folder_cycle(
    session: AsyncSession, channel: Channel, source: Channel
) -> list[UUID] | None:
    """Detect the circular folder reference an adoption would create.

    A channel "references" another channel when its ``folder_path`` is that
    other channel's dedicated folder (``VIDEO_BASE_DIR/<uuid>``). Letting
    ``channel`` adopt ``source``'s folder adds the link ``channel -> source``.
    This walks ``source``'s own chain of references and returns the list of
    channel ids that would form a cycle (starting and ending with the repeated
    id) — including the self-reference ``channel -> channel`` — or ``None``
    when the adoption is safe. The referenced channel's folder is never
    modified by an adoption; only cycles are rejected by the callers.
    """
    if source.id == channel.id:
        return [channel.id, channel.id]

    result = await session.exec(select(Channel).where(Channel.status != "deleted"))
    channels = list(result.scalars().all())
    by_id = {c.id: c for c in channels}
    # Owner map: dedicated folder path -> owning channel id.
    owner_by_path = {_dedicated_folder_path(c.id): c.id for c in channels}

    visited: list[UUID] = [channel.id]
    seen = {channel.id}
    current: Channel | None = source
    while current is not None:
        if current.id in seen:
            visited.append(current.id)
            return visited
        seen.add(current.id)
        visited.append(current.id)
        if not current.folder_path:
            return None
        owner_id = owner_by_path.get(current.folder_path)
        if owner_id is None or owner_id == current.id:
            # Own dedicated folder or a custom folder: the chain ends.
            return None
        current = by_id.get(owner_id)
    return None


# Resolved per-channel proxy URLs, cached briefly. The upload loop resolves
# the proxy before every chunk, so without a cache a full batch (hundreds of
# concurrent uploads) opens a database connection per chunk. That connection
# storm keeps SQLite busy for the whole batch and is what made unrelated
# endpoints — channel settings saves — take from seconds up to minutes while
# videos were uploading. Proxy changes invalidate the entry explicitly, so a
# switched proxy still applies to the very next chunk.
_proxy_url_cache: dict[UUID, tuple[float, str | None]] = {}
_PROXY_URL_CACHE_TTL_SECONDS = 5.0


def invalidate_channel_proxy_cache(channel_id: UUID | None = None) -> None:
    """Drop cached proxy resolutions so the next call re-reads the channel."""
    if channel_id is None:
        _proxy_url_cache.clear()
    else:
        _proxy_url_cache.pop(channel_id, None)


async def get_current_channel_proxy_url(channel_id: UUID) -> str | None:
    """Read the channel's current proxy without relying on an ORM cache.

    Results are served from a short-lived cache (see above). While automatic
    proxy recovery is in progress nothing is cached and the caller keeps
    waiting, exactly like before.
    """
    from app.core.db import async_session_maker

    cached = _proxy_url_cache.get(channel_id)
    if cached is not None and time.monotonic() < cached[0]:
        return cached[1]

    while True:
        async with async_session_maker() as session:
            channel = await session.get(Channel, channel_id)
            if channel is None or channel.status == "deleted":
                _proxy_url_cache.pop(channel_id, None)
                raise RuntimeError("Channel no longer exists")
            if not channel.proxy_recovery_pending:
                if channel.proxy_id is None:
                    proxy_url = None
                else:
                    proxy = await session.get(Proxy, channel.proxy_id)
                    if proxy is None or not proxy.is_active:
                        raise RuntimeError("Configured channel proxy is unavailable")
                    proxy_url = decrypt(proxy.url_enc)
                _proxy_url_cache[channel_id] = (
                    time.monotonic() + _PROXY_URL_CACHE_TTL_SECONDS,
                    proxy_url,
                )
                return proxy_url
        # Keep the worker alive without sending a request through direct mode
        # while automatic proxy recovery is still in progress.
        await asyncio.sleep(1)


class ChannelService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.project_service = ProjectService(session)

    async def list(self, include_deleted: bool = True) -> list[Channel]:
        query = select(Channel)
        if not include_deleted:
            query = query.where(Channel.status != "deleted")
        result = await self.session.exec(query)
        return list(result.scalars().all())

    async def get(self, channel_id: UUID) -> Channel | None:
        return await self.session.get(Channel, channel_id)

    async def get_proxy_url(self, channel: Channel) -> str | None:
        if not channel.proxy_id:
            return None
        proxy = await self.session.get(Proxy, channel.proxy_id)
        if not proxy:
            raise RuntimeError(f"Channel {channel.id} proxy not found")
        return decrypt(proxy.url_enc)

    async def get_proxy_settings(self, channel: Channel) -> dict:
        proxy_payload = None
        if channel.proxy_id:
            proxy = await self.session.get(Proxy, channel.proxy_id)
            if proxy:
                from app.services.proxy_service import ProxyService, proxy_payload as serialize_proxy

                proxy_payload = serialize_proxy(
                    proxy, await ProxyService(self.session).count(proxy.id)
                )
        return {
            "proxy_id": str(channel.proxy_id) if channel.proxy_id else None,
            "auto_switch": channel.proxy_auto_switch,
            "proxy": proxy_payload,
        }

    async def get_by_youtube_id(
        self, youtube_id: str, include_deleted: bool = False
    ) -> Channel | None:
        query = select(Channel).where(Channel.youtube_channel_id == youtube_id)
        if not include_deleted:
            query = query.where(Channel.status != "deleted")
        result = await self.session.exec(query)
        return result.scalars().first()

    async def create(
        self,
        youtube_channel_id: str,
        title: str,
        refresh_token: str,
        access_token: str,
        access_token_expires_at: datetime,
        project_id: UUID,
        folder_path: str | None = None,
        default_category_id: str | None = "",
        proxy_id: UUID | None = None,
        proxy_auto_switch: bool = False,
        youtube_title: str | None = None,
        handle: str | None = None,
        thumbnail_url: str | None = None,
        subscriber_count: int | None = None,
        view_count: int | None = None,
        metadata_updated_at: datetime | None = None,
    ) -> Channel:
        if default_category_id is None:
            default_category_id = ""
        if folder_path:
            folder_path = settings.relative_path(folder_path)
        channel = Channel(
            youtube_channel_id=youtube_channel_id,
            title=title,
            project_id=project_id,
            refresh_token_enc=refresh_token,
            access_token_enc=access_token,
            access_token_expires_at=access_token_expires_at,
            folder_path=folder_path,
            default_category_id=default_category_id,
            proxy_id=proxy_id,
            proxy_auto_switch=proxy_auto_switch,
            youtube_title=youtube_title,
            handle=handle,
            thumbnail_url=thumbnail_url,
            subscriber_count=subscriber_count,
            view_count=view_count,
            metadata_updated_at=metadata_updated_at,
        )
        validate_channel_templates(channel.title_template, channel.description_template)
        self.session.add(channel)
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.created", {"channel": _channel_payload(channel)}
        )
        return channel

    async def update(
        self, channel: Channel, *, commit: bool = True, **data: str | int | bool | None
    ) -> Channel:
        title_template = data.get("title_template", channel.title_template)
        description_template = data.get(
            "description_template", channel.description_template
        )
        validate_channel_templates(title_template, description_template)

        if "folder_path" in data and data["folder_path"]:
            data["folder_path"] = settings.relative_path(data["folder_path"])

        for key, value in data.items():
            if key == "default_category_id" and (value is None or value == ""):
                value = ""
            setattr(channel, key, value)
        # A proxy change must reach in-flight uploads on their next chunk,
        # not after the cache TTL expires.
        if "proxy_id" in data or "proxy_recovery_pending" in data:
            invalidate_channel_proxy_cache(channel.id)
        # Recalculate configurable upload limit when related settings change.
        # Do this before committing.  Queries flush the changed settings, so
        # the calculation also works for the single-transaction bulk update.
        limit_fields = {
            "upload_limit_enabled",
            "upload_limit_count",
            "upload_limit_period_hours",
        }
        limit_pause_changed = False
        limit_pause_cleared = False
        # A user can explicitly request ``status=active`` to remove a pause.
        # That must not bypass an upload limit which is still exhausted.  It
        # also repairs an old/stale pause deadline by calculating it again.
        reconcile_limit = bool(limit_fields & set(data.keys())) or (
            data.get("status") == "active"
        )
        if reconcile_limit:
            limit_info = await self.check_upload_limit(channel)
            if channel.status == "paused" and channel.pause_reason == "upload_limit":
                if not limit_info["is_limited"]:
                    channel.status = "active"
                    channel.paused_until = None
                    channel.pause_reason = None
                    channel.error_message = None
                    limit_pause_cleared = True
                else:
                    # A changed period can move the time at which the whole
                    # quota becomes available.  Never leave an old pause
                    # deadline behind after the limit settings are changed.
                    channel.paused_until = limit_info["reset_at"]
                    limit_pause_changed = True
            elif channel.status == "active":
                if limit_info["is_limited"]:
                    reset_at = limit_info["reset_at"]
                    channel.status = "paused"
                    channel.paused_until = reset_at
                    channel.pause_reason = "upload_limit"
                    limit_pause_changed = True

        if commit:
            await self.session.commit()
            await self.session.refresh(channel)

        if limit_pause_cleared:
            await notifications.broadcast(
                "channel.unpaused", {"channel_id": str(channel.id)}
            )
        elif limit_pause_changed:
            await notifications.broadcast(
                "channel.paused",
                {
                    "channel_id": str(channel.id),
                    "reason": "upload_limit",
                    "reset_at": (
                        channel.paused_until.isoformat()
                        if channel.paused_until else None
                    ),
                },
            )

        await notifications.broadcast(
            "channel.updated", {"channel": _channel_payload(channel)}
        )
        return channel

    async def delete(self, channel: Channel) -> None:
        """Hard-delete the channel and all related data."""
        from app.services.proxy_service import stop_channel_proxy_recovery

        await stop_channel_proxy_recovery(channel.id)
        invalidate_channel_proxy_cache(channel.id)
        channel_id = channel.id
        folder_path = channel.folder_path
        await self.session.exec(
            delete(UploadTask).where(UploadTask.channel_id == channel_id)
        )
        await self.session.exec(
            delete(UploadedVideoHash).where(UploadedVideoHash.channel_id == channel_id)
        )
        await self.session.exec(
            delete(ChannelProjectAuth).where(
                ChannelProjectAuth.channel_id == channel_id
            )
        )
        await self.session.delete(channel)
        await self.session.commit()

        # Decide whether the folder may be removed while the session is still
        # open (the count query needs it), but delete the files only after the
        # response is on its way: rmtree of a folder with thousands of video
        # files can take minutes on slow disks and must not hold the request.
        folder_to_remove = None
        if folder_path:
            resolved_folder = settings.resolve_path(folder_path)
            result = await self.session.exec(
                select(func.count(Channel.id)).where(
                    Channel.folder_path == folder_path,
                    Channel.id != channel_id,
                    Channel.status != "deleted",
                )
            )
            other_count = result.scalar_one()
            if other_count == 0:
                folder_to_remove = resolved_folder

        await notifications.broadcast(
            "channel.deleted", {"channel_id": str(channel_id)}
        )

        if folder_to_remove is not None:
            asyncio.get_running_loop().create_task(
                self._remove_folder(folder_to_remove, folder_path)
            )

    async def _remove_folder(self, resolved_folder: Path, folder_path: str) -> None:
        """Delete a channel folder in the background (best effort)."""
        try:
            await asyncio.to_thread(
                shutil.rmtree, resolved_folder, ignore_errors=False
            )
            logger.info("Deleted channel folder: {}", folder_path)
        except FileNotFoundError:
            logger.info("Channel folder already missing: {}", folder_path)
        except OSError as exc:
            logger.warning(
                "Failed to delete channel folder {}: {}", folder_path, exc
            )

    async def soft_delete(self, channel: Channel, message: str | None = None) -> None:
        """Mark channel as deleted on YouTube and cancel active tasks."""
        from app.services.coordinator import coordinator
        from app.services.proxy_service import stop_channel_proxy_recovery

        await stop_channel_proxy_recovery(channel.id)
        await coordinator.stop_channel_metadata(channel.id)
        invalidate_channel_proxy_cache(channel.id)
        channel.status = "deleted"
        channel.session_status = "expired"
        channel.session_status_changed_at = datetime.now(timezone.utc)
        channel.auto_upload_enabled = False
        channel.error_message = message or "Channel deleted on YouTube"
        await self.cancel_active_tasks(channel)
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.deleted", {"channel": _channel_payload(channel)}
        )

    async def set_session_status(
        self,
        channel: Channel,
        status: str,
        message: str | None = None,
    ) -> None:
        channel.session_status = status
        channel.session_status_changed_at = datetime.now(timezone.utc)
        if message is not None and status in ("expired", "error"):
            channel.error_message = message
        await self.session.commit()
        await self.session.refresh(channel)
        event = {
            "active": None,
            "expired": "channel.session_expired",
            "reauth_required": "channel.reauth_required",
        }.get(status)
        if event:
            payload: dict = {"channel": _channel_payload(channel)}
            if message:
                payload["message"] = message
            await notifications.broadcast(event, payload)

    async def set_reauth_required(
        self,
        channel: Channel,
        pending_project_id: UUID,
        authorization_url: str,
        expires_at: datetime | None = None,
    ) -> None:
        channel.pending_project_id = pending_project_id
        channel.session_status = "reauth_required"
        channel.session_status_changed_at = datetime.now(timezone.utc)
        await self.session.commit()
        await self.session.refresh(channel)
        payload: dict = {
            "channel": _channel_payload(channel),
            "pending_project_id": str(pending_project_id),
            "authorization_url": authorization_url,
            "proxy_settings": await self.get_proxy_settings(channel),
        }
        if expires_at is not None:
            payload["expires_at"] = iso_utc(expires_at)
            payload["expires_in_seconds"] = max(
                0,
                int(
                    (
                        ensure_aware(expires_at) - datetime.now(timezone.utc)
                    ).total_seconds()
                ),
            )
        await notifications.broadcast("channel.reauth_required", payload)

    async def request_reauthorization(self, channel: Channel, message: str) -> None:
        """Create one OAuth state and notify the client that reauth is needed."""
        if channel.status == "deleted" or channel.session_status == "reauth_required":
            return

        project = await self.session.get(GCPProject, channel.project_id)
        if project is None:
            await self.set_session_status(channel, "expired", message)
            return

        from app.services.youtube_auth import (
            _generate_code_challenge,
            _generate_code_verifier,
            build_authorization_url,
            generate_state,
        )

        verifier = _generate_code_verifier()
        state = generate_state()
        expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=settings.oauth_state_ttl_minutes
        )
        self.session.add(
            OAuthState(
                state=state,
                project_id=project.id,
                channel_id=channel.id,
                code_verifier=verifier,
                redirect_uri=project.redirect_uri,
                channel_title=channel.title or "",
                folder_from_channel_id=None,
                proxy_id=channel.proxy_id,
                proxy_auto_switch=channel.proxy_auto_switch,
                expires_at=expires_at,
            )
        )
        await self.session.commit()
        authorization_url = build_authorization_url(
            project,
            project.redirect_uri,
            state,
            _generate_code_challenge(verifier),
        )
        await self.set_reauth_required(
            channel, project.id, authorization_url, expires_at=expires_at
        )

    async def clear_reauth(self, channel: Channel) -> None:
        channel.pending_project_id = None
        await self.set_session_status(channel, "active")

    async def save_project_auth(
        self,
        channel: Channel,
        project_id: UUID,
        refresh_token: str,
        access_token: str,
        access_token_expires_at: datetime,
    ) -> None:
        result = await self.session.exec(
            select(ChannelProjectAuth).where(
                ChannelProjectAuth.channel_id == channel.id,
                ChannelProjectAuth.project_id == project_id,
            )
        )
        record = result.scalars().first()
        if record:
            record.refresh_token_enc = refresh_token
            record.access_token_enc = access_token
            record.access_token_expires_at = access_token_expires_at
            record.updated_at = datetime.now(timezone.utc)
        else:
            record = ChannelProjectAuth(
                channel_id=channel.id,
                project_id=project_id,
                refresh_token_enc=refresh_token,
                access_token_enc=access_token,
                access_token_expires_at=access_token_expires_at,
            )
            self.session.add(record)
        await self.session.commit()

    async def get_project_auth(
        self, channel_id: UUID, project_id: UUID
    ) -> ChannelProjectAuth | None:
        result = await self.session.exec(
            select(ChannelProjectAuth).where(
                ChannelProjectAuth.channel_id == channel_id,
                ChannelProjectAuth.project_id == project_id,
            )
        )
        return result.scalars().first()

    async def try_auth_with_saved_project(
        self, channel: Channel, project_id: UUID
    ) -> str | None:
        from app.services.youtube_auth import _do_refresh, ChannelSessionExpiredError

        auth = await self.get_project_auth(channel.id, project_id)
        if not auth:
            return None

        project = await self.session.get(GCPProject, project_id)
        if not project:
            return None

        now = datetime.now(timezone.utc)
        if now < ensure_aware(auth.access_token_expires_at) - timedelta(seconds=60):
            await self.session.commit()
            return decrypt(auth.access_token_enc)

        # End the read transactions from the lookups above before the network
        # refresh: it can hang for up to the HTTP timeout on a dead proxy.
        await self.session.commit()
        try:
            payload = await _do_refresh(
                decrypt(auth.refresh_token_enc), project, self.project_service,
                await get_current_channel_proxy_url(channel.id),
            )
        except ChannelSessionExpiredError:
            await self.session.delete(auth)
            await self.session.commit()
            return None
        except Exception:
            return None

        access_token = payload["access_token"]
        expires_in = payload.get("expires_in", 3600)
        auth.access_token_enc = encrypt(access_token)
        auth.access_token_expires_at = now + timedelta(seconds=expires_in)
        auth.updated_at = now
        await self.session.commit()

        if channel.project_id == project_id:
            channel.access_token_enc = auth.access_token_enc
            channel.refresh_token_enc = auth.refresh_token_enc
            channel.access_token_expires_at = auth.access_token_expires_at
            await self.session.commit()

        return access_token

    async def ensure_access_token(
        self, channel: Channel, *, request_reauth: bool = True
    ) -> str:
        if channel.session_status != "active":
            raise RuntimeError(
                f"Channel session is not active (status: {channel.session_status})"
            )

        project = await self.session.get(GCPProject, channel.project_id)
        if project is None:
            raise RuntimeError(f"Channel {channel.id} has no project")

        now = datetime.now(timezone.utc)
        if now < ensure_aware(channel.access_token_expires_at) - timedelta(seconds=60):
            # End the read transaction opened by the project lookup: callers
            # continue with network work (snippet refresh, upload batches), and
            # an open WAL snapshot blocks checkpoints for its whole duration.
            await self.session.commit()
            return decrypt(channel.access_token_enc)

        # Same here: the token refresh below can hang on a dead proxy for up
        # to the HTTP timeout (minutes), so no transaction may be open.
        await self.session.commit()
        try:
            access_token = await refresh_access_token(
                channel,
                project,
                self.project_service,
                await get_current_channel_proxy_url(channel.id),
            )
        except ChannelSessionExpiredError as exc:
            if not request_reauth:
                raise
            await self.request_reauthorization(channel, exc.message)
            raise RuntimeError(
                f"Channel session expired, re-authorization required: {exc.message}"
            ) from exc

        await self.session.commit()
        await self.session.refresh(channel)
        return access_token

    async def pause(
        self, channel: Channel, minutes: int, reason: str | None = None
    ) -> None:
        channel.status = "paused"
        channel.paused_until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        channel.pause_reason = reason or "manual"
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.paused",
            {
                "channel_id": str(channel.id),
                "reason": reason,
                "minutes": minutes,
            },
        )
        logger.info("Channel {} paused until {}", channel.id, channel.paused_until)

    async def unpause(self, channel: Channel) -> None:
        if channel.status == "deleted":
            raise RuntimeError("Cannot unpause a deleted channel")
        # ``POST /unpause`` reaches this method directly.  Do not let a
        # manual resume circumvent an exhausted configurable upload limit;
        # replace a stale deadline with the current limit deadline instead.
        if channel.upload_limit_enabled:
            limit_info = await self.check_upload_limit(channel)
            if limit_info["is_limited"]:
                await self.pause_for_upload_limit(channel, limit_info["reset_at"])
                return
        channel.status = "active"
        channel.paused_until = None
        channel.pause_reason = None
        channel.error_message = None
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.unpaused", {"channel": _channel_payload(channel)}
        )
        logger.info("Channel {} unpaused", channel.id)

    async def count_uploads_in_period(
        self, channel: Channel, since: datetime
    ) -> int:
        result = await self.session.exec(
            select(func.count(UploadTask.id)).where(
                UploadTask.channel_id == channel.id,
                UploadTask.status == "completed",
                UploadTask.completed_at >= since,
            )
        )
        return result.scalar_one()

    async def check_upload_limit(self, channel: Channel) -> dict:
        """Return current upload limit state for the channel.

        Uses a sliding window: uploads completed in the last
        ``upload_limit_period_hours`` are counted against
        ``upload_limit_count``.  When it is exhausted, ``reset_at`` is the
        time when *all* uploads currently in that window have expired.  This
        deliberately pauses a full quota batch for one full period, rather
        than resuming after one slot becomes free and immediately pausing
        again.
        """
        if not channel.upload_limit_enabled:
            return {
                "is_limited": False,
                "reset_at": None,
                "remaining": None,
                "period_start": None,
                "period_end": None,
                "uploads_in_period": 0,
            }

        period_hours = channel.upload_limit_period_hours or 24
        count_limit = channel.upload_limit_count or 0
        now = datetime.now(timezone.utc)
        period_start = now - timedelta(hours=period_hours)
        uploads = await self.count_uploads_in_period(channel, period_start)
        remaining = max(0, count_limit - uploads)
        is_limited = remaining <= 0

        reset_at = None
        if is_limited and uploads > 0:
            latest_result = await self.session.exec(
                select(UploadTask.completed_at)
                .where(
                    UploadTask.channel_id == channel.id,
                    UploadTask.status == "completed",
                    UploadTask.completed_at >= period_start,
                )
                .order_by(UploadTask.completed_at.desc())
                .limit(1)
            )
            latest = latest_result.scalar_one_or_none()
            if latest:
                reset_at = ensure_aware(latest) + timedelta(hours=period_hours)

        return {
            "is_limited": is_limited,
            "reset_at": reset_at,
            "remaining": remaining,
            "period_start": period_start,
            "period_end": now,
            "uploads_in_period": uploads,
        }

    async def get_uploaded_stats(
        self, channel: Channel
    ) -> dict:
        """Return uploaded-video statistics for the channel."""
        total_result = await self.session.exec(
            select(func.count(UploadTask.id)).where(
                UploadTask.channel_id == channel.id,
                UploadTask.status == "completed",
            )
        )
        total_uploaded = total_result.scalar_one()
        limit_info = await self.check_upload_limit(channel)
        return {
            "total_uploaded": total_uploaded,
            "uploads_in_current_period": limit_info["uploads_in_period"],
            "period_hours": channel.upload_limit_period_hours or 24,
            "is_limited": limit_info["is_limited"],
            "remaining": limit_info["remaining"],
            "reset_at": limit_info["reset_at"],
        }

    async def pause_for_upload_limit(
        self, channel: Channel, reset_at: datetime | None = None
    ) -> None:
        now = datetime.now(timezone.utc)
        if reset_at and reset_at > now:
            minutes = max(1, int((reset_at - now).total_seconds() // 60))
            paused_until = reset_at
        else:
            minutes = settings.pause_on_upload_limit_minutes
            paused_until = now + timedelta(minutes=minutes)
        channel.status = "paused"
        channel.paused_until = paused_until
        channel.pause_reason = "upload_limit"
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.paused",
            {
                "channel_id": str(channel.id),
                "reason": "upload_limit",
                "minutes": minutes,
                "reset_at": paused_until.isoformat(),
            },
        )
        logger.info(
            "Channel {} paused for upload limit until {}", channel.id, paused_until
        )

    async def clear_upload_limit_pause(self, channel: Channel) -> bool:
        """Unpause a channel if it was paused by the configurable upload limit.

        Returns True if the channel was unpaused.
        """
        if channel.status == "paused" and channel.pause_reason == "upload_limit":
            await self.unpause(channel)
            return True
        return False

    async def set_error(self, channel: Channel, message: str) -> None:
        channel.status = "error"
        channel.error_message = message
        await self.session.commit()
        await self.session.refresh(channel)
        await notifications.broadcast(
            "channel.error",
            {
                "channel": _channel_payload(channel),
                "error": message,
            },
        )
        logger.warning("Channel {} error: {}", channel.id, message)

    async def cancel_active_tasks(self, channel: Channel) -> None:
        active_statuses = {"pending", "session_created", "uploading"}
        result = await self.session.exec(
            select(UploadTask).where(
                UploadTask.channel_id == channel.id,
                UploadTask.status.in_(active_statuses),
            )
        )
        tasks = list(result.scalars().all())
        if not tasks:
            return
        for task in tasks:
            task.status = "cancelled"
            task.error_message = task.error_message or "Channel deleted"
            await notifications.broadcast(
                "task.cancelled",
                {
                    "task": _task_payload(task),
                    "reason": "channel_deleted",
                },
            )
        await self.session.commit()
        await notifications.broadcast(
            "tasks.cancelled",
            {
                "channel_id": str(channel.id),
                "count": len(tasks),
            },
        )

    async def cleanup_old_tasks(self, channel_id: UUID, max_tasks: int = 500) -> None:
        """Remove oldest completed/failed/cancelled tasks to keep DB clean."""
        count_result = await self.session.exec(
            select(func.count(UploadTask.id)).where(
                UploadTask.channel_id == channel_id
            )
        )
        count = count_result.scalar_one()
        if count <= max_tasks:
            return

        excess = count - max_tasks
        # One bulk DELETE instead of per-row ORM deletes: deleting thousands
        # of accumulated rows one by one used to hold SQLite's single write
        # lock for the whole loop, stalling unrelated saves.
        oldest_ids = select(UploadTask.id).where(
            UploadTask.channel_id == channel_id,
            UploadTask.status.in_(["completed", "failed", "cancelled"]),
        ).order_by(UploadTask.created_at.asc()).limit(excess)
        await self.session.exec(delete(UploadTask).where(UploadTask.id.in_(oldest_ids)))
        await self.session.commit()

    async def consume_project_quota(self, channel: Channel) -> None:
        project = await self.session.get(GCPProject, channel.project_id)
        if project is None:
            return
        project.quota_used += settings.youtube_upload_quota_cost
        await notifications.broadcast(
            "project.updated",
            {"project": project_payload(project)},
        )

    async def refresh_channel_snippet(self, channel: Channel, access_token: str) -> bool:
        """Refresh channel metadata/statistics from YouTube and broadcast updates.

        Returns True if thumbnail_url changed.
        """
        from app.services.youtube_auth import (
            ChannelSessionExpiredError,
            YouTubeTransientError,
            fetch_youtube_channel_info,
        )

        # The YouTube request below can hang on a dead proxy for up to the
        # HTTP timeout (minutes). End any read transaction the caller (or an
        # earlier query in this session) left open: a WAL snapshot held across
        # the call blocks checkpoints and starves the whole database.
        await self.session.commit()
        try:
            info = await fetch_youtube_channel_info(
                access_token, await get_current_channel_proxy_url(channel.id)
            )
        except Exception as exc:
            from app.services.youtube_auth import YouTubeChannelNotFoundError

            if isinstance(exc, YouTubeChannelNotFoundError):
                await self.soft_delete(channel, str(exc))
                return False
            if isinstance(exc, ChannelSessionExpiredError):
                await self.request_reauthorization(channel, exc.message)
                return False
            if isinstance(exc, YouTubeTransientError):
                # Usually a proxy whose IP YouTube is throttling or blocking.
                # Nothing about the channel changed, so keep the session as-is
                # and retry on the next refresh instead of dumping a traceback.
                logger.warning(
                    "Channel {} metadata refresh deferred: HTTP {} {} {}",
                    channel.id,
                    exc.status_code,
                    exc.reason or "-",
                    exc.message,
                )
                return False
            logger.exception("Failed to refresh channel {} snippet", channel.id)
            return False

        old_thumbnail = channel.thumbnail_url
        previous = {
            "youtube_title": channel.youtube_title,
            "handle": channel.handle,
            "thumbnail_url": channel.thumbnail_url,
            "subscriber_count": channel.subscriber_count,
            "view_count": channel.view_count,
        }
        current = {
            "youtube_title": info.get("title"),
            "handle": info.get("handle"),
            "thumbnail_url": info.get("thumbnail_url"),
            "subscriber_count": info.get("subscriber_count"),
            "view_count": info.get("view_count"),
        }
        changed_fields = [key for key, value in current.items() if previous[key] != value]
        changed = bool(changed_fields)
        channel.youtube_title = info.get("title")
        channel.handle = info.get("handle")
        channel.thumbnail_url = info.get("thumbnail_url")
        channel.subscriber_count = info.get("subscriber_count")
        channel.view_count = info.get("view_count")
        channel.metadata_updated_at = datetime.now(timezone.utc)
        if channel.youtube_title and channel.title == channel.youtube_channel_id:
            channel.title = channel.youtube_title
        await self.session.commit()
        await self.session.refresh(channel)
        if changed:
            await notifications.broadcast(
                "channel.updated", {"channel": _channel_payload(channel)}
            )
            await notifications.broadcast(
                "channel.metadata_updated",
                {
                    "channel_id": str(channel.id),
                    "changed_fields": changed_fields,
                    "metadata": {
                        "youtube_title": channel.youtube_title,
                        "handle": channel.handle,
                        "thumbnail_url": channel.thumbnail_url,
                        "subscriber_count": channel.subscriber_count,
                        "view_count": channel.view_count,
                        "metadata_updated_at": channel.metadata_updated_at.isoformat(),
                    },
                    "channel": _channel_payload(channel),
                },
            )
        if old_thumbnail != channel.thumbnail_url:
            await notifications.broadcast(
                "channel.avatar_updated",
                {
                    "channel_id": str(channel.id),
                    "thumbnail_url": channel.thumbnail_url,
                },
            )
            return True
        return False
