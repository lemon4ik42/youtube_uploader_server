import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.core.config import settings
from app.core.datetime_utils import iso_utc
from app.core.logging import logger
from app.core.security import decrypt, encrypt
from app.models.ai_model import CUSTOM_PROVIDER_PREFIX
from app.models.channel import Channel
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject
from app.models.proxy import Proxy
from app.models.tag import ChannelTag, Tag
from app.schemas.channel import (
    ChannelAuthCallback,
    ChannelAuthStart,
    ChannelAvatarOut,
    ChannelBatchSettings,
    ChannelBatchSettingsUpdate,
    ChannelBulkSettingsUpdate,
    ChannelBulkUpdateResult,
    ChannelBulkFolderUpdate,
    ChannelBulkVideoSettingsUpdate,
    ChannelOut,
    ChannelSessionStatusOut,
    ChannelUpdate,
    ChannelUploadLimit,
    ChannelUploadLimitUpdate,
    ChannelUploadedCount,
    ChannelUploadedVideosCount,
    FolderVideoOut,
)
from app.schemas.proxy import ChannelProxyUpdate
from app.services.channel_service import ChannelService
from app.services.coordinator import coordinator
from app.services.notification_service import notifications
from app.services.project_selector import _maybe_reset_quota, select_project
from app.services.project_service import ProjectService
from app.services.youtube_auth import (
    ChannelSessionExpiredError,
    YouTubeChannelNotFoundError,
    _generate_code_challenge,
    _generate_code_verifier,
    build_authorization_url,
    exchange_code_for_tokens,
    fetch_youtube_channel_info,
    generate_state,
)
from app.services.proxy_service import (
    ProxyService,
    consume_proxy_reservation,
    stop_channel_proxy_recovery,
)

router = APIRouter(dependencies=[Depends(verify_token)])


@router.post("/auth/start")
async def start_channel_auth(
    data: ChannelAuthStart,
    session: AsyncSession = Depends(get_session),
) -> dict:
    project_service = ProjectService(session)
    projects = await project_service.list()
    if not projects:
        raise HTTPException(
            status_code=400,
            detail="No GCP projects configured. Add a project first.",
        )

    existing_channel: Channel | None = None
    if data.channel_id:
        existing_channel = await session.get(Channel, data.channel_id)
        if not existing_channel:
            raise HTTPException(status_code=404, detail="Channel not found")
        if existing_channel.status == "deleted":
            raise HTTPException(status_code=400, detail="Channel is deleted")
        if existing_channel.session_status == "active":
            if existing_channel.proxy_recovery_pending:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Channel is waiting for a working proxy; retry later",
                )
            channel_service = ChannelService(session)
            # Best-effort pre-check: when YouTube confirms the session is
            # still active, the client gets a 409 instead of a pointless
            # re-authorization form. It used to run inline with no deadline,
            # so a dead proxy held this endpoint for up to the HTTP timeout
            # (minutes) before the user ever saw the authorization link. Now
            # the check is bounded by OAUTH_SESSION_PRECHECK_TIMEOUT_SECONDS;
            # when YouTube is unreachable the link is issued anyway —
            # re-authorization is harmless, the callback re-validates the
            # channel and replaces the tokens.
            precheck_timeout = settings.oauth_session_precheck_timeout_seconds
            if precheck_timeout > 0:
                try:
                    proxy_url = await channel_service.get_proxy_url(existing_channel)
                except Exception:
                    # Missing proxy row: cannot verify, but the OAuth flow
                    # itself does not need it — issue the link.
                    proxy_url = None
                # End the read transaction opened by the channel/proxy
                # lookups before the (bounded) network verification: an
                # open WAL snapshot must not live across network calls.
                await session.commit()

                async def _verify_session() -> None:
                    access_token = await channel_service.ensure_access_token(
                        existing_channel, request_reauth=False
                    )
                    await fetch_youtube_channel_info(access_token, proxy_url)

                try:
                    await asyncio.wait_for(_verify_session(), timeout=precheck_timeout)
                except ChannelSessionExpiredError as exc:
                    await channel_service.set_session_status(
                        existing_channel, "expired", exc.message
                    )
                except YouTubeChannelNotFoundError as exc:
                    await channel_service.soft_delete(existing_channel, str(exc))
                    raise HTTPException(
                        status_code=410,
                        detail="YouTube channel no longer exists",
                    ) from exc
                except asyncio.TimeoutError:
                    # A half-finished token refresh may have left the request
                    # session in a transaction; drop it before issuing state.
                    await session.rollback()
                    logger.warning(
                        "Channel {}: session pre-check timed out after {}s; "
                        "issuing the authorization link anyway",
                        existing_channel.id,
                        precheck_timeout,
                    )
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "Channel {}: session pre-check failed ({}); "
                        "issuing the authorization link anyway",
                        existing_channel.id,
                        exc,
                    )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="YouTube confirmed that the channel session is still active",
                    )

    if not data.channel_id and not data.channel_title:
        raise HTTPException(
            status_code=400,
            detail="channel_title is required for new channels",
        )

    if data.folder_from_channel_id:
        source_channel = await session.get(Channel, data.folder_from_channel_id)
        if not source_channel:
            raise HTTPException(
                status_code=404, detail="Source channel for folder not found"
            )
        if existing_channel:
            from app.services.channel_service import find_folder_cycle

            cycle = await find_folder_cycle(
                session, existing_channel, source_channel
            )
            if cycle:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Circular video folder reference: "
                        + " -> ".join(str(cid) for cid in cycle)
                        + ". The referenced channel's folder must not lead "
                        "back to this channel."
                    ),
                )

    if data.proxy_id:
        proxy = await session.get(Proxy, data.proxy_id)
        if not proxy or not proxy.is_active:
            raise HTTPException(status_code=400, detail="Proxy is not active or does not exist")

    exclude_project_id = None
    preferred_project_id = None
    # Reaching this point with an "active" session means the pre-check could
    # not verify it (timeout, unreachable YouTube, or pre-check disabled):
    # the user explicitly asked to re-authorize, so the flow must work even
    # when this is the only configured project or its quota is exhausted —
    # the same semantics as a session that is already expired.
    reauthorizing = existing_channel is not None
    if existing_channel and not data.project_id:
        if reauthorizing:
            # Re-authorization must be possible even when this is the only
            # configured project or its upload quota is exhausted.
            preferred_project_id = (
                existing_channel.pending_project_id or existing_channel.project_id
            )
        else:
            # Quota failover prefers a different project for an active session.
            exclude_project_id = existing_channel.project_id

    project = None
    if data.project_id:
        project = await session.get(GCPProject, data.project_id)
        if not project or not project.is_active:
            raise HTTPException(
                status_code=400,
                detail="Requested GCP project is not active or does not exist",
            )
        _maybe_reset_quota(project)

    if not project:
        # New channels can be authorized on any active project; quota is only
        # consumed during uploads, not when adding a channel.
        project = await select_project(
            session,
            preferred_project_id=preferred_project_id,
            exclude_project_id=exclude_project_id,
            require_quota=bool(existing_channel) and not reauthorizing,
        )
        if not project:
            raise HTTPException(
                status_code=400,
                detail="No suitable active GCP project found",
            )

    redirect_uri = data.redirect_uri or project.redirect_uri
    verifier = _generate_code_verifier()
    challenge = _generate_code_challenge(verifier)
    state = generate_state()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=settings.oauth_state_ttl_minutes)

    auth_proxy_id = data.proxy_id or (existing_channel.proxy_id if existing_channel else None)
    auth_proxy_auto_switch = (
        data.proxy_auto_switch
        if data.proxy_id is not None or not existing_channel
        else existing_channel.proxy_auto_switch
    )
    oauth_state = OAuthState(
        state=state,
        project_id=project.id,
        channel_id=data.channel_id,
        code_verifier=verifier,
        redirect_uri=redirect_uri,
        channel_title=data.channel_title or existing_channel.title or "",
        folder_from_channel_id=data.folder_from_channel_id,
        proxy_id=auth_proxy_id,
        proxy_auto_switch=auth_proxy_auto_switch,
        expires_at=expires_at,
    )
    session.add(oauth_state)
    await session.commit()

    authorization_url = build_authorization_url(
        project,
        redirect_uri,
        state,
        challenge,
    )

    selected_proxy = None
    selected_proxy_id = auth_proxy_id
    proxy_auto_switch = (
        auth_proxy_auto_switch
    )
    if selected_proxy_id:
        selected_proxy = await session.get(Proxy, selected_proxy_id)
    proxy_settings = {
        "proxy_id": str(selected_proxy_id) if selected_proxy_id else None,
        "auto_switch": proxy_auto_switch,
        "proxy": None,
    }
    if selected_proxy:
        from app.services.proxy_service import proxy_payload

        proxy_settings["proxy"] = proxy_payload(
            selected_proxy, await ProxyService(session).count(selected_proxy.id)
        )

    payload = {
        "state": state,
        "authorization_url": authorization_url,
        "project_id": str(project.id),
        "proxy_settings": proxy_settings,
        "expires_at": iso_utc(expires_at),
        "expires_in_seconds": max(
            0, int((expires_at - datetime.now(timezone.utc)).total_seconds())
        ),
    }
    if data.channel_id:
        payload["channel_id"] = str(data.channel_id)
    else:
        payload["channel_title"] = data.channel_title

    await notifications.broadcast(
        "channel.auth_url",
        {
            "state": state,
            "authorization_url": authorization_url,
            "channel_title": data.channel_title,
            "project_id": str(project.id),
            "channel_id": str(data.channel_id) if data.channel_id else None,
            "proxy_settings": proxy_settings,
            "expires_at": payload["expires_at"],
            "expires_in_seconds": payload["expires_in_seconds"],
        },
    )

    return payload


@router.post("/auth/callback", response_model=ChannelOut)
async def channel_auth_callback(
    data: ChannelAuthCallback,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    now = datetime.now(timezone.utc)
    result = await session.exec(
        select(OAuthState).where(OAuthState.state == data.state)
    )
    oauth_state = result.scalars().first()
    if not oauth_state:
        raise HTTPException(status_code=400, detail="Invalid or expired state")
    from app.core.datetime_utils import ensure_aware
    if ensure_aware(oauth_state.expires_at) < now:
        raise HTTPException(status_code=400, detail="Invalid or expired state")

    project = await session.get(GCPProject, oauth_state.project_id)
    if not project:
        raise HTTPException(status_code=400, detail="Project not found")

    # Validate before consuming the one-time OAuth code. Apply only after
    # authorization succeeds; omitted/null preserves reauthorization tags.
    tag_ids = set(data.tag_ids) if data.tag_ids is not None else None
    if tag_ids:
        found = (await session.exec(select(Tag.id).where(Tag.id.in_(tag_ids)))).scalars().all()
        if set(found) != tag_ids:
            raise HTTPException(status_code=404, detail="Tag not found")

    project_service = ProjectService(session)
    try:
        proxy_url = None
        # Prefer the proxy selected for this OAuth flow. This matters when the
        # client explicitly changes the proxy while reauthorizing a channel.
        if oauth_state.proxy_id:
            proxy = await session.get(Proxy, oauth_state.proxy_id)
            proxy_url = decrypt(proxy.url_enc) if proxy and proxy.is_active else None
        if not proxy_url and oauth_state.channel_id:
            existing_for_auth = await session.get(Channel, oauth_state.channel_id)
            if existing_for_auth:
                proxy_url = await ChannelService(session).get_proxy_url(existing_for_auth)
        tokens = await exchange_code_for_tokens(
            data.code,
            oauth_state.redirect_uri,
            oauth_state.code_verifier,
            project,
            project_service,
            proxy_url,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Token exchange failed: {exc}",
        ) from exc

    access_token = tokens["access_token"]
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise HTTPException(
            status_code=400,
            detail="No refresh token returned. Ensure 'prompt=consent' was used.",
        )

    try:
        info = await fetch_youtube_channel_info(access_token, proxy_url)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch YouTube channel info: {exc}",
        ) from exc

    youtube_id = info["id"]
    youtube_title = info.get("title") or ""
    handle = info.get("handle")
    thumbnail_url = info.get("thumbnail_url")
    subscriber_count = info.get("subscriber_count")
    view_count = info.get("view_count")

    channel_service = ChannelService(session)
    duplicate = await channel_service.get_by_youtube_id(youtube_id)

    if oauth_state.channel_id:
        channel = await session.get(Channel, oauth_state.channel_id)
        if not channel:
            raise HTTPException(status_code=404, detail="Channel not found")
        if duplicate and duplicate.id != channel.id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="This YouTube channel is already connected to another record",
            )
        try:
            channel = await channel_service.update(
                channel,
                project_id=project.id,
                refresh_token_enc=encrypt(refresh_token),
                access_token_enc=encrypt(access_token),
                access_token_expires_at=now + timedelta(seconds=tokens.get("expires_in", 3600)),
                youtube_channel_id=youtube_id,
                title=youtube_title or channel.title,
                youtube_title=youtube_title or None,
                handle=handle,
                thumbnail_url=thumbnail_url,
                subscriber_count=subscriber_count,
                view_count=view_count,
                metadata_updated_at=now,
                status="active",
                session_status="active",
                session_status_changed_at=now,
                pending_project_id=None,
                error_message=None,
                paused_until=None,
                proxy_id=oauth_state.proxy_id,
                proxy_auto_switch=oauth_state.proxy_auto_switch,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    else:
        if duplicate:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="This YouTube channel is already connected",
            )
        try:
            channel = await channel_service.create(
                youtube_channel_id=youtube_id,
                title=youtube_title or oauth_state.channel_title,
                refresh_token=encrypt(refresh_token),
                access_token=encrypt(access_token),
                access_token_expires_at=now + timedelta(seconds=tokens.get("expires_in", 3600)),
                project_id=project.id,
                proxy_id=oauth_state.proxy_id,
                proxy_auto_switch=oauth_state.proxy_auto_switch,
                youtube_title=youtube_title or None,
                handle=handle,
                thumbnail_url=thumbnail_url,
                subscriber_count=subscriber_count,
                view_count=view_count,
                metadata_updated_at=now,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    # Resolve folder path
    if oauth_state.folder_from_channel_id:
        source_channel = await session.get(Channel, oauth_state.folder_from_channel_id)
        cycle = None
        if source_channel and source_channel.folder_path:
            from app.services.channel_service import find_folder_cycle

            cycle = await find_folder_cycle(session, channel, source_channel)
        if cycle:
            # The folder graph changed between /auth/start and this callback.
            # The tokens are already exchanged, so instead of failing the whole
            # authorization the channel keeps its current folder (or gets its
            # dedicated one) and the adoption is skipped with a warning.
            logger.warning(
                "Channel {}: ignoring folder_from_channel_id {} — circular "
                "folder reference {}",
                channel.id,
                oauth_state.folder_from_channel_id,
                " -> ".join(str(cid) for cid in cycle),
            )
            if not channel.folder_path:
                folder = settings.resolve_path(settings.video_base_dir) / str(channel.id)
                folder.mkdir(parents=True, exist_ok=True)
                channel.folder_path = settings.relative_path(folder)
        elif source_channel and source_channel.folder_path:
            channel.folder_path = source_channel.folder_path
    else:
        folder = settings.resolve_path(settings.video_base_dir) / str(channel.id)
        folder.mkdir(parents=True, exist_ok=True)
        channel.folder_path = settings.relative_path(folder)
    await session.commit()
    await session.refresh(channel)

    await channel_service.save_project_auth(
        channel,
        project.id,
        encrypt(refresh_token),
        encrypt(access_token),
        now + timedelta(seconds=tokens.get("expires_in", 3600)),
    )

    if tag_ids is not None:
        await session.exec(delete(ChannelTag).where(ChannelTag.channel_id == channel.id))
        session.add_all([
            ChannelTag(channel_id=channel.id, tag_id=tag_id)
            for tag_id in tag_ids
        ])
    await session.delete(oauth_state)
    await session.commit()
    await session.refresh(channel)

    await notifications.broadcast(
        "channel.connected",
        {"channel": ChannelOut.model_validate(channel).model_dump(mode="json")},
    )

    # Wake the worker so it can resume pending uploads with the new credentials.
    await coordinator.wake_channel(channel.id)
    # OAuth already fetched and persisted current metadata above. Start the
    # per-channel async refresher; its next API request runs after the interval.
    await coordinator.start_channel_metadata(
        channel.id,
        refresh_now=False,
        initial_delay=settings.channel_snippet_refresh_interval_seconds,
    )

    return channel


@router.put("/{channel_id}/proxy", response_model=ChannelOut)
async def update_channel_proxy(
    channel_id: UUID,
    data: ChannelProxyUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    values = data.model_dump(exclude_unset=True)
    if values.get("proxy_id"):
        proxy = await session.get(Proxy, values["proxy_id"])
        if not proxy or not proxy.is_active:
            raise HTTPException(status_code=400, detail="Proxy is not active or does not exist")
    if "auto_switch" in values:
        values["proxy_auto_switch"] = values.pop("auto_switch")
    if "proxy_id" in values:
        values["proxy_recovery_pending"] = False
        await stop_channel_proxy_recovery(channel_id)
    channel = await service.update(channel, **values)
    if "proxy_id" in values:
        await consume_proxy_reservation(channel.proxy_id)
    await notifications.broadcast(
        "channel.proxy_updated",
        {"channel": ChannelOut.model_validate(channel).model_dump(mode="json")},
    )
    return channel


@router.get("", response_model=list[ChannelOut])
async def list_channels(
    session: AsyncSession = Depends(get_session),
) -> list[ChannelOut]:
    service = ChannelService(session)
    return await service.list()


@router.get("/{channel_id}", response_model=ChannelOut)
async def get_channel(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel


@router.get("/{channel_id}/session-status", response_model=ChannelSessionStatusOut)
async def get_channel_session_status(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelSessionStatusOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    return ChannelSessionStatusOut(
        session_status=channel.session_status,
        pending_project_id=channel.pending_project_id,
        message=channel.error_message,
    )


@router.get("/{channel_id}/avatar", response_model=ChannelAvatarOut)
async def get_channel_avatar(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelAvatarOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    return ChannelAvatarOut(thumbnail_url=channel.thumbnail_url)


@router.get("/{channel_id}/folder-videos", response_model=list[FolderVideoOut])
async def list_channel_folder_videos(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[FolderVideoOut]:
    from app.services.coordinator import _VIDEO_EXTENSIONS

    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    folder = settings.resolve_path(channel.folder_path)
    if not folder or not os.path.isdir(folder):
        return []

    results: list[FolderVideoOut] = []
    try:
        for name in os.listdir(folder):
            ext = os.path.splitext(name)[1].lower()
            if ext not in _VIDEO_EXTENSIONS:
                continue
            full = os.path.join(folder, name)
            if not os.path.isfile(full):
                continue
            stat = os.stat(full)
            results.append(
                FolderVideoOut(
                    channel_id=channel.id,
                    file_name=name,
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                )
            )
    except OSError:
        return []

    results.sort(key=lambda v: v.file_name)
    return results


@router.put("/bulk-settings", response_model=ChannelBulkUpdateResult)
async def bulk_update_channel_settings(
    data: ChannelBulkSettingsUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelBulkUpdateResult:
    service = ChannelService(session)

    settings_data = data.model_dump(exclude_unset=True, exclude={"channel_ids"})
    # Ignore explicit nulls so only provided values are applied — except
    # ai_model, where an explicit null means "disable AI generation".
    settings_data = {
        k: v
        for k, v in settings_data.items()
        if v is not None or (k == "ai_model")
    }
    if not settings_data:
        raise HTTPException(status_code=400, detail="No settings to apply")

    if "ai_model" in settings_data:
        await _validate_custom_ai_provider(session, settings_data.get("ai_model"))

    # Deduplicate channel ids while preserving order.
    channel_ids = list(dict.fromkeys(data.channel_ids))

    channels: list[Channel] = []
    not_found: list[UUID] = []
    for channel_id in channel_ids:
        channel = await service.get(channel_id)
        if not channel or channel.status == "deleted":
            not_found.append(channel_id)
            continue
        channels.append(channel)

    if not channels:
        raise HTTPException(status_code=404, detail="No channels found")

    # Validate min_batch_size <= max_batch_size per channel using merged values
    # before applying anything, so the bulk operation is all-or-nothing.
    for channel in channels:
        new_max = settings_data.get("max_batch_size", channel.max_batch_size)
        new_min = settings_data.get("min_batch_size", channel.min_batch_size)
        if new_min is not None and new_max is not None and new_min > new_max:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"min_batch_size ({new_min}) cannot exceed max_batch_size "
                    f"({new_max}) for channel {channel.id}"
                ),
            )

    # Apply every channel's changes inside ONE transaction. Committing per
    # channel used to make the bulk save queue on SQLite's single writer behind
    # each commit, so with a proxy check running the request took many seconds.
    # One commit keeps the write lock held only briefly and still applies
    # atomically.
    updated: list[Channel] = []
    try:
        for channel in channels:
            updated.append(await service.update(channel, commit=False, **settings_data))
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for channel in updated:
        await session.refresh(channel)

    await coordinator.wake_scan()
    # A channel currently parked in its inter-upload wait must re-read these
    # settings now, not after its old delay window expires.
    for channel in updated:
        coordinator.wake_pacing(channel.id)


    return ChannelBulkUpdateResult(
        updated=[ChannelOut.model_validate(c) for c in updated],
        not_found=not_found,
    )


@router.put("/bulk-video-settings", response_model=ChannelBulkUpdateResult)
async def bulk_update_channel_video_settings(
    data: ChannelBulkVideoSettingsUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelBulkUpdateResult:
    """Apply video metadata defaults to several channels at once.

    Covers the per-channel video settings (title template, description
    template, privacy, category) — the counterpart of ``/bulk-settings``,
    which handles batch upload and limit options.
    """
    from app.services.template_validation import validate_channel_templates

    service = ChannelService(session)

    settings_data = data.model_dump(exclude_unset=True, exclude={"channel_ids"})
    # Ignore explicit nulls so only provided values are applied.
    settings_data = {k: v for k, v in settings_data.items() if v is not None}
    if not settings_data:
        raise HTTPException(status_code=400, detail="No settings to apply")

    # Deduplicate channel ids while preserving order.
    channel_ids = list(dict.fromkeys(data.channel_ids))

    channels: list[Channel] = []
    not_found: list[UUID] = []
    for channel_id in channel_ids:
        channel = await service.get(channel_id)
        if not channel or channel.status == "deleted":
            not_found.append(channel_id)
            continue
        channels.append(channel)

    if not channels:
        raise HTTPException(status_code=404, detail="No channels found")

    # Validate the templates against each channel's merged values before
    # applying anything, so the bulk operation is all-or-nothing.
    if "title_template" in settings_data or "description_template" in settings_data:
        for channel in channels:
            try:
                validate_channel_templates(
                    settings_data.get("title_template", channel.title_template),
                    settings_data.get(
                        "description_template", channel.description_template
                    ),
                )
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Channel {channel.id}: {exc}",
                ) from exc

    # Apply every channel's changes inside ONE transaction (same rationale as
    # /bulk-settings: one short write lock instead of queuing behind N commits).
    updated: list[Channel] = []
    try:
        for channel in channels:
            updated.append(await service.update(channel, commit=False, **settings_data))
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for channel in updated:
        await session.refresh(channel)

    return ChannelBulkUpdateResult(
        updated=[ChannelOut.model_validate(c) for c in updated],
        not_found=not_found,
    )


@router.put("/bulk-folder-settings", response_model=ChannelBulkUpdateResult)
async def bulk_update_channel_folder(
    data: ChannelBulkFolderUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelBulkUpdateResult:
    """Switch the video folder of several channels at once.

    Every listed channel either switches to the referenced channel's folder
    (``folder_from_channel_id``) or back to its own dedicated folder under
    VIDEO_BASE_DIR (explicit ``null``) — the same semantics as the
    ``folder_from_channel_id`` field of ``PUT /channels/{channel_id}``.

    Validation is all-or-nothing and runs for every listed channel before
    anything is applied: a missing or deleted source channel, a channel
    referencing its own folder, or any circular folder reference rejects the
    whole request with a specific error naming the offending channel — no
    channel is saved.
    """
    from app.services.channel_service import find_folder_cycle

    service = ChannelService(session)

    # Deduplicate channel ids while preserving order.
    channel_ids = list(dict.fromkeys(data.channel_ids))

    channels: list[Channel] = []
    not_found: list[UUID] = []
    for channel_id in channel_ids:
        channel = await service.get(channel_id)
        if not channel or channel.status == "deleted":
            not_found.append(channel_id)
            continue
        channels.append(channel)

    if not channels:
        raise HTTPException(status_code=404, detail="No channels found")

    updates: list[tuple[Channel, str]] = []

    if data.folder_from_channel_id is None:
        # Explicit null: every channel goes back to its own dedicated folder
        # under VIDEO_BASE_DIR (created on demand, same as for new channels).
        # A dedicated folder is the root of the folder-reference chain, so no
        # circular reference is possible here.
        for channel in channels:
            dedicated = settings.resolve_path(settings.video_base_dir) / str(channel.id)
            dedicated.mkdir(parents=True, exist_ok=True)
            updates.append((channel, settings.relative_path(dedicated)))
    else:
        source = await session.get(Channel, data.folder_from_channel_id)
        if not source:
            raise HTTPException(
                status_code=404, detail="Source channel for folder not found"
            )
        if source.status == "deleted":
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Source channel {source.id} is deleted and cannot be "
                    "used as a video folder source"
                ),
            )
        if not source.folder_path:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Source channel {source.id} has no video folder configured"
                ),
            )

        # Validate EVERY listed channel before applying anything, so one bad
        # choice leaves all folders untouched. Only the listed channels'
        # folders change — the source keeps its folder exactly as-is.
        for channel in channels:
            if channel.id == source.id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Channel {channel.id} cannot reference its own video folder"
                    ),
                )
            cycle = await find_folder_cycle(session, channel, source)
            if cycle:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Channel {channel.id}: circular video folder reference "
                        + " -> ".join(str(cid) for cid in cycle)
                        + ". The referenced channel's folder must not lead back "
                        "to this channel."
                    ),
                )
        for channel in channels:
            updates.append((channel, source.folder_path))

    # Apply every channel's folder change inside ONE transaction (same
    # rationale as /bulk-settings: one short write lock instead of queuing
    # behind N commits).
    updated: list[Channel] = []
    try:
        for channel, folder_path in updates:
            updated.append(
                await service.update(channel, commit=False, folder_path=folder_path)
            )
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for channel in updated:
        await session.refresh(channel)

    return ChannelBulkUpdateResult(
        updated=[ChannelOut.model_validate(c) for c in updated],
        not_found=not_found,
    )


async def _validate_custom_ai_provider(
    session: AsyncSession, ai_model: str | None
) -> None:
    """Reject ``custom:<id>`` selections pointing at a missing provider."""
    if not ai_model or not ai_model.startswith(CUSTOM_PROVIDER_PREFIX):
        return
    from app.models.ai_model import AICustomProvider

    try:
        provider_id = UUID(ai_model[len(CUSTOM_PROVIDER_PREFIX):])
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid AI provider reference"
        ) from None
    provider = await session.get(AICustomProvider, provider_id)
    if not provider:
        raise HTTPException(status_code=400, detail="AI provider not found")


@router.put("/{channel_id}", response_model=ChannelOut)
async def update_channel(
    channel_id: UUID,
    data: ChannelUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")

    update_data = data.model_dump(exclude_unset=True)

    if update_data.get("proxy_id"):
        proxy = await session.get(Proxy, update_data["proxy_id"])
        if not proxy or not proxy.is_active:
            raise HTTPException(status_code=400, detail="Proxy is not active or does not exist")

    if "ai_model" in update_data:
        await _validate_custom_ai_provider(session, update_data["ai_model"])

    if "proxy_id" in update_data:
        update_data["proxy_recovery_pending"] = False
        await stop_channel_proxy_recovery(channel_id)

    folder_key_present = "folder_from_channel_id" in update_data
    folder_from_channel_id = update_data.pop("folder_from_channel_id", None)
    if folder_from_channel_id:
        source_channel = await session.get(Channel, folder_from_channel_id)
        if not source_channel:
            raise HTTPException(
                status_code=404, detail="Source channel for folder not found"
            )
        # Only this channel's folder changes: the referenced channel keeps
        # its folder exactly as-is. What is rejected here is a circular
        # reference (A uses B's folder while B, directly or through a chain,
        # uses A's) — it used to be accepted and the UI then showed the two
        # channels referencing each other.
        from app.services.channel_service import find_folder_cycle

        cycle = await find_folder_cycle(session, channel, source_channel)
        if cycle:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Circular video folder reference: "
                    + " -> ".join(str(cid) for cid in cycle)
                    + ". The referenced channel's folder must not lead back "
                    "to this channel."
                ),
            )
        update_data["folder_path"] = source_channel.folder_path
    elif folder_key_present:
        # Explicit null: switch the channel back to its own dedicated folder
        # under VIDEO_BASE_DIR (created on demand, same as for new channels).
        # An absent key still means "do not change the folder".
        dedicated = settings.resolve_path(settings.video_base_dir) / str(channel.id)
        dedicated.mkdir(parents=True, exist_ok=True)
        update_data["folder_path"] = settings.relative_path(dedicated)

    if update_data.get("status") == "active":
        update_data["paused_until"] = None
        update_data["error_message"] = None
        update_data["pause_reason"] = None
    elif update_data.get("status") == "paused" and "pause_reason" not in update_data:
        update_data["pause_reason"] = "manual"

    # Validate min_batch_size <= max_batch_size using merged values.
    new_max = update_data.get("max_batch_size", channel.max_batch_size)
    new_min = update_data.get("min_batch_size", channel.min_batch_size)
    if new_min is not None and new_max is not None and new_min > new_max:
        raise HTTPException(
            status_code=400,
            detail=f"min_batch_size ({new_min}) cannot exceed max_batch_size ({new_max})",
        )

    try:
        channel = await service.update(channel, **update_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if "proxy_id" in update_data:
        await consume_proxy_reservation(channel.proxy_id)

    if update_data.get("status") == "paused":
        await coordinator.stop_channel_worker(channel_id)

    limit_fields = {"upload_limit_enabled", "upload_limit_count", "upload_limit_period_hours"}
    batch_fields = {
        "max_batch_size",
        "min_batch_size",
        "inter_upload_delay_seconds",
        "shuffle_upload_order",
        "skip_blocked_videos",
    }
    if (limit_fields | batch_fields) & set(update_data.keys()):
        await coordinator.wake_scan()
        # Interrupt a running inter-upload wait so it re-reads the fresh
        # pacing/limit values instead of sleeping out the old window.
        coordinator.wake_pacing(channel_id)

    return channel


@router.get("/{channel_id}/upload-limit", response_model=ChannelUploadLimit)
async def get_channel_upload_limit(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelUploadLimit:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    info = await service.check_upload_limit(channel)
    return ChannelUploadLimit(
        enabled=channel.upload_limit_enabled,
        count=channel.upload_limit_count,
        period_hours=channel.upload_limit_period_hours,
        uploads_in_current_period=info["uploads_in_period"],
        period_started_at=info["period_start"],
        period_ends_at=info["period_end"],
        is_limited=info["is_limited"],
        remaining=info["remaining"] if info["remaining"] is not None else 0,
        reset_at=info["reset_at"],
    )


@router.get("/{channel_id}/uploaded-count", response_model=ChannelUploadedCount)
async def get_channel_uploaded_count(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelUploadedCount:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    stats = await service.get_uploaded_stats(channel)
    return ChannelUploadedCount(
        total_uploaded=stats["total_uploaded"],
        uploads_in_current_period=stats["uploads_in_current_period"],
        period_hours=stats["period_hours"],
        is_limited=stats["is_limited"],
        remaining=stats["remaining"],
        reset_at=stats["reset_at"],
    )


@router.get("/{channel_id}/videos/count", response_model=ChannelUploadedVideosCount)
async def get_channel_videos_count(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelUploadedVideosCount:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    stats = await service.get_uploaded_stats(channel)
    return ChannelUploadedVideosCount(
        channel_id=channel.id,
        uploaded_count=stats["total_uploaded"],
    )


@router.put("/{channel_id}/upload-limit", response_model=ChannelOut)
async def update_channel_upload_limit(
    channel_id: UUID,
    data: ChannelUploadLimitUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")

    update_data = data.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Ignore explicit nulls and map the public field names onto the channel columns.
    update_data = {k: v for k, v in update_data.items() if v is not None}
    field_map = {
        "enabled": "upload_limit_enabled",
        "count": "upload_limit_count",
        "period_hours": "upload_limit_period_hours",
    }
    update_data = {field_map.get(k, k): v for k, v in update_data.items()}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        channel = await service.update(channel, **update_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await coordinator.wake_scan()
    coordinator.wake_pacing(channel_id)
    return channel


@router.get("/{channel_id}/batch-settings", response_model=ChannelBatchSettings)
async def get_channel_batch_settings(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelBatchSettings:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    return ChannelBatchSettings(
        max_batch_size=channel.max_batch_size,
        min_batch_size=channel.min_batch_size,
        inter_upload_delay_seconds=channel.inter_upload_delay_seconds,
        shuffle_upload_order=channel.shuffle_upload_order,
        skip_blocked_videos=channel.skip_blocked_videos,
    )


@router.put("/{channel_id}/batch-settings", response_model=ChannelBatchSettings)
async def update_channel_batch_settings(
    channel_id: UUID,
    data: ChannelBatchSettingsUpdate,
    session: AsyncSession = Depends(get_session),
) -> ChannelBatchSettings:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")

    update_data = {k: v for k, v in data.model_dump(exclude_unset=True).items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Validate min <= max against the merged values.
    new_max = update_data.get("max_batch_size", channel.max_batch_size)
    new_min = update_data.get("min_batch_size", channel.min_batch_size)
    if new_min > new_max:
        raise HTTPException(
            status_code=400,
            detail=f"min_batch_size ({new_min}) cannot exceed max_batch_size ({new_max})",
        )

    try:
        channel = await service.update(channel, **update_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await coordinator.wake_scan()
    coordinator.wake_pacing(channel_id)
    return ChannelBatchSettings(
        max_batch_size=channel.max_batch_size,
        min_batch_size=channel.min_batch_size,
        inter_upload_delay_seconds=channel.inter_upload_delay_seconds,
        shuffle_upload_order=channel.shuffle_upload_order,
        skip_blocked_videos=channel.skip_blocked_videos,
    )


async def _stop_quietly(awaitable, *, timeout: float, what: str) -> None:
    """Await a teardown step with a hard timeout.

    A step stuck on database contention must never hang the whole DELETE
    request: on timeout the underlying task keeps unwinding in the
    background, while the request proceeds (the row is already gone, so
    nothing new can start for the channel).
    """
    try:
        await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("Timed out after {}s waiting for {}", timeout, what)


@router.delete("/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> None:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    # Mark the channel deleted and commit BEFORE any teardown. While the row
    # still says "active", the scan loop keeps waking workers for it: under
    # SQLite write contention the row used to survive for seconds after the
    # previous worker was stopped, and a worker resurrected by the scan loop
    # loaded it, started a new batch and kept uploading/broadcasting events —
    # the client then resurrected the just-deleted channel in the UI. With
    # the row marked deleted up front, every worker (old, resurrected or in
    # flight) bails out on its next database read.
    channel.status = "deleted"
    channel.auto_upload_enabled = False
    await session.commit()

    await _stop_quietly(
        coordinator.stop_channel_worker(channel_id),
        timeout=10.0,
        what=f"channel worker stop for {channel_id}",
    )
    await _stop_quietly(
        stop_channel_proxy_recovery(channel_id),
        timeout=10.0,
        what=f"proxy recovery stop for {channel_id}",
    )
    await _stop_quietly(
        coordinator.stop_channel_metadata(channel_id),
        timeout=10.0,
        what=f"metadata loop stop for {channel_id}",
    )
    await service.delete(channel)


@router.post("/{channel_id}/pause", response_model=ChannelOut)
async def pause_channel(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    await service.pause(channel, settings.pause_on_manual_minutes, reason="manual")
    await coordinator.stop_channel_worker(channel_id)
    return channel


@router.post("/{channel_id}/unpause", response_model=ChannelOut)
async def unpause_channel(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    await service.unpause(channel)
    await coordinator.wake_scan()
    return channel


@router.post("/{channel_id}/clear-error", response_model=ChannelOut)
async def clear_channel_error(
    channel_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    await service.unpause(channel)
    await coordinator.wake_scan()
    return channel
