import asyncio
import hashlib
import os
import random
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings
from app.core.datetime_utils import ensure_aware
from app.core.db import async_session_maker
from app.core.logging import logger
from app.models.channel import Channel
from app.models.blocked_hash import BlockedVideoHash
from app.models.oauth_state import OAuthState
from app.models.project import GCPProject
from app.models.proxy import Proxy
from app.models.upload_task import UploadTask
from app.models.rejected_hash import RejectedVideoHash
from app.models.uploaded_hash import UploadedVideoHash
from app.schemas.channel import ChannelOut
from app.schemas.task import TaskOut
from app.services.ai_cache import pick_cached_metadata, save_cached_metadata
from app.services.ai_service import generate_video_metadata
from app.services.channel_service import ChannelService, get_current_channel_proxy_url
from app.services.http_client import get_client
from app.services.memory_limiter import upload_memory_limiter
from app.services.notification_service import NotificationService, notifications
from app.services.project_selector import select_project
from app.services.proxy_service import (
    ProxyService,
    start_proxy_recovery_for_proxy,
)
from app.services.youtube_auth import (
    _generate_code_challenge,
    _generate_code_verifier,
    build_authorization_url,
    generate_state,
)
from app.services.youtube_uploader import (
    YouTubeUploadError,
    create_upload_session,
    delete_video,
    is_copyright_blocked_everywhere,
    query_upload_progress,
    upload_file_to_session,
    wait_for_video_processing,
)


def _task_payload(task: UploadTask) -> dict:
    return TaskOut.model_validate(task).model_dump(mode="json")


def _channel_payload(channel: Channel) -> dict:
    return ChannelOut.model_validate(channel).model_dump(mode="json")

_VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".webm",
    ".flv",
    ".wmv",
    ".m4v",
    ".mpeg",
    ".mpg",
}


class ChannelPaused(Exception):
    pass


@dataclass
class _BatchResult:
    """Work that must run after the shared-folder transfer lock is released."""

    processing_candidates: list[tuple[UUID, str]] = field(default_factory=list)
    hit_quota: bool = False
    hit_auth: bool = False


_HASH_CHUNK_SIZE = 1024 * 1024
# Cached digests keyed by absolute path. Each entry is a few hundred bytes, so a
# generous cap still costs well under a megabyte while removing the repeated
# full-library reads the scan loop would otherwise perform.
_HASH_CACHE_MAX_ENTRIES = 20_000
_hash_cache: "OrderedDict[str, tuple[tuple[int, int], str]]" = OrderedDict()
# SQLite caps a statement at 999 bound parameters by default.
_HASH_QUERY_CHUNK = 500


async def _compute_file_hash(file_path: str) -> str:
    """Hash a video file, reusing a cached digest when the file is unchanged.

    The scan loop re-hashes every file in every channel folder on each pass
    (default: every 10 seconds). Without a cache an idle server reads its whole
    video library from disk continuously and allocates a fresh buffer per chunk,
    which inflates the allocator arenas even though nothing is uploading. The
    cache key includes size and mtime, so an edited or replaced file is hashed
    again.
    """

    def _hash() -> str:
        resolved = settings.resolve_path(file_path)
        if resolved is None:
            raise FileNotFoundError(f"Cannot resolve file path: {file_path}")
        stat = os.stat(resolved)
        key = str(resolved)
        signature = (stat.st_size, stat.st_mtime_ns)
        cached = _hash_cache.get(key)
        if cached is not None and cached[0] == signature:
            _hash_cache.move_to_end(key)
            return cached[1]

        hasher = hashlib.blake2b()
        # One reusable buffer instead of a new 1 MiB bytes object per chunk.
        buffer = bytearray(_HASH_CHUNK_SIZE)
        view = memoryview(buffer)
        with open(resolved, "rb", buffering=0) as f:
            while True:
                read = f.readinto(buffer)
                if not read:
                    break
                hasher.update(view[:read])
        digest = hasher.hexdigest()

        _hash_cache[key] = (signature, digest)
        _hash_cache.move_to_end(key)
        while len(_hash_cache) > _HASH_CACHE_MAX_ENTRIES:
            _hash_cache.popitem(last=False)
        return digest

    return await asyncio.to_thread(_hash)


def forget_cached_hash(file_path: str) -> None:
    """Drop a cached digest, e.g. after the file is deleted."""
    resolved = settings.resolve_path(file_path)
    if resolved is not None:
        _hash_cache.pop(str(resolved), None)


def remember_cached_hash(file_path: str, digest: str) -> None:
    """Seed the cache with a digest computed while streaming the file in.

    The upload endpoint already hashes the bytes on their way to disk, so the
    scan loop does not need to read the file back to learn the same digest.
    """
    resolved = settings.resolve_path(file_path)
    if resolved is None:
        return
    try:
        stat = os.stat(resolved)
    except OSError:
        return
    key = str(resolved)
    _hash_cache[key] = ((stat.st_size, stat.st_mtime_ns), digest)
    _hash_cache.move_to_end(key)
    while len(_hash_cache) > _HASH_CACHE_MAX_ENTRIES:
        _hash_cache.popitem(last=False)


# YouTube metadata limits
_MAX_YOUTUBE_TITLE_LENGTH = 100
_MAX_YOUTUBE_DESCRIPTION_LENGTH = 5000


def _format_metadata(channel: Channel, file_path: str) -> tuple[str, str]:
    path = Path(file_path)
    values = {
        "filename": path.stem,
        "file": path.name,
        "ext": path.suffix.lstrip("."),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
    }

    title_template = (channel.title_template or "").strip()
    try:
        title = title_template.format(**values) if title_template else values["filename"]
    except (KeyError, ValueError):
        title = values["filename"]
    title = title.strip() or values["filename"]

    # YouTube rejects titles longer than 100 characters. If the template
    # produces an oversized title, fall back to the filename.
    if len(title) > _MAX_YOUTUBE_TITLE_LENGTH:
        title = values["filename"][: _MAX_YOUTUBE_TITLE_LENGTH]

    description_template = (channel.description_template or "").strip()
    try:
        description = (
            description_template.format(**values) if description_template else ""
        )
    except (KeyError, ValueError):
        description = ""

    if len(description) > _MAX_YOUTUBE_DESCRIPTION_LENGTH:
        description = description[: _MAX_YOUTUBE_DESCRIPTION_LENGTH]

    return title, description


async def _prepare_task_metadata(
    session: AsyncSession, channel: Channel, task: UploadTask, file_path: str
) -> None:
    """Fill missing task title/description.

    When the channel has an AI model configured (an API type or "auto"), the
    video is sent to the AI model with the saved title prompt (and the
    description prompt when description generation is enabled). With
    ``ai_use_cached`` the saved generation cache for this video hash is used
    once it holds the maximum number of unique entries. If no saved model
    works, the channel templates fill the gaps exactly like a channel without
    AI settings.

    The AI call is bounded by ``AI_GENERATION_TIMEOUT_SECONDS`` per video and
    runs with the batch session's read transaction closed, so a slow or
    broken AI setup can only delay one video by that long before the
    templates take over — the upload worker never stalls indefinitely.
    """
    needs_title = task.title is None or not task.title.strip()
    needs_description = task.description is None
    want_description = needs_description and channel.ai_generate_description

    if channel.ai_model and (needs_title or want_description):
        ai_title: str | None = None
        ai_description: str | None = None
        if channel.ai_use_cached and task.file_hash:
            cached = await pick_cached_metadata(session, task.file_hash)
            if cached:
                ai_title = cached["title"]
                ai_description = cached["description"]
        if ai_title is None:
            ai_meta = None
            # End the read transaction (task loads, cache reads) before the
            # network-bound generation: a WAL snapshot must not live across
            # a potentially long AI call.
            await session.commit()
            logger.info(
                "Channel {} generating AI metadata for {}",
                channel.id,
                Path(file_path).name,
            )
            try:
                ai_meta = await asyncio.wait_for(
                    generate_video_metadata(
                        channel.ai_model,
                        file_path,
                        want_description=want_description,
                    ),
                    timeout=settings.ai_generation_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "AI metadata generation timed out after {:.0f}s for {}; "
                    "falling back to channel templates",
                    settings.ai_generation_timeout_seconds,
                    file_path,
                )
            except Exception as exc:
                logger.warning(
                    "AI metadata generation failed for {}: {}", file_path, exc
                )
            if ai_meta:
                if ai_meta.get("title"):
                    ai_title = ai_meta["title"]
                if want_description and ai_meta.get("description"):
                    ai_description = ai_meta["description"]
                if task.file_hash:
                    await save_cached_metadata(
                        session,
                        task.file_hash,
                        ai_meta.get("title") or "",
                        ai_meta.get("description") or "",
                    )
        if ai_title and needs_title:
            task.title = ai_title
            needs_title = False
        if ai_description and want_description and needs_description:
            task.description = ai_description
            needs_description = False

    if needs_title or needs_description:
        generated_title, generated_description = _format_metadata(channel, file_path)
        if needs_title:
            task.title = generated_title
        if needs_description:
            task.description = generated_description


class UploadCoordinator:
    def __init__(self, notifications: NotificationService) -> None:
        self.notifications = notifications
        self._stop_event = asyncio.Event()
        self._scan_wake = asyncio.Event()
        self._scan_task: asyncio.Task | None = None
        self._metadata_tasks: dict[UUID, asyncio.Task] = {}
        self._workers: dict[UUID, asyncio.Task] = {}
        self._worker_created_at: dict[UUID, datetime] = {}
        # One stop-event per long-running channel worker loop. ``stop()`` and
        # ``stop_channel_worker()`` set this event; the loop polls it between
        # iterations so it exits cleanly instead of being hard-cancelled mid-
        # upload.
        self._worker_stop_events: dict[UUID, asyncio.Event] = {}
        self._preserve_tasks_on_stop: set[UUID] = set()
        # One event per channel currently parked in the inter-upload pacing
        # wait inside ``_process_batch``. Saving the channel's pacing/limit
        # settings sets it; the wait wakes, re-reads the fresh values and
        # recomputes the remaining time, so a delay lowered mid-wait (even
        # below the already-elapsed time) starts the batch immediately
        # instead of sleeping out the old window.
        self._pacing_events: dict[UUID, asyncio.Event] = {}
        # Serializes task collection for channels that share a video folder.
        # Each sharer has its own worker; without mutual exclusion two
        # workers collecting in the same scan pass both claimed the same
        # files (the "claimed" filter only sees committed rows, which land
        # after both collectors ran) and every file went to YouTube once per
        # sharing channel.
        self._folder_collect_locks: dict[str, asyncio.Lock] = {}
        # A folder-wide gate used after a transfer when one or more uploaded
        # videos are still being checked/deleted. It prevents another sharer
        # from starting its already-collected batch between the completed
        # transfer and the copyright decision, without holding the transfer
        # lock during YouTube's potentially long processing phase.
        self._folder_processing_events: dict[str, asyncio.Event] = {}
        self._folder_processing_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def wake_scan(self) -> None:
        """Trigger the scan loop to run immediately."""
        self._scan_wake.set()

    def _pacing_event(self, channel_id: UUID) -> asyncio.Event:
        event = self._pacing_events.get(channel_id)
        if event is None:
            event = asyncio.Event()
            self._pacing_events[channel_id] = event
        return event

    def wake_pacing(self, channel_id: UUID) -> None:
        """Interrupt the channel's inter-upload wait to re-read its settings.

        No-op when the channel is not currently waiting (the next worker
        iteration reads the fresh values anyway).
        """
        event = self._pacing_events.get(channel_id)
        if event is not None:
            event.set()

    async def start(self) -> None:
        # ensure shared http client is initialized
        get_client()
        self._scan_task = asyncio.create_task(self._scan_loop())
        async with async_session_maker() as session:
            result = await session.exec(
                select(Channel).where(
                    Channel.status != "deleted",
                )
            )
            channel_ids = [channel.id for channel in result.scalars().all()]
        for channel_id in channel_ids:
            # Existing channels are refreshed on their regular schedule, not
            # all at startup. The deterministic offset spreads API requests.
            await self.start_channel_metadata(channel_id, refresh_now=False)
        logger.info("Upload coordinator started")

    async def start_channel_metadata(
        self,
        channel_id: UUID,
        *,
        refresh_now: bool = True,
        initial_delay: int | None = None,
    ) -> None:
        """Start one async metadata refresher for a channel if not running."""
        task = self._metadata_tasks.get(channel_id)
        if task is not None and not task.done():
            return
        self._metadata_tasks[channel_id] = asyncio.create_task(
            self._channel_metadata_loop(
                channel_id, refresh_now=refresh_now, initial_delay=initial_delay
            )
        )

    async def stop_channel_metadata(self, channel_id: UUID) -> None:
        task = self._metadata_tasks.pop(channel_id, None)
        if task is None:
            return
        if task is asyncio.current_task():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._stop_event.set()
        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass
        metadata_tasks = list(self._metadata_tasks.items())
        self._metadata_tasks.clear()
        for _, task in metadata_tasks:
            task.cancel()
        for _, task in metadata_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            workers = list(self._workers.values())
            stop_events = list(self._worker_stop_events.values())
        for ev in stop_events:
            ev.set()
        for worker in workers:
            worker.cancel()
        for worker in workers:
            try:
                await worker
            except asyncio.CancelledError:
                pass
        async with self._lock:
            self._workers.clear()
            self._worker_created_at.clear()
            self._worker_stop_events.clear()
        logger.info("Upload coordinator stopped")

    async def wake_channel(
        self, channel_id: UUID, task_ids: list[UUID] | None = None
    ) -> None:
        async with self._lock:
            if channel_id not in self._workers:
                self._worker_stop_events[channel_id] = asyncio.Event()
                self._workers[channel_id] = asyncio.create_task(
                    self._channel_worker(channel_id, target_task_ids=task_ids)
                )
                self._worker_created_at[channel_id] = datetime.now(timezone.utc)

    async def stop_channel_worker(
        self, channel_id: UUID, preserve_tasks: bool = False
    ) -> None:
        if preserve_tasks:
            self._preserve_tasks_on_stop.add(channel_id)
        async with self._lock:
            worker = self._workers.pop(channel_id, None)
            self._worker_created_at.pop(channel_id, None)
            stop_event = self._worker_stop_events.pop(channel_id, None)
        if stop_event is not None:
            stop_event.set()
        if worker:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        elif preserve_tasks:
            self._preserve_tasks_on_stop.discard(channel_id)

    async def _scan_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._scan_once()
            except Exception:
                logger.exception("Scan loop error")
            try:
                await asyncio.wait_for(
                    self._scan_wake.wait(),
                    timeout=settings.channel_scan_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            else:
                self._scan_wake.clear()

    async def _maybe_auto_resume(
        self,
        channel_service: ChannelService,
        channel: Channel,
        now: datetime,
    ) -> Channel | None:
        """Auto-resume a channel if its non-manual pause has expired.

        A non-manual pause without a ``paused_until`` value is also resumed,
        because it cannot be intentional. Manual pauses are never touched.

        Returns the refreshed channel, or ``None`` if it no longer exists.
        """
        if channel.status != "paused":
            return channel
        if channel.pause_reason == "manual":
            return channel

        paused_until = ensure_aware(channel.paused_until)
        if paused_until is not None and paused_until > now:
            return channel

        if channel.pause_reason == "upload_limit":
            # Do not clear an expired/stale pause blindly.  The configured
            # limit remains authoritative until it actually has capacity;
            # recalculate its deadline in case settings or older pause data
            # left ``paused_until`` behind.
            limit_info = await channel_service.check_upload_limit(channel)
            if limit_info["is_limited"]:
                await channel_service.pause_for_upload_limit(
                    channel, limit_info["reset_at"]
                )
            else:
                await channel_service.clear_upload_limit_pause(channel)
        else:
            await channel_service.unpause(channel)

        return await channel_service.get(channel.id)

    async def _scan_once(self) -> None:
        async with async_session_maker() as session:
            channel_service = ChannelService(session)
            channels = await channel_service.list()
            now = datetime.now(timezone.utc)

            for channel in channels:
                # End the previous iteration's read transaction so the WAL
                # snapshot is bounded to one channel's checks instead of the
                # whole scan (a snapshot blocks checkpoints while it lives).
                await session.commit()
                if channel.status in ("error", "deleted"):
                    # A dead channel's worker never runs, so its active
                    # tasks would claim files in a SHARED folder forever —
                    # an active sharer then cannot upload them ("limit
                    # shows N remaining but nothing uploads"). Release them.
                    await self._fail_dead_channel_tasks(
                        session,
                        channel.id,
                        f"Channel is {channel.status}; task released so the "
                        "shared folder is not blocked",
                    )
                    continue
                if channel.session_status != "active":
                    await self._fail_dead_channel_tasks(
                        session,
                        channel.id,
                        f"Channel session is {channel.session_status}; task "
                        "released so the shared folder is not blocked",
                    )
                    continue
                if not channel.auto_upload_enabled:
                    continue
                # Auto-resume non-manual pauses once the pause time has passed.
                # This must happen before the paused_until check so a channel whose
                # pause window has expired can be processed in this scan.
                channel = await self._maybe_auto_resume(channel_service, channel, now)
                if not channel or channel.status in ("error", "deleted"):
                    continue

                if channel.paused_until and ensure_aware(channel.paused_until) > now:
                    continue
                if channel.status == "paused":
                    # Manual pauses without a paused_until should not be processed.
                    continue
                folder = settings.resolve_path(channel.folder_path)
                if not folder or not os.path.isdir(folder):
                    continue

                # Ensure a worker loop is alive for this channel. The worker
                # itself checks every iteration whether to start uploading,
                # so the scan loop only needs to guarantee one exists.
                async with self._lock:
                    worker = self._workers.get(channel.id)
                    worker_dead = worker is not None and worker.done()
                if worker_dead:
                    async with self._lock:
                        self._workers.pop(channel.id, None)
                        self._worker_created_at.pop(channel.id, None)
                        self._worker_stop_events.pop(channel.id, None)
                if worker is None or worker_dead:
                    await self.wake_channel(channel.id)

                # If the configurable upload limit is exhausted, pause the channel
                # so it auto-resumes when the window expires. The running worker
                # notices this on its next iteration and simply sleeps.
                if channel.upload_limit_enabled:
                    limit_info = await channel_service.check_upload_limit(channel)
                    if limit_info["is_limited"]:
                        await channel_service.pause_for_upload_limit(
                            channel, limit_info["reset_at"]
                        )
                        continue

    def _get_folder_collect_lock(self, folder_path: str) -> asyncio.Lock:
        """One lock per shared folder, so sharers serialize collection."""
        lock = self._folder_collect_locks.get(folder_path)
        if lock is None:
            lock = asyncio.Lock()
            self._folder_collect_locks[folder_path] = lock
        return lock

    def _folder_processing_event(self, folder_path: str) -> asyncio.Event:
        event = self._folder_processing_events.get(folder_path)
        if event is None:
            event = asyncio.Event()
            event.set()
            self._folder_processing_events[folder_path] = event
        return event

    def _begin_folder_processing_wait(self, folder_path: str) -> None:
        event = self._folder_processing_event(folder_path)
        self._folder_processing_counts[folder_path] = (
            self._folder_processing_counts.get(folder_path, 0) + 1
        )
        event.clear()

    def _end_folder_processing_wait(self, folder_path: str) -> None:
        remaining = self._folder_processing_counts.get(folder_path, 0) - 1
        if remaining <= 0:
            self._folder_processing_counts.pop(folder_path, None)
            self._folder_processing_event(folder_path).set()
        else:
            self._folder_processing_counts[folder_path] = remaining

    async def _wait_for_folder_processing(self, folder_path: str) -> None:
        """Wait until this shared folder has no pending post-upload checks."""
        await self._folder_processing_event(folder_path).wait()

    async def _folder_sharing_channels(
        self, session: AsyncSession, channel: Channel
    ) -> list[UUID]:
        """Ids of every non-deleted channel whose folder_path equals ours.

        Channels sharing a video folder form one upload queue: a file must be
        uploaded once, no matter which channel's worker picks it up. Always
        includes ``channel.id`` itself, even if it is the only sharer.
        """
        if not channel.folder_path:
            return [channel.id]
        result = await session.exec(
            select(Channel.id).where(
                Channel.folder_path == channel.folder_path,
                Channel.status != "deleted",
            )
        )
        sharer_ids = list(result.scalars().all())
        if channel.id not in sharer_ids:
            sharer_ids.append(channel.id)
        return sharer_ids

    async def _collect_new_files(
        self, session: AsyncSession, channel: Channel, channel_service: ChannelService
    ) -> tuple[list[tuple[str, str]], int]:
        """Collect all eligible files and the batch limit.

        Returns ``(files, limit)`` where ``files`` is the full list of
        (file_path, file_hash) pairs that are neither uploaded nor rejected,
        and ``limit`` is the maximum number of tasks the next batch may take.
        The list is NOT truncated to ``limit`` here so the caller can compare
        the *true* waiting count against ``min_batch_size``.
        """
        base_limit = min(channel.max_batch_size or 200, 200)
        limit = base_limit
        if channel.upload_limit_enabled:
            limit_info = await channel_service.check_upload_limit(channel)
            if limit_info["is_limited"]:
                logger.info(
                    "Channel {} collect_new_files: upload limit reached "
                    "({}/{} in {}h), returning empty batch",
                    channel.id,
                    limit_info["uploads_in_period"],
                    channel.upload_limit_count,
                    channel.upload_limit_period_hours,
                )
                return [], 0
            remaining = limit_info.get("remaining")
            if remaining is not None:
                limit = min(base_limit, max(0, remaining))
                if limit <= 0:
                    logger.info(
                        "Channel {} collect_new_files: no remaining uploads, "
                        "returning empty batch",
                        channel.id,
                    )
                    return [], 0
        # End the read transaction opened by the limit check (or left over
        # from the caller) before hashing: hashing a large batch of new files
        # can take minutes, and an open WAL snapshot blocks checkpoints for
        # the whole time.
        await session.commit()
        folder = settings.resolve_path(channel.folder_path)
        if not folder:
            return [], limit
        try:
            # scandir reuses the directory entry's stat data, so this avoids one
            # extra syscall per file compared to listdir + is_file().
            with os.scandir(folder) as it:
                entries = [
                    settings.relative_path(entry.path)
                    for entry in it
                    if os.path.splitext(entry.name)[1].lower() in _VIDEO_EXTENSIONS
                    and entry.is_file()
                ]
        except OSError:
            return [], limit

        if not entries:
            return [], limit

        hashes = [
            await _compute_file_hash(settings.resolve_path(file_path))
            for file_path in entries
        ]

        # Two set-membership queries instead of two queries per file. Scanning a
        # folder with hundreds of videos used to issue hundreds of round-trips on
        # every pass of the scan loop, each materializing ORM instances that were
        # discarded immediately.
        #
        # Uploaded hashes are matched across every channel that SHARES this
        # folder, not just this one: several channels may reference the same
        # video folder, and each of them has its own worker. With a
        # per-channel filter every sharer saw the file as "new" and uploaded
        # it again — one file went to YouTube once per sharing channel.
        folder_sharers = await self._folder_sharing_channels(session, channel)
        # Normally a shared folder is one queue and an uploaded hash excludes
        # a file for every sharer.  Shuffle mode deliberately balances the
        # same folder across its channels instead: each channel skips only
        # videos it has uploaded itself and prefers videos unused by the
        # other sharers.
        balanced_shared_shuffle = (
            bool(getattr(channel, "shuffle_upload_order", False))
            and len(folder_sharers) > 1
        )
        unique_hashes = list(dict.fromkeys(hashes))
        uploaded: set[str] = set()
        uploaded_by_current: set[str] = set()
        uploaded_by_others: dict[str, int] = {}
        rejected: set[str] = set()
        blocked: set[str] = set()
        for chunk_start in range(0, len(unique_hashes), _HASH_QUERY_CHUNK):
            chunk = unique_hashes[chunk_start : chunk_start + _HASH_QUERY_CHUNK]
            uploaded_result = await session.exec(
                select(UploadedVideoHash.file_hash).where(
                    UploadedVideoHash.channel_id.in_(folder_sharers),
                    UploadedVideoHash.file_hash.in_(chunk),
                )
            )
            uploaded.update(uploaded_result.scalars().all())
            if balanced_shared_shuffle:
                history_result = await session.exec(
                    select(UploadTask.channel_id, UploadTask.file_hash).where(
                        UploadTask.channel_id.in_(folder_sharers),
                        UploadTask.status == "completed",
                        UploadTask.file_hash.in_(chunk),
                    )
                )
                for uploader_id, file_hash in history_result.all():
                    if uploader_id == channel.id:
                        uploaded_by_current.add(file_hash)
                    else:
                        uploaded_by_others[file_hash] = (
                            uploaded_by_others.get(file_hash, 0) + 1
                        )
            rejected_result = await session.exec(
                select(RejectedVideoHash.file_hash).where(
                    RejectedVideoHash.file_hash.in_(chunk)
                )
            )
            rejected.update(rejected_result.scalars().all())
            if getattr(channel, "skip_blocked_videos", False):
                blocked_result = await session.exec(
                    select(BlockedVideoHash.file_hash).where(
                        BlockedVideoHash.file_hash.in_(chunk)
                    )
                )
                blocked.update(blocked_result.scalars().all())

        # A shared folder is one upload queue: skip files that a sharer's
        # worker already has an active task for (pending/session_created/
        # uploading), not just this channel's own tasks. Without this, two
        # sharers' workers starting batches in the same scan pass each
        # collected the same file and uploaded it twice — the user saw "two
        # videos in a row" on channels whose folder references another
        # channel.
        active_result = await session.exec(
            select(UploadTask.file_path).where(
                UploadTask.channel_id.in_(folder_sharers),
                UploadTask.status.in_(("pending", "session_created", "uploading")),
                UploadTask.file_path.in_(entries),
            )
        )
        claimed_paths = set(active_result.scalars().all())

        # Only skip files that are already uploaded, rejected by YouTube, or
        # claimed by another sharer's active task.  The shuffle branch keeps
        # files uploaded by *other* sharers available, so it can distribute
        # the folder's videos between channels; it still never re-uploads a
        # video on the current channel.
        selected = [
            (file_path, file_hash)
            for file_path, file_hash in zip(entries, hashes)
            if (
                file_hash not in uploaded_by_current
                if balanced_shared_shuffle
                else file_hash not in uploaded
            )
            and file_hash not in rejected
            and file_hash not in blocked
            and file_path not in claimed_paths
        ]

        if balanced_shared_shuffle:
            # Shuffle within every usage-count tier, but place zero-use files
            # first, followed by the least-used ones.  The caller applies the
            # batch limit after this ordering, so a small batch still gets the
            # intended priority.
            tiers: dict[int, list[tuple[str, str]]] = {}
            for item in selected:
                tiers.setdefault(uploaded_by_others.get(item[1], 0), []).append(item)
            selected = []
            for usage_count in sorted(tiers):
                random.shuffle(tiers[usage_count])
                selected.extend(tiers[usage_count])

        return selected, limit

    async def _channel_metadata_loop(
        self,
        channel_id: UUID,
        *,
        refresh_now: bool,
        initial_delay: int | None,
    ) -> None:
        interval = max(1, settings.channel_snippet_refresh_interval_seconds)
        try:
            if not refresh_now:
                initial_delay = (
                    channel_id.int % interval
                    if initial_delay is None
                    else max(0, initial_delay)
                )
                if initial_delay:
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=initial_delay
                        )
                    except asyncio.TimeoutError:
                        pass
                    if self._stop_event.is_set():
                        return

            while not self._stop_event.is_set():
                async with async_session_maker() as session:
                    channel = await session.get(Channel, channel_id)
                    if channel is None or channel.status == "deleted":
                        return
                    if (
                        channel.session_status == "active"
                        and not channel.proxy_recovery_pending
                    ):
                        service = ChannelService(session)
                        try:
                            access_token = await service.ensure_access_token(channel)
                            await service.refresh_channel_snippet(channel, access_token)
                        except Exception:
                            logger.exception(
                                "Metadata refresh failed for channel {}", channel_id
                            )

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        finally:
            if self._metadata_tasks.get(channel_id) is asyncio.current_task():
                self._metadata_tasks.pop(channel_id, None)

    async def _channel_worker(
        self, channel_id: UUID, target_task_ids: list[UUID] | None = None
    ) -> None:
        """Long-running per-channel loop.

        Each iteration checks whether the channel is allowed to upload right
        now. If it is, runs a single batch; if not (paused, quota reached, no
        files, min_batch not met yet), sleeps briefly and tries again. The
        loop only exits when ``stop_channel_worker`` / ``coordinator.stop``
        sets the per-channel stop event or cancels this coroutine.
        """
        stop_event = self._worker_stop_events.get(channel_id)
        if stop_event is None:
            # Defensive: should never happen because wake_channel sets it,
            # but gracefully no-op if we somehow got here.
            return

        try:
            while not stop_event.is_set():
                try:
                    await self._worker_iteration(channel_id, target_task_ids)
                except ChannelPaused:
                    logger.info(
                        "Channel {} iteration paused, waiting for next tick",
                        channel_id,
                    )
                except Exception:
                    logger.exception(
                        "Channel {} worker iteration failed", channel_id
                    )

                # Deleted/error channels must not retain an idle worker forever.
                async with async_session_maker() as state_session:
                    state_channel = await state_session.get(Channel, channel_id)
                    if not state_channel or state_channel.status in ("error", "deleted"):
                        break

                # Clear the one-shot task filter after the first iteration.
                if target_task_ids:
                    target_task_ids = None

                # Idle sleep until the next iteration or an external stop.
                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=settings.channel_scan_interval_seconds
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            async with self._lock:
                self._workers.pop(channel_id, None)
                self._worker_created_at.pop(channel_id, None)
                self._worker_stop_events.pop(channel_id, None)
            # Mark any leftover unprocessed tasks as failed so the next scan
            # starts with a clean state.
            if channel_id in self._preserve_tasks_on_stop:
                self._preserve_tasks_on_stop.discard(channel_id)
            else:
                try:
                    await self._fail_unprocessed_tasks(channel_id)
                except Exception:
                    logger.exception(
                        "Failed to clean up unprocessed tasks for channel {}", channel_id
                    )

    async def _worker_iteration(
        self, channel_id: UUID, target_task_ids: list[UUID] | None = None
    ) -> None:
        """Single pass: decide whether to start a batch and, if so, run it."""
        async with async_session_maker() as session:
            channel_service = ChannelService(session)
            channel = await channel_service.get(channel_id)
            if not channel or channel.status in ("error", "deleted"):
                return
            if channel.session_status != "active":
                return
            # Uploads may have been disabled while a worker from before the
            # change was still alive; such a worker must not start batches.
            if not channel.auto_upload_enabled:
                return

            now = datetime.now(timezone.utc)

            # Auto-resume non-manual pauses once the pause time has passed.
            channel = await self._maybe_auto_resume(
                channel_service, channel, now
            )
            if not channel or channel.status in ("error", "deleted"):
                return

            # Manual or intentionally-scheduled pauses: sit idle and retry
            # on the next iteration instead of exiting permanently.
            if channel.status == "paused":
                return

            limit_info = await channel_service.check_upload_limit(channel)
            if limit_info["is_limited"]:
                await channel_service.pause_for_upload_limit(
                    channel, limit_info["reset_at"]
                )
                raise ChannelPaused()

            max_tasks = limit_info.get("remaining")
            if max_tasks is not None:
                max_tasks = min(
                    channel.max_batch_size or 200,
                    max(0, max_tasks),
                )
            tasks = await self._load_active_tasks(
                session,
                channel,
                target_task_ids=target_task_ids,
                max_tasks=max_tasks,
            )

            # When the worker was started by the scan loop (no specific task
            # IDs requested) and there are no active tasks yet, scan the
            # channel folder and create new upload tasks from eligible files.
            if not tasks and not target_task_ids:
                await self._purge_stale_tasks(session, channel.id)
                await channel_service.cleanup_old_tasks(channel.id)

                # A channel whose inter-upload window is still open must not
                # claim files from its folder: the claim parks those files
                # behind this channel's delay, and a ready sharer cannot
                # take them anymore — the queue stalls for the whole window
                # even though uploads are allowed elsewhere. Skip collecting;
                # the next worker tick retries once the window has elapsed
                # (or was lowered by a settings save).
                delay = max(0, channel.inter_upload_delay_seconds or 0)
                if delay:
                    remaining = self._pace_remaining(channel, delay)
                    if remaining > 0:
                        logger.info(
                            "Channel {} skipping collection: {:.1f}s of the "
                            "inter-upload delay window left",
                            channel.id,
                            remaining,
                        )
                        return

                # Serialize collection+task creation per shared folder: two
                # sharers' workers running in the same scan pass must not
                # both claim the same files. The lock spans collect+commit
                # so the second collector sees the first one's committed
                # task rows; batch execution is serialized by the same lock
                # around _process_batch below.
                collect_lock = self._get_folder_collect_lock(
                    channel.folder_path or ""
                )
                async with collect_lock:
                    # A previous sharer may have completed transfer but still
                    # be deciding whether a copyright-blocked video must be
                    # deleted. Do not claim a replacement until that sequence
                    # finishes; the event is set before the transfer lock is
                    # released, so there is no race here.
                    # Do not retain a SQLite read transaction for the whole
                    # post-processing wait.
                    await session.commit()
                    await self._wait_for_folder_processing(
                        channel.folder_path or ""
                    )
                    new_files, limit = await self._collect_new_files(
                        session, channel, channel_service
                    )
                    min_batch = max(
                        1, getattr(channel, "min_batch_size", 1) or 1
                    )
                    if new_files and len(new_files) < min_batch:
                        logger.info(
                            "Channel {} has {} files ready, below min_batch_size "
                            "{}; waiting",
                            channel.id,
                            len(new_files),
                            min_batch,
                        )
                        return

                    if new_files:
                        if (
                            getattr(channel, "shuffle_upload_order", False)
                            and len(
                                await self._folder_sharing_channels(session, channel)
                            ) <= 1
                        ):
                            random.shuffle(new_files)
                        # Apply the batch limit only after the min_batch_size
                        # check so the threshold is compared against the true
                        # waiting count.
                        total_waiting = len(new_files)
                        new_files = new_files[:limit]
                        tasks = []
                        for file_path, file_hash in new_files:
                            task = UploadTask(
                                channel_id=channel.id,
                                file_path=file_path,
                                status="pending",
                                file_hash=file_hash,
                            )
                            session.add(task)
                            tasks.append(task)
                        await session.commit()
                        for task in tasks:
                            await session.refresh(task)

                        logger.info(
                            "Channel {} starting batch of {} files "
                            "({} total queued, limit {})",
                            channel.id,
                            len(tasks),
                            total_waiting,
                            limit,
                        )

                        await self.notifications.broadcast(
                            "tasks.created",
                            {
                                "channel_id": str(channel.id),
                                "count": len(tasks),
                                "files": [
                                    file_path for file_path, _ in new_files
                                ],
                                "tasks": [_task_payload(task) for task in tasks],
                            },
                        )
                    elif not new_files:
                        logger.debug(
                            "Channel {} has no new files to upload", channel.id
                        )

            if not tasks:
                logger.debug(
                    "Channel {} found no active tasks and no new files", channel.id
                )
                return

            try:
                access_token = await channel_service.ensure_access_token(channel)
            except Exception as exc:
                logger.warning(
                    "Access token unavailable for channel {}: {}", channel_id, exc
                )
                return

            try:
                # One upload pipeline per shared folder at a time: another
                # sharer's batch transfer must complete before this channel
                # starts transferring. The lock covers the TRANSFER ONLY —
                # the inter-upload pacing wait runs without it (previously
                # the whole _process_batch held it, so a channel pacing out
                # a long delay blocked every other sharer's queue, and
                # lowering the blocked channel's delay had no effect).
                folder_lock = self._get_folder_collect_lock(
                    channel.folder_path or ""
                )
                await self._process_batch(
                    session,
                    channel_service,
                    channel,
                    tasks,
                    access_token,
                    folder_lock=folder_lock,
                )
                logger.info(
                    "Channel {} batch of {} task(s) finished",
                    channel_id,
                    len(tasks),
                )
            except ChannelPaused:
                # Intentional pause (upload limit, quota, deleted channel, etc.).
                # The channel has already been paused; the next iteration will
                # see the pause and just wait.
                logger.info("Channel {} paused during batch", channel_id)
                raise
            except Exception:
                logger.exception("Batch processing failed for channel {}", channel_id)

    async def _load_active_tasks(
        self,
        session: AsyncSession,
        channel: Channel,
        target_task_ids: list[UUID] | None = None,
        max_tasks: int | None = None,
    ) -> list[UploadTask]:
        base_limit = min(channel.max_batch_size or 200, 200)
        # ``or`` would turn an explicit 0 ("no remaining uploads") back into
        # the unbounded base limit, so distinguish None from 0.
        limit = base_limit if max_tasks is None else max(0, max_tasks)
        query = (
            select(UploadTask)
            .where(
                UploadTask.channel_id == channel.id,
                UploadTask.status.in_(
                    ["pending", "session_created", "uploading"]
                ),
            )
            .order_by(UploadTask.created_at)
            .limit(limit)
        )
        if target_task_ids:
            query = query.where(UploadTask.id.in_(target_task_ids))
        result = await session.exec(query)
        return list(result.scalars().all())

    async def _purge_stale_tasks(
        self, session: AsyncSession, channel_id: UUID
    ) -> None:
        """Delete old unprocessed/failed/cancelled tasks so the next batch starts clean.

        ``retry`` is a legacy status from an older server version; those
        rows are neither loaded as active tasks nor claimed by the shared-
        folder filter, so they would otherwise sit in the table forever.
        """
        from sqlalchemy import delete

        result = await session.exec(
            delete(UploadTask).where(
                UploadTask.channel_id == channel_id,
                UploadTask.status.in_(
                    {"pending", "failed", "cancelled", "retry"}
                ),
            )
        )
        await session.commit()
        deleted = getattr(result, "rowcount", 0) or 0
        if deleted:
            logger.info("Purged {} stale tasks for channel {}", deleted, channel_id)

    async def _fail_dead_channel_tasks(
        self, session: AsyncSession, channel_id: UUID, message: str
    ) -> None:
        """Fail a channel's active tasks when that channel can never run.

        A channel in error/deleted state (or with a non-active session)
        never starts a worker, so its ``pending``/``session_created``/
        ``uploading`` tasks keep claiming files in a SHARED folder forever.
        Every other sharer's collection skips those files ("the limit shows
        N remaining but nothing uploads", surviving restarts). Failing the
        zombies releases the files; if the channel is later repaired, the
        files are simply re-collected (their hashes were never recorded, so
        nothing double-uploads).
        """
        result = await session.exec(
            select(UploadTask).where(
                UploadTask.channel_id == channel_id,
                UploadTask.status.in_(("pending", "session_created", "uploading")),
            )
        )
        tasks = list(result.scalars().all())
        if not tasks:
            return
        logger.info(
            "Failing {} task(s) of dead channel {}: {}",
            len(tasks),
            channel_id,
            message,
        )
        for task in tasks:
            task.status = "failed"
            task.error_message = message
            # A session from a channel that no longer authenticates is dead
            # weight; do not let a repaired run try to resume it.
            task.session_url = None
            task.progress_percent = 0
        await session.commit()
        for task in tasks:
            await self.notifications.broadcast(
                "task.failed",
                {"task": _task_payload(task), "will_retry": False},
            )

    async def _fail_unprocessed_tasks(self, channel_id: UUID) -> None:
        """Mark leftover unprocessed tasks for a channel as failed."""
        async with async_session_maker() as session:
            result = await session.exec(
                select(UploadTask).where(
                    UploadTask.channel_id == channel_id,
                    UploadTask.status.in_({"pending"}),
                )
            )
            tasks = list(result.scalars().all())
            if tasks:
                await self._fail_all_tasks(
                    session, tasks, "Upload worker finished without processing task"
                )

    async def _process_batch(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
        access_token: str,
        folder_lock: asyncio.Lock | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        limit = min(channel.max_batch_size or 200, 200)
        session_sem = asyncio.Semaphore(min(50, limit))

        # Tasks that entered the batch with a saved session URL: those sessions
        # come from a previous, interrupted pass, so the upload step must query
        # their committed offset before resuming. Sessions created inside this
        # batch are fresh and skip that round-trip.
        _loaded_with_session: set[UUID] = {
            t.id for t in tasks if t.session_url
        }

        # Prepare titles/descriptions for the collected tasks.
        for task in tasks:
            abs_file_path = str(settings.resolve_path(task.file_path))
            await _prepare_task_metadata(session, channel, task, abs_file_path)
            # Defensive: title must never be empty; fall back to the filename.
            if not task.title or not task.title.strip():
                task.title = Path(abs_file_path).stem[: _MAX_YOUTUBE_TITLE_LENGTH]
            # Enforce YouTube limits even when the client supplied the values.
            if len(task.title) > _MAX_YOUTUBE_TITLE_LENGTH:
                task.title = Path(abs_file_path).stem[: _MAX_YOUTUBE_TITLE_LENGTH]
            if task.description is None:
                task.description = ""
            if len(task.description) > _MAX_YOUTUBE_DESCRIPTION_LENGTH:
                task.description = task.description[: _MAX_YOUTUBE_DESCRIPTION_LENGTH]
        await session.commit()

        project = await session.get(GCPProject, channel.project_id)
        if project is None:
            await self._fail_all_tasks(session, tasks, "Channel project not found")
            return
        # The session-creation and upload phases below run for minutes; end
        # the read transaction opened by the project lookup so the WAL
        # snapshot is not held across them.
        await session.commit()

        # 1. Pacing: wait out inter_upload_delay_seconds since the last
        #    *completed* upload BEFORE opening any YouTube session for this
        #    batch. The wait is interruptible: saving the channel's pacing
        #    or limit settings wakes it and the fresh values are re-read.
        #    See _pacing_wait for details. The wait runs OUTSIDE the
        #    shared-folder lock; the lock is taken for the transfer only.
        paced = await self._pacing_wait(session, channel_service, channel, tasks)
        if paced is None:
            return
        tasks = paced

        # The wait can outlive the access token: YouTube tokens live ~1 hour
        # and users configure delays of an hour or more. The token was
        # ensured at iteration start, before the wait; without re-ensuring it
        # here the upload starts with expired credentials (401 -> failed task
        # + a pointless re-authorization request).
        try:
            access_token = await channel_service.ensure_access_token(channel)
        except Exception as exc:
            logger.warning(
                "Access token unavailable for channel {} after pacing: {}",
                channel.id,
                exc,
            )
            return

        folder_key = channel.folder_path or ""
        processing_gate_started = False
        if folder_lock is None:
            batch_result = await self._run_batch(
                session,
                channel_service,
                channel,
                tasks,
                access_token,
                project,
                limit,
                session_sem,
                _loaded_with_session,
            )
        else:
            async with folder_lock:
                # A sharer can already have a claimed batch when another
                # sharer's transfer finishes. Gate the transfer itself as
                # well as collection so it cannot start before deletion of a
                # detected blocked video.
                # This wait may take minutes; close any read snapshot before
                # it so SQLite WAL checkpoints are never held back.
                await session.commit()
                await self._wait_for_folder_processing(folder_key)
                # The lock serializes TRANSFERS on a shared folder only; pacing
                # itself is per channel (see _pace_remaining), so a sharer whose
                # own window has elapsed proceeds immediately even if another
                # sharer just finished uploading.
                batch_result = await self._run_batch(
                    session,
                    channel_service,
                    channel,
                    tasks,
                    access_token,
                    project,
                    limit,
                    session_sem,
                    _loaded_with_session,
                )
                if batch_result and batch_result.processing_candidates:
                    self._begin_folder_processing_wait(folder_key)
                    processing_gate_started = True

        try:
            # Only the byte transfer is protected by the shared-folder lock.
            # The folder gate prevents another sharer from starting its next
            # transfer while these processing/deletion coroutines run.
            if batch_result and batch_result.processing_candidates:
                await self._wait_for_processing_candidates(
                    channel.id, batch_result.processing_candidates
                )
        finally:
            if processing_gate_started:
                self._end_folder_processing_wait(folder_key)

        # These checks intentionally follow the post-upload processing wait.
        # Otherwise a batch that exactly reaches its limit would pause and
        # skip recording an all-country copyright block.
        if batch_result:
            limit_info = await channel_service.check_upload_limit(channel)
            if limit_info["is_limited"]:
                await channel_service.pause_for_upload_limit(
                    channel, limit_info["reset_at"]
                )
                raise ChannelPaused()

            if batch_result.hit_quota:
                if await self._handle_quota_exceeded(
                    session, channel_service, channel, tasks
                ):
                    raise ChannelPaused()
                await self._pause_channel_for_quota(
                    session, channel_service, channel, tasks
                )
                raise ChannelPaused()

            if batch_result.hit_auth:
                await self._handle_auth_error(
                    session, channel_service, channel, tasks,
                    "Authorization error during upload",
                )

    def _pace_remaining(self, channel: Channel, delay: float) -> float:
        """Seconds of this channel's inter-upload window still left.

        Pacing is strictly PER CHANNEL: the window anchors to this channel's
        own ``last_upload_at``. Another sharer of the same video folder
        uploading does NOT open a window here — each channel's delay and
        limit apply independently (channel A waiting out its hour does not
        hold channel B back).
        """
        last_upload = ensure_aware(channel.last_upload_at)
        if last_upload is None:
            return 0.0
        waited = (datetime.now(timezone.utc) - last_upload).total_seconds()
        return max(0.0, delay - waited)

    async def _pacing_wait(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
    ) -> list[UploadTask] | None:
        """Interruptible inter-upload wait; returns the (possibly trimmed)
        task list, or None when the batch must be dropped.

        Runs WITHOUT the shared-folder lock: holding the lock across the
        whole wait used to block every other sharer's queue for an entire
        delay window, and lowering the blocked channel's delay had no
        effect (the channel was not in a pacing wait at all — it was
        waiting for the lock). Raises ChannelPaused when the channel was
        paused while waiting.
        """
        pacing_event = self._pacing_event(channel.id)
        pacing_event.clear()
        while True:
            try:
                await session.refresh(
                    channel,
                    attribute_names=[
                        "status",
                        "pause_reason",
                        "auto_upload_enabled",
                        "max_batch_size",
                        "inter_upload_delay_seconds",
                        "last_upload_at",
                        "upload_limit_enabled",
                        "upload_limit_count",
                        "upload_limit_period_hours",
                    ],
                )
            except Exception:
                # The channel row disappeared mid-batch (hard delete); its
                # own flow owns the task cleanup.
                await session.rollback()
                return
            if channel.status in ("error", "deleted"):
                await session.rollback()
                return
            if not channel.auto_upload_enabled:
                # Uploads were disabled mid-wait; the tasks stay pending and
                # the scan loop stops re-creating the worker.
                await session.rollback()
                return
            if channel.status == "paused":
                await session.rollback()
                raise ChannelPaused()

            # Re-check the upload limit with fresh values: a limit enabled
            # or lowered while this batch was waiting must hold here.
            limit_info = await channel_service.check_upload_limit(channel)
            if limit_info["is_limited"]:
                await channel_service.pause_for_upload_limit(
                    channel, limit_info["reset_at"]
                )
                raise ChannelPaused()
            remaining_limit = limit_info.get("remaining")
            cap = min(channel.max_batch_size or 200, 200)
            if remaining_limit is not None:
                cap = min(cap, max(0, remaining_limit))
            if cap < len(tasks):
                trimmed = tasks[:cap]
                logger.info(
                    "Channel {} pacing: batch trimmed from {} to {} task(s) "
                    "after a settings change",
                    channel.id,
                    len(tasks),
                    len(trimmed),
                )
                tasks = trimmed
            if not tasks:
                await session.rollback()
                return

            # End the read transaction opened by the refresh and the limit
            # check before waiting: a WAL snapshot must not live across the
            # (potentially hour-long) wait.
            await session.commit()

            delay = max(0, channel.inter_upload_delay_seconds or 0)
            # A batch that consists only of retry tasks (``attempt > 0``,
            # e.g. left over from a proxy failure) skips the wait so a
            # recovered route is used immediately; a batch with at least one
            # fresh task is paced.
            is_retry_batch = bool(tasks) and all((t.attempt or 0) > 0 for t in tasks)
            remaining_delay = 0.0
            if delay and not is_retry_batch:
                remaining_delay = self._pace_remaining(channel, delay)
            if remaining_delay <= 0:
                break
            logger.info(
                "Channel {} pacing: waiting {:.1f}s before the next batch "
                "(inter_upload_delay_seconds={})",
                channel.id,
                remaining_delay,
                delay,
            )
            try:
                await asyncio.wait_for(
                    pacing_event.wait(), timeout=remaining_delay
                )
            except asyncio.TimeoutError:
                # The window elapsed; proceed to the upload.
                break
            # Settings changed while waiting: re-read and recompute.
            pacing_event.clear()
        return tasks

    async def _run_batch(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
        access_token: str,
        project: GCPProject,
        limit: int,
        session_sem: asyncio.Semaphore,
        _loaded_with_session: set[UUID],
    ) -> _BatchResult | None:
        """Create YouTube sessions for the batch tasks and upload them.

        Called with the shared-folder lock already held (see
    ``_process_batch``): one transfer pipeline per shared folder.
        """
        # 2. Create a test session. Permanently invalid files are rejected one by
        #    one until a valid candidate is found. Channel-level errors (upload
        #    limit, quota, auth) stop the whole batch.
        #
        # A task that already carries a saved session (from a previous
        # interrupted pass) keeps it: the upload step resumes that session
        # instead of opening a duplicate, so a mid-batch restart does not
        # orphan the old URL or re-pay for the bytes already committed.
        test_idx = 0
        first_session: str | None = None
        while test_idx < len(tasks):
            candidate = tasks[test_idx]
            candidate_abs_path = str(settings.resolve_path(candidate.file_path))
            if candidate.session_url:
                # Saved session exists — reuse it; validation happens by
                # resuming (a dead URL falls back to a new session there).
                first_session = candidate.session_url
                break
            try:
                first_session = await create_upload_session(
                    project,
                    channel,
                    access_token,
                    candidate_abs_path,
                    candidate.title or Path(candidate_abs_path).stem,
                    candidate.description or "",
                    await get_current_channel_proxy_url(channel.id),
                )
                candidate.session_url = first_session
                candidate.status = "session_created"
                await session.commit()
                await self.notifications.broadcast(
                    "task.updated", {"task": _task_payload(candidate)}
                )
                break
            except YouTubeUploadError as exc:
                if exc.is_invalid_file:
                    logger.warning(
                        "Rejecting invalid file {}: [{}] {}",
                        candidate.file_path,
                        exc.reason,
                        exc.message,
                    )
                    await self._reject_task(session, candidate, exc)
                    test_idx += 1
                    continue
                if exc.is_retryable:
                    # Transport died while opening the test session. Switch the
                    # proxy (or schedule recovery) and hand the whole batch back
                    # to the worker loop as retryable ``pending`` tasks, so the
                    # next pass resumes immediately once a route exists — the old
                    # behavior failed every task and re-paid the full
                    # inter-upload delay before the retry.
                    if channel.proxy_auto_switch and channel.proxy_id:
                        await ProxyService().switch_channel(channel.id)
                    else:
                        await start_proxy_recovery_for_proxy(channel.proxy_id)
                    for retry_task in tasks:
                        retry_task.status = "pending"
                        retry_task.session_url = None
                        retry_task.progress_percent = 0
                        retry_task.attempt = (retry_task.attempt or 0) + 1
                        retry_task.error_message = f"[{exc.reason}] {exc.message}"
                    await session.commit()
                    for retry_task in tasks:
                        await self.notifications.broadcast(
                            "task.failed",
                            {
                                "task": _task_payload(retry_task),
                                "will_retry": True,
                            },
                        )
                    return
                await self._handle_batch_session_error(
                    session, channel_service, channel, tasks, exc
                )
                raise ChannelPaused()
            except Exception as exc:
                await self._fail_all_tasks(
                    session, tasks, f"Session creation failed: {exc}"
                )
                return

        if first_session is None:
            # Every candidate was a permanently invalid file.
            return

        # Drop the invalid candidates that were skipped before the valid test task.
        tasks = tasks[test_idx:]
        first_task = tasks[0]

        async def _create_session(task: UploadTask) -> str | None:
            async with session_sem:
                if task.session_url:
                    # Saved session from an earlier pass: reuse it. The upload
                    # step resumes from its committed offset and only opens a
                    # fresh session if this URL turns out to be dead.
                    return task.session_url
                try:
                    task_abs_file_path = str(settings.resolve_path(task.file_path))
                    url = await create_upload_session(
                        project,
                        channel,
                        access_token,
                        task_abs_file_path,
                        task.title or Path(task_abs_file_path).stem,
                        task.description or "",
                        await get_current_channel_proxy_url(channel.id),
                    )
                    task.session_url = url
                    task.status = "session_created"
                    await self.notifications.broadcast(
                        "task.updated", {"task": _task_payload(task)}
                    )
                    return url
                except YouTubeUploadError as exc:
                    task.error_message = f"[{exc.reason}] {exc.message}"
                    task.session_url = None
                    if exc.is_upload_limit:
                        task.status = "failed"
                        task.error_message = "youtube_rate_limit"
                        await self.notifications.broadcast(
                            "task.updated", {"task": _task_payload(task)}
                        )
                        return None
                    if exc.is_quota:
                        return "__QUOTA__"
                    if exc.is_auth_error:
                        task.status = "failed"
                        await self.notifications.broadcast(
                            "task.updated", {"task": _task_payload(task)}
                        )
                        return "__AUTH__"
                    if exc.is_invalid_file:
                        task.status = "failed"
                        if task.file_hash:
                            session.add(
                                RejectedVideoHash(
                                    file_hash=task.file_hash,
                                    reason=exc.reason,
                                )
                            )
                        await self.notifications.broadcast(
                            "video.rejected",
                            {
                                "task": _task_payload(task),
                                "reason": exc.reason,
                            },
                        )
                        return None
                    if exc.is_retryable:
                        # Transport died: keep the task retryable instead of
                        # failing it, and flag the channel for a proxy switch.
                        task.status = "pending"
                        task.session_url = None
                        task.progress_percent = 0
                        task.attempt = (task.attempt or 0) + 1
                        task.error_message = f"[{exc.reason}] {exc.message}"
                        await self.notifications.broadcast(
                            "task.failed",
                            {
                                "task": _task_payload(task),
                                "will_retry": True,
                            },
                        )
                        return "__RETRY__"
                    task.status = "failed"
                    await self.notifications.broadcast(
                        "task.updated", {"task": _task_payload(task)}
                    )
                    return None
                except Exception as exc:
                    task.status = "failed"
                    task.error_message = f"Session creation failed: {exc}"
                    await self.notifications.broadcast(
                        "task.failed",
                        {
                            "task": _task_payload(task),
                            "will_retry": False,
                        },
                    )
                    return None

        rest_tasks = tasks[1:]
        session_results = await asyncio.gather(
            *[_create_session(t) for t in rest_tasks], return_exceptions=True
        )

        # Check for channel-level errors in session creation.
        if any(r == "__QUOTA__" for r in session_results if not isinstance(r, BaseException)):
            if await self._handle_quota_exceeded(session, channel_service, channel, tasks):
                raise ChannelPaused()
            await self._pause_channel_for_quota(session, channel_service, channel, tasks)
            raise ChannelPaused()
        if any(r == "__AUTH__" for r in session_results if not isinstance(r, BaseException)):
            await self._handle_auth_error(
                session,
                channel_service,
                channel,
                tasks,
                "Authorization error during upload",
            )
            return

        # Transport failures during session creation: switch the route now and
        # hand the affected tasks back as pending. The next worker pass resumes
        # them immediately once a working proxy exists — no delay re-payment.
        if any(r == "__RETRY__" for r in session_results if not isinstance(r, BaseException)):
            await session.commit()
            if channel.proxy_auto_switch and channel.proxy_id:
                await ProxyService().switch_channel(channel.id)
            else:
                await start_proxy_recovery_for_proxy(channel.proxy_id)
            return

        await session.commit()

        # 3. Create sessions for the remaining videos concurrently, then upload
        # all videos concurrently (max 200).
        upload_sem = asyncio.Semaphore(limit)

        class _SkipUpload:
            pass

        upload_tasks = [t for t in tasks if t.session_url]
        # Session URLs the tasks carried into this batch. Those were created by
        # an earlier pass; their committed offset is unknown, so the upload step
        # asks YouTube where to resume. Sessions opened by this batch start at
        # zero by construction and skip the extra status round-trip.
        restored_sessions: dict[UUID, str] = {}
        for _t in tasks:
            if _t.session_url and _t.id in _loaded_with_session:
                restored_sessions[_t.id] = _t.session_url

        async def _proxy_route_ready(channel_id: UUID) -> None:
            """Resolve when the channel has a usable proxy route again.

            Usable means: not ``proxy_recovery_pending`` AND (a live assigned
            proxy, or explicit no-proxy mode). Raises if the channel disappears
            or gets deleted so the waiter stops early instead of hanging.
            """
            while True:
                async with async_session_maker() as session:
                    channel = await session.get(Channel, channel_id)
                    if channel is None or channel.status == "deleted":
                        raise RuntimeError("Channel no longer exists")
                    if not channel.proxy_recovery_pending:
                        if channel.proxy_id is None:
                            # Manual no-proxy mode: direct route is always ready.
                            return
                        proxy = await session.get(Proxy, channel.proxy_id)
                        if proxy is not None and proxy.is_active:
                            return
                await asyncio.sleep(1)

        async def _switch_proxy_and_wait(
            channel_id: UUID, *, proxy_id: UUID | None, auto_switch: bool
        ) -> bool:
            """Replace a dead proxy and wait until the new route is usable.

            A ``network_error`` during an upload means the proxy died
            mid-transfer. Previously the task was marked ``failed`` and the next
            attempt waited a full ``inter_upload_delay_seconds`` (users configure
            10 minutes), so one dead proxy stalled the channel for a whole delay
            window. Instead the upload pauses here until a working route exists
            and then resumes through it immediately — the delay budget is meant
            to space out *successful* uploads, not to punish proxy failures.

            Returns True when the channel has a usable route again, False when
            the wait timed out or the channel is gone.
            """
            if auto_switch and proxy_id is not None:
                logger.info(
                    "Channel {}: proxy failed mid-upload; switching and resuming",
                    channel_id,
                )
                await ProxyService().switch_channel(channel_id)
            else:
                # No auto-switch: pause until the operator fixes the route by
                # hand (new proxy, different proxy, or no-proxy mode).
                logger.info(
                    "Channel {}: proxy failed mid-upload; waiting for a manual "
                    "or automatic route fix",
                    channel_id,
                )
                if proxy_id is not None:
                    await start_proxy_recovery_for_proxy(proxy_id)

            wait_timeout = float(settings.proxy_switch_wait_timeout_seconds)
            try:
                await asyncio.wait_for(
                    _proxy_route_ready(channel_id), timeout=wait_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Channel {} still has no usable proxy route after {}s; "
                    "failing this batch's affected tasks",
                    channel_id,
                    wait_timeout,
                )
                return False
            except RuntimeError:
                logger.warning("Channel {} disappeared during proxy recovery", channel_id)
                return False
            return True

        async def _upload(task: UploadTask) -> dict | BaseException:
            if task.status == "failed":
                return _SkipUpload()

            max_attempts = max(1, settings.proxy_retry_attempts)
            session_expired = False
            for attempt in range(1, max_attempts + 1):
                # Read the current proxy assignment on every attempt: the user
                # may switch the channel's proxy or drop to no-proxy mode while
                # a previous attempt is waiting out a recovery.
                async with async_session_maker() as route_session:
                    route_channel = await route_session.get(Channel, channel.id)
                    if route_channel is None or route_channel.status == "deleted":
                        return RuntimeError("Channel no longer exists")
                    route_proxy_id = route_channel.proxy_id
                    route_auto_switch = route_channel.proxy_auto_switch

                abs_file_path = str(settings.resolve_path(task.file_path))
                file_size = os.path.getsize(abs_file_path)

                # Saved session from an earlier pass: query its committed
                # offset first and resume from there instead of re-sending the
                # whole file. Sessions opened by this batch start at zero, so
                # they skip the extra status round-trip.
                start_offset = 0
                is_restored = task.id in restored_sessions
                if task.session_url and is_restored and not session_expired:
                    try:
                        offset = await query_upload_progress(
                            task.session_url,
                            access_token,
                            proxy_resolver=lambda: get_current_channel_proxy_url(
                                channel.id
                            ),
                        )
                        if offset == -1:
                            # The saved session already completed: fetch the
                            # final resource by re-issuing an empty final PUT.
                            video_resource = await upload_file_to_session(
                                task.session_url,
                                abs_file_path,
                                access_token,
                                proxy_resolver=lambda: get_current_channel_proxy_url(
                                    channel.id
                                ),
                                start_offset=file_size,
                            )
                            return video_resource
                        start_offset = offset
                        task.progress_percent = (
                            file_size and int(offset * 100 / file_size) or 0
                        )
                    except YouTubeUploadError as exc:
                        if not exc.is_session_expired:
                            if exc.is_retryable and attempt < max_attempts:
                                ready = await _switch_proxy_and_wait(
                                    channel.id,
                                    proxy_id=route_proxy_id,
                                    auto_switch=route_auto_switch,
                                )
                                if ready:
                                    continue
                                return exc
                            return exc
                        # Dead session URL: fall through and open a new one.
                        session_expired = True
                        task.session_url = None
                        task.progress_percent = 0

                if not task.session_url:
                    # No saved (or saved-but-dead) session: create one now and
                    # persist it so a later interruption resumes from it.
                    try:
                        url = await create_upload_session(
                            project,
                            channel,
                            access_token,
                            abs_file_path,
                            task.title or Path(abs_file_path).stem,
                            task.description or "",
                            await get_current_channel_proxy_url(channel.id),
                        )
                    except YouTubeUploadError as exc:
                        if exc.is_retryable and attempt < max_attempts:
                            ready = await _switch_proxy_and_wait(
                                channel.id,
                                proxy_id=route_proxy_id,
                                auto_switch=route_auto_switch,
                            )
                            if ready:
                                continue
                            return exc
                        return exc
                    async with async_session_maker() as task_session:
                        saved = await task_session.get(UploadTask, task.id)
                        if saved is not None:
                            saved.session_url = url
                            saved.status = "session_created"
                            await task_session.commit()
                    task.session_url = url
                    task.status = "session_created"
                    await self.notifications.broadcast(
                        "task.updated", {"task": _task_payload(task)}
                    )

                try:
                    booked = await upload_memory_limiter.acquire(file_size)
                    try:
                        # Live progress for the UI: without a callback the
                        # task sat at progress 0 (status ``session_created``,
                        # "nothing is uploading") for the whole transfer of a
                        # large file. The callback only mutates this task's
                        # ORM object and broadcasts SSE events — never commits:
                        # concurrent uploads share this session, and commits
                        # from a callback would race with them. The final
                        # statuses below commit the last value.
                        progress_state = {
                            "percent": task.progress_percent or 0,
                            "last_sent": 0.0,
                        }

                        async def _report_progress(
                            uploaded: int, total: int,
                            *, _task=task, _state=progress_state,
                        ) -> None:
                            percent = int(uploaded * 100 / total) if total else 0
                            if percent <= _state["percent"]:
                                return
                            _state["percent"] = percent
                            _task.progress_percent = percent
                            loop = asyncio.get_running_loop()
                            if loop.time() - _state["last_sent"] >= 2.0:
                                _state["last_sent"] = loop.time()
                                await self.notifications.broadcast(
                                    "task.updated", {"task": _task_payload(_task)}
                                )

                        async with upload_sem:
                            video_resource = await upload_file_to_session(
                                task.session_url,
                                abs_file_path,
                                access_token,
                                progress_callback=_report_progress,
                                proxy_resolver=lambda: get_current_channel_proxy_url(
                                    channel.id
                                ),
                                start_offset=start_offset,
                            )
                    finally:
                        await upload_memory_limiter.release(booked)
                    return video_resource
                except YouTubeUploadError as exc:
                    if exc.is_session_expired:
                        # The session died between the status check and the
                        # data transfer; loop to open a fresh one (same attempt
                        # budget) and upload from scratch.
                        session_expired = True
                        task.session_url = None
                        task.progress_percent = 0
                        continue
                    if exc.is_retryable and attempt < max_attempts:
                        ready = await _switch_proxy_and_wait(
                            channel.id,
                            proxy_id=route_proxy_id,
                            auto_switch=route_auto_switch,
                        )
                        if ready:
                            # Keep the session: a proxy switch must not throw
                            # away the committed offset.
                            continue
                        task.session_url = None
                        task.progress_percent = 0
                        task.status = "failed"
                        task.error_message = (
                            f"[{exc.reason}] {exc.message}"
                            if exc.reason
                            else exc.message
                        )
                        return exc
                    return exc
                except Exception as exc:
                    return exc
            return Exception("Upload attempts exhausted")


        upload_results = await asyncio.gather(
            *[_upload(t) for t in upload_tasks], return_exceptions=True
        )

        # 4. Process results.
        hit_quota = False
        hit_auth = False
        hit_proxy_error = False
        processing_candidates: list[tuple[UUID, str]] = []

        # Results correspond only to tasks with a session URL. Zipping against
        # the original list corrupts task statuses when session creation failed
        # for one of the earlier tasks.
        for task, result in zip(upload_tasks, upload_results):
            if isinstance(result, type) and issubclass(result, _SkipUpload):
                continue
            if isinstance(result, _SkipUpload):
                continue
            if isinstance(result, dict):
                video_id = result.get("id")
                task.youtube_video_id = video_id
                task.status = "completed"
                task.progress_percent = 100
                task.error_message = None
                task.completed_at = datetime.now(timezone.utc)
                # The session has served its purpose; clear it so the row does
                # not accumulate a dead URL until the task is purged.
                task.session_url = None
                # compute hash now if missing
                if not task.file_hash:
                    task.file_hash = await _compute_file_hash(
                        settings.resolve_path(task.file_path)
                    )
                # Record the hash for every channel that shares this folder
                # (including this one). A shared folder is a shared upload
                # queue: a file must be uploaded once, by whichever channel
                # gets to it first, and every other sharer must then treat
                # it as already uploaded. Recording only the uploading
                # channel's id let each sharer re-upload the same file.
                sharer_ids = await self._folder_sharing_channels(
                    session, channel
                )
                for sharer_id in sharer_ids:
                    existing_hash = await session.exec(
                        select(UploadedVideoHash).where(
                            UploadedVideoHash.channel_id == sharer_id,
                            UploadedVideoHash.file_hash == task.file_hash,
                        )
                    )
                    if not existing_hash.scalars().first():
                        session.add(
                            UploadedVideoHash(
                                channel_id=sharer_id,
                                file_hash=task.file_hash,
                                youtube_video_id=video_id or "",
                                file_path=task.file_path,
                            )
                        )
                if getattr(channel, "skip_blocked_videos", False) and video_id:
                    processing_candidates.append((task.id, video_id))
                await channel_service.consume_project_quota(channel)
                channel.last_upload_at = datetime.now(timezone.utc)
                # The channel object has been alive for the whole batch
                # (inter-upload delays plus the uploads themselves), so the
                # user may have saved other changes — e.g. the video folder —
                # while it ran. Commit and reload the row before broadcasting
                # so the event carries the current state instead of reverting
                # the client to stale values.
                await session.commit()
                try:
                    await session.refresh(channel)
                except Exception:
                    # The channel row disappeared (hard-deleted mid-batch);
                    # nothing sane left to broadcast.
                    continue
                await self.notifications.broadcast(
                    "channel.updated", {"channel": _channel_payload(channel)}
                )
                await self.notifications.broadcast(
                    "task.completed",
                    {
                        "task": _task_payload(task),
                        "video_id": video_id,
                    },
                )
            elif isinstance(result, YouTubeUploadError):
                if result.is_channel_deleted:
                    await channel_service.soft_delete(channel, result.message)
                    raise ChannelPaused()
                task.error_message = f"[{result.reason}] {result.message}"
                if result.is_upload_limit:
                    task.status = "failed"
                    task.error_message = "youtube_rate_limit"
                    task.session_url = None
                elif result.is_invalid_file:
                    task.status = "failed"
                    task.session_url = None
                    if task.file_hash:
                        session.add(
                            RejectedVideoHash(
                                file_hash=task.file_hash,
                                reason=result.reason,
                            )
                        )
                    await self.notifications.broadcast(
                        "video.rejected",
                        {
                            "task": _task_payload(task),
                            "reason": result.reason,
                        },
                    )
                elif result.is_quota:
                    hit_quota = True
                elif result.is_auth_error:
                    hit_auth = True
                    task.status = "failed"
                    task.session_url = None
                elif result.is_retryable:
                    # Every in-flight retry already switched proxies and waited
                    # for a usable route; landing here means the retries ran
                    # out. Keep the task retryable so the next worker pass
                    # picks it up instead of purging it as a dead file.
                    hit_proxy_error = True
                    task.session_url = None
                    task.progress_percent = 0
                    task.status = "pending"
                    task.attempt = (task.attempt or 0) + 1
                    task.error_message = f"[{result.reason}] {result.message}"
                    await self.notifications.broadcast(
                        "task.failed",
                        {
                            "task": _task_payload(task),
                            "will_retry": True,
                        },
                    )
                else:
                    task.status = "failed"
                    # Terminal failure: the saved session is worthless now.
                    task.session_url = None
                    await self.notifications.broadcast(
                        "task.failed",
                        {
                            "task": _task_payload(task),
                            "will_retry": False,
                        },
                    )
            else:
                task.session_url = None
                task.progress_percent = 0
                task.status = "failed"
                task.error_message = f"Upload failed: {result}"
                await self.notifications.broadcast(
                    "task.failed",
                    {
                        "task": _task_payload(task),
                        "will_retry": False,
                    },
                )

        await session.commit()

        if hit_proxy_error:
            # Retries already ran inside ``_upload`` (switch + wait). If tasks
            # still failed, the route is genuinely broken: schedule the channel
            # for another automatic search and let the pending tasks retry on
            # the next worker pass instead of failing them permanently.
            if channel.proxy_auto_switch and channel.proxy_id:
                await start_proxy_recovery_for_proxy(channel.proxy_id)
            logger.info(
                "Channel {}: {} task(s) will retry after proxy failures",
                channel.id,
                sum(1 for t in tasks if t.status == "pending"),
            )

        return _BatchResult(
            processing_candidates=processing_candidates,
            hit_quota=hit_quota,
            hit_auth=hit_auth,
        )

    async def _wait_for_processing_candidates(
        self, channel_id: UUID, candidates: list[tuple[UUID, str]]
    ) -> None:
        """Finish post-upload checks outside the shared-folder transfer lock."""

        # Network polls and YouTube deletions can run concurrently. Token
        # reads/refreshes and SQLite writes remain serialized: if a batch has
        # many videos and the token has just expired, this prevents N refresh
        # requests and concurrent SQLite writer contention.
        token_lock = asyncio.Lock()
        result_lock = asyncio.Lock()
        retry_delay = max(0.1, settings.youtube_processing_poll_seconds)

        async def _check_one(task_id: UUID, video_id: str) -> None:
            async def _fresh_access_token() -> str:
                # A processing run can outlive an OAuth token. Every polling
                # coroutine owns its short-lived session, avoiding concurrent
                # use of the batch's AsyncSession.
                async with token_lock:
                    async with async_session_maker() as token_session:
                        fresh_channel = await token_session.get(Channel, channel_id)
                        if fresh_channel is None or fresh_channel.status == "deleted":
                            raise RuntimeError("Channel no longer exists")
                        return await ChannelService(token_session).ensure_access_token(
                            fresh_channel
                        )

            # Do not advance the queue after a completed transfer until the
            # owner-visible processing status is available. A failed status
            # read is retried here rather than being mistaken for an upload
            # failure (which could otherwise duplicate the video).
            while True:
                try:
                    final_video = await wait_for_video_processing(
                        video_id,
                        "",
                        access_token_resolver=_fresh_access_token,
                        proxy_resolver=lambda: get_current_channel_proxy_url(channel_id),
                    )
                    break
                except RuntimeError as exc:
                    logger.info(
                        "Channel {} stopped while waiting for video {} processing: {}",
                        channel_id,
                        video_id,
                        exc,
                    )
                    return
                except Exception as exc:
                    logger.warning(
                        "Channel {}: retrying processing check for uploaded video "
                        "{} after error: {}",
                        channel_id,
                        video_id,
                        exc,
                    )
                    await asyncio.sleep(retry_delay)

            if not is_copyright_blocked_everywhere(final_video):
                return

            # Deletion is deliberately before adding the hash to the blocked
            # registry. The outer worker waits for every candidate coroutine,
            # so neither this channel nor its peers start selecting another
            # file before the blocked YouTube video has gone away.
            while True:
                try:
                    await delete_video(
                        video_id,
                        await _fresh_access_token(),
                        proxy_resolver=lambda: get_current_channel_proxy_url(
                            channel_id
                        ),
                    )
                    break
                except RuntimeError as exc:
                    logger.info(
                        "Channel {} stopped while deleting blocked video {}: {}",
                        channel_id,
                        video_id,
                        exc,
                    )
                    return
                except YouTubeUploadError as exc:
                    if exc.status_code == 404:
                        # The video is already absent, which satisfies the
                        # required deletion before the next queue iteration.
                        break
                    logger.warning(
                        "Channel {}: retrying deletion of blocked video {} after "
                        "error: {}",
                        channel_id,
                        video_id,
                        exc,
                    )
                except Exception as exc:
                    logger.warning(
                        "Channel {}: retrying deletion of blocked video {} after "
                        "error: {}",
                        channel_id,
                        video_id,
                        exc,
                    )
                await asyncio.sleep(retry_delay)

            async with result_lock:
                async with async_session_maker() as result_session:
                    task = await result_session.get(UploadTask, task_id)
                    if task is None or not task.file_hash:
                        return
                    existing = await result_session.exec(
                        select(BlockedVideoHash).where(
                            BlockedVideoHash.file_hash == task.file_hash
                        )
                    )
                    if not existing.scalars().first():
                        result_session.add(
                            BlockedVideoHash(
                                file_hash=task.file_hash,
                                youtube_video_id=video_id,
                            )
                        )
                    task.error_message = "copyright_blocked_all_countries"
                    await result_session.commit()
                    await result_session.refresh(task)

            await self.notifications.broadcast(
                "video.blocked",
                {
                    "task": _task_payload(task),
                    "video_id": video_id,
                    "reason": "copyright_blocked_all_countries",
                },
            )

        results = await asyncio.gather(
            *[_check_one(task_id, video_id) for task_id, video_id in candidates],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error(
                    "Unexpected error while checking an uploaded video's processing: {}",
                    result,
                )

    async def _handle_quota_exceeded(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
    ) -> bool:
        """Try to find another project with quota and request re-authorization.

        Returns True if a re-authorization request was issued.
        """
        fallback = await select_project(session, exclude_project_id=channel.project_id)
        if fallback is None:
            return False

        # Try saved credentials for the fallback project first.
        saved_token = await channel_service.try_auth_with_saved_project(
            channel, fallback.id
        )
        if saved_token:
            auth = await channel_service.get_project_auth(channel.id, fallback.id)
            if auth:
                channel.project_id = fallback.id
                channel.refresh_token_enc = auth.refresh_token_enc
                channel.access_token_enc = auth.access_token_enc
                channel.access_token_expires_at = auth.access_token_expires_at
                channel.session_status = "active"
                channel.pending_project_id = None
                channel.error_message = None
                await session.commit()
                await session.refresh(channel)
                await self.notifications.broadcast(
                    "channel.updated", {"channel": _channel_payload(channel)}
                )
            return False

        verifier = _generate_code_verifier()
        challenge = _generate_code_challenge(verifier)
        state = generate_state()
        expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=settings.oauth_state_ttl_minutes
        )
        redirect_uri = fallback.redirect_uri

        oauth_state = OAuthState(
            state=state,
            project_id=fallback.id,
            channel_id=channel.id,
            code_verifier=verifier,
            redirect_uri=redirect_uri,
            channel_title=channel.title or "",
            proxy_id=channel.proxy_id,
            proxy_auto_switch=channel.proxy_auto_switch,
            expires_at=expires_at,
        )
        session.add(oauth_state)
        await session.commit()

        authorization_url = build_authorization_url(
            fallback, redirect_uri, state, challenge
        )
        await channel_service.set_reauth_required(
            channel, fallback.id, authorization_url, expires_at=expires_at
        )
        await self._cancel_batch_tasks(
            session, channel, tasks, "quotaExceeded_reauth_required"
        )

        return True

    async def _handle_batch_session_error(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
        exc: YouTubeUploadError,
    ) -> None:
        if exc.is_channel_deleted:
            await channel_service.soft_delete(channel, exc.message)
            raise ChannelPaused()
        if exc.is_upload_limit:
            await self._pause_channel_for_limit(
                session, channel_service, channel, tasks
            )
            raise ChannelPaused()
        if exc.is_quota:
            if await self._handle_quota_exceeded(session, channel_service, channel, tasks):
                raise ChannelPaused()
            await self._pause_channel_for_quota(
                session, channel_service, channel, tasks
            )
            raise ChannelPaused()
        if exc.is_auth_error:
            await self._handle_auth_error(
                session, channel_service, channel, tasks,
                f"Auth error: {exc.message}",
            )
            return
        await self._fail_all_tasks(session, tasks, f"Session error: {exc.message}")

    async def _reject_task(
        self,
        session: AsyncSession,
        task: UploadTask,
        exc: YouTubeUploadError,
    ) -> None:
        """Mark a single task as failed and add its hash to the global reject list."""
        task.status = "failed"
        task.error_message = f"[{exc.reason}] {exc.message}"
        if task.file_hash:
            existing = await session.exec(
                select(RejectedVideoHash).where(
                    RejectedVideoHash.file_hash == task.file_hash
                )
            )
            if not existing.scalars().first():
                session.add(
                    RejectedVideoHash(
                        file_hash=task.file_hash,
                        reason=exc.reason,
                    )
                )
        await session.commit()
        await self.notifications.broadcast(
            "video.rejected",
            {
                "task": _task_payload(task),
                "reason": exc.reason,
            },
        )

    async def _cancel_batch_tasks(
        self,
        session: AsyncSession,
        channel: Channel,
        tasks: list[UploadTask],
        reason: str,
    ) -> None:
        """Mark non-terminal tasks in the current batch as cancelled."""
        terminal = {"completed", "failed", "cancelled"}
        cancelled_any = False
        for task in tasks:
            if task.status in terminal:
                continue
            task.status = "cancelled"
            task.error_message = task.error_message or reason
            cancelled_any = True
            await self.notifications.broadcast(
                "task.cancelled",
                {
                    "task": _task_payload(task),
                    "reason": reason,
                },
            )
        if cancelled_any:
            await session.commit()

    async def _pause_channel_for_limit(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
    ) -> None:
        await self._cancel_batch_tasks(
            session, channel, tasks, "youtube_rate_limit"
        )
        await channel_service.pause(
            channel, settings.pause_on_upload_limit_minutes, reason="youtube_rate_limit"
        )

    async def _pause_channel_for_quota(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
    ) -> None:
        await self._cancel_batch_tasks(session, channel, tasks, "youtube_rate_limit")
        await channel_service.pause(channel, 12 * 60, reason="youtube_rate_limit")

    async def _fail_all_tasks(
        self, session: AsyncSession, tasks: list[UploadTask], message: str
    ) -> None:
        for task in tasks:
            task.status = "failed"
            task.error_message = message
        await session.commit()
        for task in tasks:
            await session.refresh(task)
        for task in tasks:
            await self.notifications.broadcast(
                "task.failed",
                {
                    "task": _task_payload(task),
                    "task_id": str(task.id),
                    "channel_id": str(task.channel_id),
                    "error": message,
                    "will_retry": False,
                },
            )

    async def _handle_auth_error(
        self,
        session: AsyncSession,
        channel_service: ChannelService,
        channel: Channel,
        tasks: list[UploadTask],
        message: str,
    ) -> None:
        """Cancel the batch and issue a reauthorization URL."""
        await self._cancel_batch_tasks(session, channel, tasks, "reauth_required")
        await channel_service.request_reauthorization(channel, message)


coordinator = UploadCoordinator(notifications)
