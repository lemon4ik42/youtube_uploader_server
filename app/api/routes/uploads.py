import asyncio
import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import aiofiles
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile, status
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import async_session_maker, get_session, verify_token
from app.core.config import settings
from app.models.rejected_hash import RejectedVideoHash
from app.models.upload_task import UploadTask
from app.models.uploaded_hash import UploadedVideoHash
from app.schemas.task import TaskOut
from app.services.channel_service import ChannelService
from app.services.coordinator import coordinator, forget_cached_hash, remember_cached_hash
from app.services.memory_limiter import upload_memory_limiter
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


async def _save_upload_to_channel_folder(
    channel,
    file: UploadFile,
    session: AsyncSession,
) -> tuple[str, bool, str | None]:
    """Save an uploaded file into the channel folder.

    The file is always written to disk first, then its hash is checked
    against UploadedVideoHash and RejectedVideoHash. If the video was
    already uploaded or rejected by YouTube, the file is **kept** in the
    folder and ``skipped`` is returned as ``True`` — the scan loop will
    ignore it when building the next upload batch.

    If a file with the same name already exists it is overwritten.

    Returns ``(file_name, skipped, reason)``.
    """
    original_name = Path(file.filename or "video.bin").name

    if not channel.folder_path:
        raise HTTPException(status_code=400, detail="Channel has no configured folder")

    folder = settings.resolve_path(channel.folder_path)
    if folder is None:
        raise HTTPException(status_code=400, detail="Channel has no configured folder")
    folder.mkdir(parents=True, exist_ok=True)

    target = folder / original_name
    logger.info(
        "Saving upload channel={} file={} target={} folder={}",
        channel.id,
        file.filename,
        target,
        folder,
    )

    # Enforce the global upload memory budget for concurrently ingested files.
    file_size = file.size
    logger.debug("Upload file.size for {}: {}", file.filename, file_size)
    if file_size is None:
        try:
            # UploadFile.seek only accepts an offset in Starlette >= 1.6,
            # so use the underlying synchronous file object via a thread.
            file_size = await asyncio.to_thread(lambda: file.file.seek(0, 2) or file.file.tell())
            await asyncio.to_thread(file.file.seek, 0)
        except Exception:
            file_size = 0
    logger.debug("Upload resolved file size for {}: {}", file.filename, file_size)
    if settings.upload_max_file_bytes and file_size and file_size > settings.upload_max_file_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File size {file_size} exceeds the maximum allowed size of {settings.upload_max_file_bytes} bytes",
        )
    booked = await upload_memory_limiter.acquire(file_size) if file_size and file_size > 0 else 0
    write_started_at = time.perf_counter()
    hasher = hashlib.blake2b()
    bytes_written = 0
    chunk_size = settings.upload_read_chunk_size or (1024 * 1024)
    try:
        # Read from the UploadFile's async stream and write to disk.
        # Using UploadFile.read() keeps the read on the event loop and avoids
        # racing with Starlette's background writes into the spooled temporary
        # file, which can surface as connection-reset errors on Windows.
        # The hash is updated on the fly so we do not have to read the file
        # again from disk afterwards, which significantly reduces the time
        # before the HTTP response can be sent (helps with nginx/proxy
        # read timeouts). Disk writes use aiofiles so the event loop is not
        # blocked by synchronous I/O on large chunks.
        logger.info("Opening target file for writing: {}", target)
        async with aiofiles.open(target, "wb") as buffer:
            logger.info("Starting write loop for {}", file.filename)
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                await buffer.write(chunk)
                # Hashing large chunks can briefly block the event loop; run it
                # in the default thread pool to keep other connections alive.
                if len(chunk) > 1024 * 1024:
                    await asyncio.to_thread(hasher.update, chunk)
                else:
                    hasher.update(chunk)
                bytes_written += len(chunk)
                logger.debug(
                    "Wrote {} bytes for {} (total {})",
                    len(chunk),
                    file.filename,
                    bytes_written,
                )
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        # The TCP connection was reset by the peer (network error 10053/10054 on
        # Windows) or the request was cancelled. Clean up the partial file so
        # the next retry does not see a half-written video.
        logger.warning(
            "Connection reset while saving {} ({} bytes written)",
            file.filename,
            bytes_written,
        )
        target.unlink(missing_ok=True)
        raise
    except OSError as exc:
        logger.exception(
            "OS error while saving {} to {}: {}",
            file.filename,
            target,
            exc,
        )
        target.unlink(missing_ok=True)
        raise HTTPException(
            status_code=500,
            detail=f"Could not write file {original_name}: {exc}",
        )
    finally:
        if booked and booked > 0:
            await upload_memory_limiter.release(booked)

    write_elapsed = time.perf_counter() - write_started_at
    file_hash = hasher.hexdigest()
    # The scan loop needs this digest; publishing it here saves re-reading the
    # whole file from disk on the next pass.
    remember_cached_hash(settings.relative_path(target), file_hash)
    logger.info(
        "Wrote {} bytes for {} in {:.3f}s, hash={}",
        bytes_written,
        file.filename,
        write_elapsed,
        file_hash,
    )

    uploaded_result = await session.exec(
        select(UploadedVideoHash).where(
            UploadedVideoHash.channel_id == channel.id,
            UploadedVideoHash.file_hash == file_hash,
        )
    )
    if uploaded_result.scalars().first():
        # Keep the file on disk; the scan loop will ignore it because its hash
        # is already in UploadedVideoHash.
        return original_name, True, "Video already uploaded"

    rejected_result = await session.exec(
        select(RejectedVideoHash).where(
            RejectedVideoHash.file_hash == file_hash
        )
    )
    if rejected_result.scalars().first():
        return original_name, True, "Video rejected as invalid"

    # A new file landed in the channel folder; return it as new so the worker
    # can pick it up once min_batch_size is satisfied.
    return original_name, False, None


def _file_info(channel, file_name: str) -> dict:
    """Return size and modification time for a file in the channel folder."""
    file_path = settings.resolve_path(channel.folder_path) / file_name
    size_bytes: int | None = None
    modified_at: datetime | None = None
    if file_path.is_file():
        stat = os.stat(file_path)
        size_bytes = stat.st_size
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return {
        "file_name": file_name,
        "size_bytes": size_bytes,
        "modified_at": modified_at,
    }


@router.post(
    "/{channel_id}/upload", status_code=status.HTTP_201_CREATED
)
async def manual_upload(
    channel_id: UUID,
    file: UploadFile = File(...),
    title: str | None = Form(None),
    description: str | None = Form(None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    logger.info(
        "Manual upload started channel={} file={} content_type={} declared_size={}",
        channel_id,
        file.filename,
        file.content_type,
        getattr(file, "size", None),
    )
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        logger.warning("Channel {} not found for manual upload", channel_id)
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.status == "error":
        logger.warning("Channel {} is in error state, rejecting manual upload", channel_id)
        raise HTTPException(
            status_code=400,
            detail="Channel is in error state. Clear the error first.",
        )
    if channel.session_status != "active":
        logger.warning(
            "Channel {} session is {}, rejecting manual upload",
            channel_id,
            channel.session_status,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Channel session is {channel.session_status}. Re-authorize first.",
        )

    try:
        # End the read transaction opened by the channel lookup: streaming a
        # multi-gigabyte file from the client can take minutes, and an open
        # WAL snapshot blocks checkpoints for the whole duration.
        await session.commit()
        file_name, skipped, reason = await _save_upload_to_channel_folder(
            channel, file, session
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Unexpected error saving upload for channel {} file {}",
            channel_id,
            file.filename,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save upload: {exc}",
        )

    logger.info(
        "Saved upload for channel {}: file={}, skipped={}, reason={}",
        channel_id,
        file_name,
        skipped,
        reason,
    )

    # Title and description are no longer used at save time; they are applied
    # by the channel worker from the channel templates when the upload task is
    # created. Keep them in the response for compatibility.
    return {
        **_file_info(channel, file_name),
        "skipped": skipped,
        "reason": reason,
        "title": title,
        "description": description,
    }


@router.post(
    "/{channel_id}/upload-batch",
    status_code=status.HTTP_202_ACCEPTED,
)
async def manual_upload_batch(
    channel_id: UUID,
    files: list[UploadFile] = File(...),
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    logger.info(
        "Batch upload requested for channel {} with {} files", channel_id, len(files)
    )
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        logger.warning("Channel {} not found for batch upload", channel_id)
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.status == "error":
        logger.warning("Channel {} is in error state, rejecting batch upload", channel_id)
        raise HTTPException(
            status_code=400,
            detail="Channel is in error state. Clear the error first.",
        )
    if channel.session_status != "active":
        logger.warning(
            "Channel {} session is {}, rejecting batch upload",
            channel_id,
            channel.session_status,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Channel session is {channel.session_status}. Re-authorize first.",
        )

    # Save files concurrently with bounded parallelism so that large batches do
    # not keep the HTTP request open too long (helps avoid reverse-proxy timeouts
    # such as nginx's 60-second default).
    # End the read transaction opened by the channel validation first: the
    # concurrent saves below stream files for minutes and must not hold a WAL
    # snapshot that blocks checkpoints.
    await session.commit()
    sem = asyncio.Semaphore(3)

    async def _save_one(file: UploadFile) -> dict:
        async with sem:
            async with async_session_maker() as file_session:
                file_name, skipped, reason = await _save_upload_to_channel_folder(
                    channel, file, file_session
                )
                return {
                    **_file_info(channel, file_name),
                    "skipped": skipped,
                    "reason": reason,
                }

    results = await asyncio.gather(*[_save_one(file) for file in files])
    logger.info(
        "Saved batch upload for channel {}: {} files", channel_id, len(results)
    )
    return list(results)


@router.delete("/{channel_id}/files/{file_name}")
async def delete_folder_file(
    channel_id: UUID,
    file_name: str,
    session: AsyncSession = Depends(get_session),
) -> dict:
    return await _do_delete_folder_file(channel_id, file_name, session)


class _DeleteFileBody(BaseModel):
    file_name: str


@router.delete("/{channel_id}/files")
@router.delete("/{channel_id}/files/")
async def delete_folder_file_body(
    channel_id: UUID,
    data: _DeleteFileBody = Body(...),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Delete by JSON body (reliable for names with #, % or spaces)."""
    return await _do_delete_folder_file(channel_id, data.file_name, session)


async def _do_delete_folder_file(
    channel_id: UUID,
    file_name: str,
    session: AsyncSession,
) -> dict:
    service = ChannelService(session)
    channel = await service.get(channel_id)
    if not channel or channel.status == "deleted":
        raise HTTPException(status_code=404, detail="Channel not found")
    if not channel.folder_path:
        raise HTTPException(
            status_code=400, detail="Channel has no configured folder"
        )

    folder = settings.resolve_path(channel.folder_path)
    if folder is None:
        raise HTTPException(
            status_code=400, detail="Channel has no configured folder"
        )
    target = (folder / file_name).resolve()
    try:
        target.relative_to(folder.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file path")

    if not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    file_path_str = settings.relative_path(target)

    # Cancel any active task for this file.
    active_statuses = {"pending", "session_created", "uploading"}
    result = await session.exec(
        select(UploadTask).where(
            UploadTask.channel_id == channel.id,
            UploadTask.file_path == file_path_str,
            UploadTask.status.in_(active_statuses),
        )
    )
    active_task = result.scalars().first()
    if active_task:
        active_task.status = "cancelled"
        active_task.error_message = active_task.error_message or "File deleted by user"
        await session.commit()
        await session.refresh(active_task)
        await notifications.broadcast(
            "task.cancelled",
            {
                "task": TaskOut.model_validate(active_task).model_dump(mode="json"),
                "reason": "file_deleted",
            },
        )

    target.unlink()
    # Drop the cached digest so a different file reusing this name is re-hashed.
    forget_cached_hash(file_path_str)

    await notifications.broadcast(
        "file.deleted",
        {
            "channel_id": str(channel.id),
            "file_name": file_name,
            "file_path": file_path_str,
            "task_id": str(active_task.id) if active_task else None,
        },
    )

    return {
        "ok": True,
        "channel_id": str(channel.id),
        "file_name": file_name,
        "task_id": str(active_task.id) if active_task else None,
    }
