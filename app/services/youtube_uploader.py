import asyncio
import mimetypes
import os
from typing import Awaitable, Callable

import aiofiles
import httpx

from app.core.config import settings
from app.core.logging import logger
from app.models.channel import Channel
from app.models.project import GCPProject
from app.services.http_client import proxy_request


_RETRYABLE_NETWORK_ERRORS = (httpx.NetworkError, httpx.TimeoutException)


class YouTubeUploadError(Exception):
    def __init__(
        self,
        message: str,
        *,
        reason: str = "",
        status_code: int = 0,
        is_upload_limit: bool = False,
        is_quota: bool = False,
        is_auth_error: bool = False,
        is_retryable: bool = False,
        is_channel_deleted: bool = False,
        is_invalid_file: bool = False,
        is_session_expired: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.status_code = status_code
        self.is_upload_limit = is_upload_limit
        self.is_quota = is_quota
        self.is_auth_error = is_auth_error
        self.is_retryable = is_retryable
        self.is_channel_deleted = is_channel_deleted
        self.is_invalid_file = is_invalid_file
        # A resumable session that YouTube no longer knows about (expired,
        # 404/410, or bad metadata). The caller should open a new session and
        # continue, not retry the dead URL.
        self.is_session_expired = is_session_expired

    def __str__(self) -> str:
        return f"[{self.status_code}/{self.reason}] {self.message}"


_RETRYABLE_REASONS = {
    "backendError",
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "contentUploadError",
    "mediaDownloadError",
}

_AUTH_REASONS = {
    "unauthorized",
    "invalidCredentials",
    "insufficientPermissions",
    "authError",
    "forbidden",
}

_BAD_REQUEST_REASONS = {
    "badRequest",
    "invalidTitle",
    "invalidDescription",
    "invalidSnippet",
    "invalidCategoryId",
}

_CHANNEL_DELETED_REASONS = {
    "channelNotFound",
    "contentOwnerAccountClosed",
    "accountDeleted",
    "channelNotFound",
}

_INVALID_FILE_REASONS = {
    "unsupportedFormat",
    "processingFailed",
    "invalidFile",
    "emptyFile",
    "tooSmall",
    "codec",
    "notAVideoFile",
    "audioFile",
    "imageFile",
    "badContainer",
    "noVideoStream",
    "multipleVideoStreams",
    "problematicVideoCodec",
    "problematicAudioCodec",
    "unknownContainer",
    "unknownVideoCodec",
    "unknownAudioCodec",
    "inconsistentResolution",
    "hasEditlist",
    "unsupportedVrStereoMode",
    "unsupportedSphericalProjectionType",
    "unsupportedHdrPixelFormat",
    "unsupportedHdrColorMetadata",
    "problematicHdrLookupTable",
    "badVideo",
}


def _parse_api_error(response: httpx.Response) -> YouTubeUploadError:
    try:
        data = response.json()
        error_container = data.get("error", {})
        errors = error_container.get("errors", [])
        reasons = [e.get("reason", "") for e in errors]
        message = error_container.get("message", response.text or "Unknown error")
    except Exception:
        reasons = []
        message = response.text or f"HTTP {response.status_code}"

    reason = reasons[0] if reasons else ""
    # Some YouTube auth responses omit the structured reason and only provide
    # this message. Treat it as an auth failure so the session can be renewed.
    auth_message = message.lower()
    is_auth_error = (
        response.status_code == 401
        or reason in _AUTH_REASONS
        or "invalid authentication credentials" in auth_message
        or "expected oauth 2 access token" in auth_message
    )
    return YouTubeUploadError(
        message,
        reason=reason,
        status_code=response.status_code,
        is_upload_limit=reason == "uploadLimitExceeded",
        is_quota=reason == "quotaExceeded",
        is_auth_error=is_auth_error,
        is_retryable=reason in _RETRYABLE_REASONS or response.status_code >= 500,
        is_channel_deleted=reason in _CHANNEL_DELETED_REASONS,
        is_invalid_file=reason in _INVALID_FILE_REASONS or reason in _BAD_REQUEST_REASONS,
        # A resumable upload URL that YouTube answers with 404/410 is dead:
        # sessions expire after ~24h or are garbage-collected. Retrying the same
        # URL can never succeed, so callers must open a new session instead of
        # counting this as a retryable network failure.
        is_session_expired=response.status_code in (404, 410),
    )


def _detect_mime(file_path: str) -> str:
    mime, _ = mimetypes.guess_type(file_path)
    return mime or "application/octet-stream"


def _build_metadata(channel: Channel, title: str, description: str) -> dict:
    snippet: dict = {
        "title": title,
        "description": description,
    }
    if channel.default_category_id:
        snippet["categoryId"] = channel.default_category_id
    return {
        "snippet": snippet,
        "status": {
            "privacyStatus": channel.privacy_status,
            "selfDeclaredMadeForKids": False,
        },
    }


async def create_upload_session(
    project: GCPProject,
    channel: Channel,
    access_token: str,
    file_path: str,
    title: str,
    description: str,
    proxy_url: str | None = None,
) -> str:
    file_path = str(settings.resolve_path(file_path))
    total_size = os.path.getsize(file_path)
    mime = _detect_mime(file_path)
    metadata = _build_metadata(channel, title, description)

    url = f"{project.api_upload}?uploadType=resumable&part=snippet,status"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": mime,
        "X-Upload-Content-Length": str(total_size),
    }

    try:
        response = await proxy_request(
            "POST", url, proxy_url=proxy_url, json=metadata, headers=headers
        )
    except _RETRYABLE_NETWORK_ERRORS as exc:
        raise YouTubeUploadError(
            f"Network error while creating upload session: {exc}",
            reason="network_error",
            is_retryable=True,
        ) from exc

    if response.status_code == 200 and "Location" in response.headers:
        return response.headers["Location"]

    raise _parse_api_error(response)


ProgressCallback = Callable[[int, int], Awaitable[None]]
ProxyResolver = Callable[[], Awaitable[str | None]]
AccessTokenResolver = Callable[[], Awaitable[str]]


async def get_uploaded_video_status(
    video_id: str,
    access_token: str,
    *,
    proxy_url: str | None = None,
    proxy_resolver: ProxyResolver | None = None,
) -> dict:
    """Return the owner-visible status of an uploaded video.

    ``processingDetails`` is only available to the owner, which is why this
    request deliberately uses the same channel credential as the upload.
    ``contentDetails.regionRestriction.allowed == []`` is YouTube's explicit
    representation of a video blocked in every country.
    """
    current_proxy_url = await proxy_resolver() if proxy_resolver else proxy_url
    try:
        response = await proxy_request(
            "GET",
            f"{settings.youtube_api_service_base}/videos",
            proxy_url=current_proxy_url,
            params={
                "id": video_id,
                "part": "processingDetails,status,contentDetails",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
    except _RETRYABLE_NETWORK_ERRORS as exc:
        raise YouTubeUploadError(
            f"Network error while checking uploaded video status: {exc}",
            reason="network_error",
            is_retryable=True,
        ) from exc
    if response.status_code != 200:
        raise _parse_api_error(response)
    items = response.json().get("items", [])
    if not items:
        raise YouTubeUploadError(
            f"Uploaded video {video_id} was not returned by YouTube",
            reason="video_not_found",
            status_code=404,
        )
    return items[0]


async def wait_for_video_processing(
    video_id: str,
    access_token: str,
    *,
    access_token_resolver: AccessTokenResolver | None = None,
    proxy_url: str | None = None,
    proxy_resolver: ProxyResolver | None = None,
    poll_seconds: float | None = None,
) -> dict:
    """Poll until YouTube reports that upload processing has finished.

    There is intentionally no arbitrary timeout: callers enable this mode to
    make the copyright decision only after processing is complete.  A fresh
    access token is obtained for every poll when a resolver is supplied, so a
    long processing run does not turn into a duplicate upload after token
    expiry.
    """
    delay = max(0.1, poll_seconds or settings.youtube_processing_poll_seconds)
    while True:
        try:
            current_token = (
                await access_token_resolver() if access_token_resolver else access_token
            )
            video = await get_uploaded_video_status(
                video_id,
                current_token,
                proxy_url=proxy_url,
                proxy_resolver=proxy_resolver,
            )
        except YouTubeUploadError as exc:
            # The transfer has already succeeded. Retrying this status request
            # is safe; treating it as an upload retry could create a duplicate
            # video, so transient poll failures stay in this loop instead.
            if exc.is_retryable:
                logger.warning(
                    "Temporary error checking processing of video {}: {}",
                    video_id,
                    exc,
                )
                await asyncio.sleep(delay)
                continue
            raise
        processing_status = (video.get("processingDetails") or {}).get(
            "processingStatus"
        )
        upload_status = (video.get("status") or {}).get("uploadStatus")
        if processing_status in {"succeeded", "failed", "terminated"}:
            return video
        # YouTube can reject an upload before processingDetails is populated;
        # that is terminal as well and avoids polling a permanently rejected ID.
        if upload_status in {"processed", "failed", "rejected", "deleted"}:
            return video
        await asyncio.sleep(delay)


def is_copyright_blocked_everywhere(video: dict) -> bool:
    """Whether YouTube reports a copyright block with no viewable country.

    A claimed/licensed video with ``allowed: []`` is the Data API's exact
    all-country restriction.  A copyright/claim upload rejection is also
    globally unavailable by definition, even though YouTube need not include
    a region restriction object for rejected uploads.
    """
    status = video.get("status") or {}
    if (
        status.get("uploadStatus") == "rejected"
        and status.get("rejectionReason") in {"claim", "copyright"}
    ):
        return True

    content = video.get("contentDetails") or {}
    restriction = content.get("regionRestriction") or {}
    return bool(content.get("licensedContent")) and restriction.get("allowed") == []


async def query_upload_progress(
    session_url: str,
    access_token: str,
    *,
    proxy_url: str | None = None,
    proxy_resolver: ProxyResolver | None = None,
) -> int:
    """Ask YouTube how many bytes of a resumable session it already has.

    Sends an empty PUT with ``Content-Range: bytes */total``. A 308 answer
    carries ``Range: bytes 0-{n-1}`` — the resume offset. Any other status
    means the session is not usable (see the caller).
    """
    total = "*"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Range": f"bytes */{total}",
        "Content-Length": "0",
    }
    current_proxy_url = await proxy_resolver() if proxy_resolver else proxy_url
    response = await proxy_request(
        "PUT", session_url, proxy_url=current_proxy_url,
        content=b"", headers=headers,
    )
    if response.status_code == 308:
        range_header = response.headers.get("Range")
        if range_header and "-" in range_header:
            return int(range_header.split("-")[1]) + 1
        return 0
    if response.status_code in (200, 201):
        # The session already finished: everything is uploaded.
        return -1
    raise _parse_api_error(response)


async def upload_file_to_session(
    session_url: str,
    file_path: str,
    access_token: str,
    progress_callback: ProgressCallback | None = None,
    proxy_url: str | None = None,
    proxy_resolver: ProxyResolver | None = None,
    start_offset: int = 0,
) -> dict:
    """Upload a file to an existing resumable session.

    ``start_offset`` resumes from a previously committed offset instead of
    re-sending the whole file: a saved session that died mid-transfer can be
    continued (or picked up after a proxy switch) without paying for the bytes
    YouTube already has.
    """
    file_path = str(settings.resolve_path(file_path))
    mime = _detect_mime(file_path)
    total_size = os.path.getsize(file_path)
    chunk_size = settings.upload_chunk_size_bytes

    if start_offset < 0 or start_offset > total_size:
        raise YouTubeUploadError(
            f"Invalid resume offset {start_offset} for file of {total_size} bytes",
            reason="invalid_offset",
        )

    uploaded = start_offset
    try:
        async with aiofiles.open(file_path, "rb") as f:
            if start_offset:
                await f.seek(start_offset)
            while uploaded < total_size:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                end = uploaded + len(chunk) - 1
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": mime,
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {uploaded}-{end}/{total_size}",
                }

                try:
                    current_proxy_url = (
                        await proxy_resolver() if proxy_resolver else proxy_url
                    )
                    response = await proxy_request(
                        "PUT", session_url, proxy_url=current_proxy_url,
                        content=chunk, headers=headers,
                    )
                except _RETRYABLE_NETWORK_ERRORS as exc:
                    raise YouTubeUploadError(
                        f"Network error during upload: {exc}",
                        reason="network_error",
                        is_retryable=True,
                    ) from exc

                if response.status_code == 308:
                    range_header = response.headers.get("Range")
                    if range_header and "-" in range_header:
                        uploaded = int(range_header.split("-")[1]) + 1
                    else:
                        uploaded += len(chunk)
                    if progress_callback:
                        await progress_callback(uploaded, total_size)
                    continue

                if response.status_code in (200, 201):
                    if progress_callback:
                        await progress_callback(total_size, total_size)
                    return response.json()

                raise _parse_api_error(response)
    except YouTubeUploadError:
        raise
    except _RETRYABLE_NETWORK_ERRORS as exc:
        raise YouTubeUploadError(
            f"Network error during upload: {exc}",
            reason="network_error",
            is_retryable=True,
        ) from exc

    raise YouTubeUploadError(
        "Upload ended without completion",
        reason="incomplete",
        is_retryable=True,
    )


async def delete_video(
    video_id: str,
    access_token: str,
    *,
    proxy_url: str | None = None,
    proxy_resolver: ProxyResolver | None = None,
) -> None:
    """Delete a video owned by the authorized channel.

    Callers can pass the same live proxy resolver used by resumable uploads,
    so a proxy switch during a long processing wait also applies to deletion.
    """
    url = f"{settings.youtube_api_service_base}/videos"
    current_proxy_url = await proxy_resolver() if proxy_resolver else proxy_url
    try:
        response = await proxy_request(
            "DELETE",
            url,
            proxy_url=current_proxy_url,
            params={"id": video_id},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    except _RETRYABLE_NETWORK_ERRORS as exc:
        raise YouTubeUploadError(
            f"Network error while deleting video: {exc}",
            reason="network_error",
            is_retryable=True,
        ) from exc
    if response.status_code in (200, 204):
        return
    raise _parse_api_error(response)


async def get_channel_info(access_token: str, proxy_url: str | None = None) -> dict:
    response = await proxy_request(
        "GET",
        f"{settings.youtube_api_service_base}/channels",
        proxy_url=proxy_url,
        params={"part": "snippet", "mine": "true"},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if response.status_code == 200:
        return response.json()
    raise _parse_api_error(response)
