import base64
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import httpx

from app.core.logging import logger
from app.core.security import decrypt, encrypt
from app.models.channel import Channel
from app.models.project import GCPProject
from app.services.http_client import proxy_request
from app.services.project_service import ProjectService


class ChannelSessionExpiredError(Exception):
    """Raised when a refresh token is no longer valid."""

    def __init__(self, message: str = "Channel session expired"):
        self.message = message
        super().__init__(message)


class YouTubeChannelNotFoundError(Exception):
    """Raised when the authorized Google account no longer has a channel."""


class YouTubeTransientError(Exception):
    """Raised when YouTube refused the request for a non-permanent reason.

    A proxy whose IP is rate-limited or blocked answers with 403/429 and a
    reason like ``ipRefererBlocked`` or ``rateLimitExceeded``. That says nothing
    about the channel's credentials, so it must not be reported as an expired
    session, and it must not surface as an unhandled ``HTTPStatusError``.
    """

    def __init__(self, message: str, *, status_code: int = 0, reason: str = ""):
        self.message = message
        self.status_code = status_code
        self.reason = reason
        super().__init__(message)


_AUTH_ERROR_REASONS = {
    "authError",
    "insufficientPermissions",
    "invalidCredentials",
    "unauthorized",
}

# 403s that mean "this IP/quota is unhappy", not "these credentials are bad".
_TRANSIENT_REASONS = {
    "ipRefererBlocked",
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "quotaExceeded",
    "dailyLimitExceeded",
    "backendError",
    "servingLimitExceeded",
    "accessNotConfigured",
}


async def _do_refresh(
    refresh_token: str,
    project: GCPProject,
    project_service: ProjectService,
    proxy_url: str | None = None,
) -> dict:
    """Raw token refresh. Returns the JSON payload from Google."""
    client_secret = project_service.get_client_secret(project)
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": project.client_id,
        "client_secret": client_secret,
    }
    try:
        response = await proxy_request(
            "POST", project.token_uri, proxy_url=proxy_url, data=data
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            body = exc.response.json()
            error_desc = body.get("error", "")
        except Exception:
            error_desc = ""
        if status in (400, 401, 403) or error_desc in (
            "invalid_grant",
            "invalid_request",
            "unauthorized_client",
        ):
            raise ChannelSessionExpiredError(
                f"Refresh token rejected: {error_desc or status}"
            ) from exc
        raise


def _generate_code_verifier() -> str:
    token = secrets.token_urlsafe(32)
    return token


def _generate_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_state() -> str:
    return secrets.token_urlsafe(32)


def build_authorization_url(
    project: GCPProject,
    redirect_uri: str,
    state: str,
    code_challenge: str,
) -> str:
    from urllib.parse import urlencode

    params = {
        "client_id": project.client_id,
        "redirect_uri": redirect_uri,
        "scope": project.scope,
        "response_type": "code",
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "access_type": "offline",
        "prompt": "consent",
    }
    return f"{project.auth_uri}?{urlencode(params)}"


async def exchange_code_for_tokens(
    code: str,
    redirect_uri: str,
    code_verifier: str,
    project: GCPProject,
    project_service: ProjectService,
    proxy_url: str | None = None,
) -> dict:
    client_secret = project_service.get_client_secret(project)
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": project.client_id,
        "client_secret": client_secret,
        "code_verifier": code_verifier,
    }
    response = await proxy_request(
        "POST", project.token_uri, proxy_url=proxy_url, data=data
    )
    response.raise_for_status()
    return response.json()


def _best_thumbnail(thumbnails: dict) -> str | None:
    for size in ("high", "medium", "default"):
        if size in thumbnails:
            return thumbnails[size].get("url")
    return None


async def fetch_youtube_channel_info(access_token: str, proxy_url: str | None = None) -> dict:
    """Return current channel metadata and statistics from YouTube Data API."""
    response = await proxy_request(
        "GET",
        "https://www.googleapis.com/youtube/v3/channels",
        proxy_url=proxy_url,
        params={"part": "snippet,statistics", "mine": "true"},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if response.status_code != 200:
        try:
            body = response.json()
            error = body.get("error", {})
            reasons = {item.get("reason") for item in error.get("errors", [])}
            message = error.get("message", "")
        except Exception:
            reasons = set()
            message = response.text
        if response.status_code == 404 or reasons & {
            "channelNotFound",
            "accountDeleted",
            "contentOwnerAccountClosed",
        } or "no youtube channel" in message.lower():
            raise YouTubeChannelNotFoundError(
                message or "YouTube channel no longer exists"
            )
        if response.status_code == 401 or (
            response.status_code == 403 and reasons & _AUTH_ERROR_REASONS
        ):
            raise ChannelSessionExpiredError(
                message or "YouTube rejected the access token"
            )
        # Anything else (403 from a blocked proxy IP, 429, 5xx) is transient.
        # Raising a typed error instead of letting ``raise_for_status`` throw
        # keeps the metadata loop from logging a bare HTTPStatusError traceback
        # and from mistaking a bad proxy for a dead channel session.
        reason = next(iter(reasons - {None}), "") if reasons else ""
        raise YouTubeTransientError(
            message or f"YouTube returned HTTP {response.status_code}",
            status_code=response.status_code,
            reason=reason,
        )
    data = response.json()
    items = data.get("items", [])
    if not items:
        raise YouTubeChannelNotFoundError("No YouTube channel found for this account")

    item = items[0]
    snippet = item.get("snippet", {})
    statistics = item.get("statistics", {})
    thumbnails = snippet.get("thumbnails", {})

    def count(name: str) -> int | None:
        value = statistics.get(name)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "id": item["id"],
        "title": snippet.get("title", ""),
        "handle": snippet.get("customUrl"),
        "thumbnail_url": _best_thumbnail(thumbnails),
        "subscriber_count": count("subscriberCount"),
        "view_count": count("viewCount"),
    }


async def refresh_access_token(
    channel: Channel,
    project: GCPProject,
    project_service: ProjectService,
    proxy_url: str | None = None,
) -> str:
    payload = await _do_refresh(
        decrypt(channel.refresh_token_enc), project, project_service, proxy_url
    )
    access_token = payload["access_token"]
    expires_in = payload.get("expires_in", 3600)
    channel.access_token_enc = encrypt(access_token)
    channel.access_token_expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=expires_in
    )
    logger.info("Access token refreshed for channel {}", channel.id)
    return access_token
