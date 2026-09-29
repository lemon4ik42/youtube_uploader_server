from datetime import datetime
from uuid import UUID
from urllib.parse import urlparse

from pydantic import BaseModel, field_validator


def validate_proxy_url(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https", "socks5"} or not parsed.hostname:
        raise ValueError("Proxy URL must use http, https or socks5 and include a host")
    return value


class ProxyCreate(BaseModel):
    url: str

    _validate_url = field_validator("url")(validate_proxy_url)


class ProxyUpdate(BaseModel):
    url: str | None = None
    is_active: bool | None = None

    _validate_url = field_validator("url")(validate_proxy_url)


class ProxyOut(BaseModel):
    id: UUID
    url: str
    is_active: bool
    assigned_channels: int
    last_checked_at: datetime | None
    last_check_ok: bool | None
    status: str
    failure_count: int
    created_at: datetime
    updated_at: datetime


class ChannelProxyUpdate(BaseModel):
    proxy_id: UUID | None = None
    auto_switch: bool | None = None


class ProxyImportRequest(BaseModel):
    proxies: list[ProxyCreate]


class ProxyImportOut(BaseModel):
    received: int
    added: int
    duplicates: int


class ProxyCheckAllOut(BaseModel):
    checked: int
    working: int
    failed: int
    proxies: list[ProxyOut]
