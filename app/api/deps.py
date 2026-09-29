from typing import AsyncGenerator

from fastapi import Header, HTTPException, status
from fastapi.requests import Request

from app.core.config import settings
from app.core.db import async_session_maker
from app.services.channel_service import ChannelService
from app.services.coordinator import coordinator
from app.services.notification_service import notifications
from app.services.project_service import ProjectService


async def get_session() -> AsyncGenerator:
    async with async_session_maker() as session:
        yield session


def verify_token(authorization: str = Header(...)) -> None:
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != settings.api_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )


def get_project_service() -> ProjectService:
    # instantiated per request in routes via Depends + session
    raise NotImplementedError


def get_notifications():
    return notifications


def get_coordinator(request: Request):
    return coordinator
