from uuid import UUID
from datetime import datetime, timezone
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.core.db import async_session_maker
from app.core.security import decrypt, encrypt
from app.models.proxy import Proxy
from app.models.channel import Channel
from app.schemas.proxy import (
    ProxyCheckAllOut,
    ProxyCreate,
    ProxyImportOut,
    ProxyImportRequest,
    ProxyOut,
    ProxyUpdate,
)
from app.services.channel_service import invalidate_channel_proxy_cache
from app.services.notification_service import notifications
from app.services.proxy_service import (
    ProxyService,
    consume_proxy_reservation,
    invalidate_proxy_url_index,
    proxy_payload,
    replace_deleted_proxy_for_channels,
    schedule_proxy_recovery,
)

router = APIRouter(dependencies=[Depends(verify_token)])

@router.get("", response_model=list[ProxyOut])
async def list_proxies(session: AsyncSession = Depends(get_session)):
    return await ProxyService(session).list()

@router.post("", response_model=ProxyOut, status_code=status.HTTP_201_CREATED)
async def create_proxy(data: ProxyCreate, session: AsyncSession = Depends(get_session)):
    result = await session.exec(select(Proxy))
    normalized_url = data.url.strip()
    if any(
        decrypt(proxy.url_enc).strip() == normalized_url
        for proxy in result.scalars().all()
    ):
        raise HTTPException(status_code=409, detail="This proxy already exists")
    proxy = Proxy(url_enc=encrypt(data.url))
    session.add(proxy)
    await session.commit()
    await session.refresh(proxy)
    invalidate_proxy_url_index()
    payload = proxy_payload(proxy, 0)
    await notifications.broadcast("proxy.created", {"proxy": payload})
    await schedule_proxy_recovery()
    return payload


@router.post("/import", response_model=ProxyImportOut)
async def import_proxies(
    data: ProxyImportRequest, session: AsyncSession = Depends(get_session)
):
    result = await session.exec(select(Proxy))
    existing_urls = {decrypt(proxy.url_enc).strip() for proxy in result.scalars().all()}
    unique_input: dict[str, ProxyCreate] = {}
    for item in data.proxies:
        unique_input.setdefault(item.url.strip(), item)

    added_proxies: list[Proxy] = []
    for url, item in unique_input.items():
        if url in existing_urls:
            continue
        proxy = Proxy(url_enc=encrypt(url))
        session.add(proxy)
        added_proxies.append(proxy)
        existing_urls.add(url)
    await session.commit()
    for proxy in added_proxies:
        await session.refresh(proxy)
    if added_proxies:
        invalidate_proxy_url_index()

    payload = {
        "received": len(data.proxies),
        "added": len(added_proxies),
        "duplicates": len(data.proxies) - len(added_proxies),
    }
    await notifications.broadcast("proxies.imported", payload)
    if added_proxies:
        await schedule_proxy_recovery()
    return payload


@router.post("/check", response_model=ProxyCheckAllOut)
async def check_all_proxies():
    # check_all manages its own short-lived sessions and may run for a long time
    # on a large proxy list; no request session is held while it runs.
    return await ProxyService().check_all()


@router.post("/{proxy_id}/check", response_model=ProxyOut)
async def check_proxy(proxy_id: UUID):
    async with async_session_maker() as session:
        proxy = await session.get(Proxy, proxy_id)
        if not proxy:
            raise HTTPException(404, "Proxy not found")
    # The network check runs without a database connection held; the shared
    # check records its result through its own short session.
    await ProxyService().check(proxy)
    async with async_session_maker() as session:
        updated = await session.get(Proxy, proxy_id, populate_existing=True)
        count = await ProxyService(session).count(proxy_id)
    return proxy_payload(updated, count)


@router.post("/auto-select", response_model=ProxyOut)
async def auto_select_proxy():
    # choose_working performs bounded network health checks using its own
    # short-lived sessions; no request session is held while it runs.
    proxy = await ProxyService().choose_working(reserve=True)
    if not proxy:
        raise HTTPException(503, "No working active proxies available")
    async with async_session_maker() as session:
        count = await ProxyService(session).count(proxy.id)
    return proxy_payload(proxy, count)

@router.put("/{proxy_id}", response_model=ProxyOut)
async def update_proxy(proxy_id: UUID, data: ProxyUpdate, session: AsyncSession = Depends(get_session)):
    proxy = await session.get(Proxy, proxy_id)
    if not proxy:
        raise HTTPException(404, "Proxy not found")
    values = data.model_dump(exclude_unset=True)
    if "url" in values:
        url = values.pop("url")
        if url is not None:
            proxy.url_enc = encrypt(url)
    for key, value in values.items():
        setattr(proxy, key, value)
    proxy.updated_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(proxy)
    invalidate_proxy_url_index()
    # A changed URL or deactivation must reach in-flight uploads on their next
    # chunk instead of after the proxy-route cache TTL expires.
    assigned = await session.exec(
        select(Channel.id).where(Channel.proxy_id == proxy.id)
    )
    for (assigned_id,) in assigned.all():
        invalidate_channel_proxy_cache(assigned_id)
    payload = proxy_payload(proxy, await ProxyService(session).count(proxy.id))
    await notifications.broadcast("proxy.updated", {"proxy": payload})
    if proxy.is_active:
        await schedule_proxy_recovery()
    return payload

@router.delete("/{proxy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_proxy(
    proxy_id: UUID,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
):
    proxy = await session.get(Proxy, proxy_id)
    if not proxy:
        raise HTTPException(404, "Proxy not found")
    result = await session.exec(select(Channel).where(Channel.proxy_id == proxy.id))
    channels = list(result.scalars().all())

    if channels:
        # Keep the deleted id as a recovery marker. New YouTube requests wait
        # until the background loop assigns a working replacement.
        for channel in channels:
            channel.proxy_id = None
            channel.proxy_recovery_pending = True

    await session.delete(proxy)
    await session.commit()

    invalidate_proxy_url_index()
    for channel in channels:
        invalidate_channel_proxy_cache(channel.id)
    await notifications.broadcast("proxy.deleted", {"proxy_id": str(proxy_id)})

    if channels:
        for channel in channels:
            await notifications.broadcast(
                "channel.proxy_switched",
                {
                    "channel_id": str(channel.id),
                    "previous_proxy_id": str(proxy_id),
                    "proxy_id": None,
                    "proxy": None,
                    "recovery_pending": True,
                },
            )
        background_tasks.add_task(
            replace_deleted_proxy_for_channels,
            [channel.id for channel in channels],
        )
