import json
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.api.routes.ai_types import key_payload, key_sort_key
from app.core.security import decrypt, encrypt
from app.models.ai_model import (
    AICustomProvider,
    AICustomProviderApiKey,
    AIModelApiKey,
)
from app.schemas.ai_model import (
    AICustomProviderCreate,
    AICustomProviderOut,
    AICustomProviderUpdate,
)
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


def provider_payload(
    provider: AICustomProvider, keys: list[AICustomProviderApiKey]
) -> AICustomProviderOut:
    """Build the public representation of a custom provider.

    API keys are never returned in full: only a masked preview is exposed.
    """
    models = json.loads(provider.models_json)
    ordered = sorted(keys, key=key_sort_key)
    return AICustomProviderOut(
        id=provider.id,
        name=provider.name,
        base_url=provider.base_url,
        models=models,
        api_keys=[key_payload(k) for k in ordered],
        is_active=provider.is_active,
        created_at=provider.created_at,
        updated_at=provider.updated_at,
    )


async def _load_keys(
    session: AsyncSession, provider_id
) -> list[AICustomProviderApiKey]:
    result = await session.exec(
        select(AICustomProviderApiKey).where(
            AICustomProviderApiKey.provider_id == provider_id
        )
    )
    return list(result.scalars().all())


async def _find_duplicate_name(
    session: AsyncSession, name: str, exclude_id: UUID | None = None
) -> AICustomProvider | None:
    statement = select(AICustomProvider).where(AICustomProvider.name == name)
    if exclude_id is not None:
        statement = statement.where(AICustomProvider.id != exclude_id)
    result = await session.exec(statement)
    return result.scalars().first()


async def _find_duplicate_api_key(
    session: AsyncSession, api_keys: list[str], exclude_provider_id
) -> str | None:
    """Compare plaintext keys against every other stored AI key (other
    custom providers and built-in API types): a key lives in one place."""
    stored: set[str] = set()

    statement = select(AICustomProviderApiKey)
    if exclude_provider_id is not None:
        statement = statement.where(
            AICustomProviderApiKey.provider_id != exclude_provider_id
        )
    result = await session.exec(statement)
    for key in result.scalars().all():
        try:
            stored.add(decrypt(key.key_enc))
        except Exception:
            continue

    result = await session.exec(select(AIModelApiKey))
    for key in result.scalars().all():
        try:
            stored.add(decrypt(key.key_enc))
        except Exception:
            continue

    for key in api_keys:
        if key in stored:
            return key
    return None


def _create_key_rows(
    provider_id, api_keys: list[str]
) -> list[AICustomProviderApiKey]:
    return [
        AICustomProviderApiKey(
            id=uuid4(),
            provider_id=provider_id,
            key_enc=encrypt(key),
        )
        for key in api_keys
    ]


@router.get("", response_model=list[AICustomProviderOut])
async def list_ai_providers(session: AsyncSession = Depends(get_session)):
    result = await session.exec(
        select(AICustomProvider).order_by(AICustomProvider.created_at)
    )
    providers = list(result.scalars().all())
    payloads = []
    for provider in providers:
        keys = await _load_keys(session, provider.id)
        payloads.append(provider_payload(provider, keys))
    return payloads


@router.post("", response_model=AICustomProviderOut, status_code=status.HTTP_201_CREATED)
async def create_ai_provider(
    data: AICustomProviderCreate, session: AsyncSession = Depends(get_session)
):
    if await _find_duplicate_name(session, data.name):
        raise HTTPException(409, "A provider with this name already exists")
    duplicate_key = await _find_duplicate_api_key(session, data.api_keys, None)
    if duplicate_key:
        raise HTTPException(
            409, "This API key is already used by another API type or provider"
        )

    provider = AICustomProvider(
        name=data.name,
        base_url=data.base_url,
        models_json=json.dumps(data.models),
        is_active=data.is_active,
    )
    session.add(provider)
    await session.flush()
    keys = _create_key_rows(provider.id, data.api_keys)
    for key in keys:
        session.add(key)
    await session.commit()
    await session.refresh(provider)
    for key in keys:
        await session.refresh(key)
    payload = provider_payload(provider, keys)
    await notifications.broadcast(
        "ai_provider.created", {"ai_provider": payload.model_dump(mode="json")}
    )
    return payload


@router.get("/{provider_id}", response_model=AICustomProviderOut)
async def get_ai_provider(
    provider_id: UUID, session: AsyncSession = Depends(get_session)
):
    provider = await session.get(AICustomProvider, provider_id)
    if not provider:
        raise HTTPException(404, "Provider not found")
    keys = await _load_keys(session, provider.id)
    return provider_payload(provider, keys)


@router.put("/{provider_id}", response_model=AICustomProviderOut)
async def update_ai_provider(
    provider_id: UUID,
    data: AICustomProviderUpdate,
    session: AsyncSession = Depends(get_session),
):
    provider = await session.get(AICustomProvider, provider_id)
    if not provider:
        raise HTTPException(404, "Provider not found")

    values = data.model_dump(exclude_unset=True)
    if "name" in values:
        if await _find_duplicate_name(session, values["name"], exclude_id=provider.id):
            raise HTTPException(409, "A provider with this name already exists")
        provider.name = values["name"]
    if "base_url" in values:
        provider.base_url = values["base_url"]
    if "models" in values:
        provider.models_json = json.dumps(values["models"])
    if "api_keys" in values:
        duplicate_key = await _find_duplicate_api_key(
            session, values["api_keys"], exclude_provider_id=provider.id
        )
        if duplicate_key:
            raise HTTPException(
                409,
                "This API key is already used by another API type or provider",
            )
        # Replacing the keys resets their health statuses: new key material
        # starts as unknown until the first request.
        for key in await _load_keys(session, provider.id):
            await session.delete(key)
        await session.flush()
        for key in _create_key_rows(provider.id, values["api_keys"]):
            session.add(key)
    if "is_active" in values:
        provider.is_active = values["is_active"]

    provider.updated_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(provider)
    keys = await _load_keys(session, provider.id)
    payload = provider_payload(provider, keys)
    await notifications.broadcast(
        "ai_provider.updated", {"ai_provider": payload.model_dump(mode="json")}
    )
    return payload


@router.delete("/{provider_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ai_provider(
    provider_id: UUID, session: AsyncSession = Depends(get_session)
):
    provider = await session.get(AICustomProvider, provider_id)
    if not provider:
        raise HTTPException(404, "Provider not found")
    for key in await _load_keys(session, provider.id):
        await session.delete(key)
    await session.delete(provider)
    await session.commit()
    await notifications.broadcast(
        "ai_provider.deleted", {"ai_provider_id": str(provider_id)}
    )
