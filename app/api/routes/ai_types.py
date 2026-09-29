import json
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.core.security import decrypt, encrypt
from app.models.ai_model import (
    AICustomProviderApiKey,
    AIModelApiKey,
    AIModelConfig,
)
from app.schemas.ai_model import AIModelKeyOut, AITypeOut, AITypeUpdate
from app.services.ai_providers import SUPPORTED_API_TYPES
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


def key_sort_key(key) -> tuple:
    # SQLite returns naive datetimes; normalize before comparing.
    created_at = key.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return (created_at, str(key.id))


def mask_api_key(key: str) -> str:
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def key_payload(key) -> AIModelKeyOut:
    return AIModelKeyOut(
        id=key.id,
        key=mask_api_key(decrypt(key.key_enc)),
        status=key.status,
        last_checked_at=key.last_checked_at,
        last_error=key.last_error,
        created_at=key.created_at,
        updated_at=key.updated_at,
    )


def ai_type_payload(
    api_type: str, config: AIModelConfig | None, keys: list[AIModelApiKey]
) -> AITypeOut:
    """Build the public representation of one API type.

    API keys are never returned in full: only a masked preview is exposed.
    An unconfigured type (no saved row) shows empty lists.
    """
    models = json.loads(config.models_json) if config else []
    ordered = sorted(keys, key=key_sort_key)
    return AITypeOut(
        api_type=api_type,
        models=models,
        api_keys=[key_payload(k) for k in ordered],
        is_active=config.is_active if config else False,
    )


async def _get_config(
    session: AsyncSession, api_type: str
) -> AIModelConfig | None:
    result = await session.exec(
        select(AIModelConfig).where(AIModelConfig.api_type == api_type)
    )
    return result.scalars().first()


async def _load_keys(session: AsyncSession, config_id) -> list[AIModelApiKey]:
    result = await session.exec(
        select(AIModelApiKey).where(AIModelApiKey.config_id == config_id)
    )
    return list(result.scalars().all())


async def _find_duplicate_api_key(
    session: AsyncSession, api_keys: list[str], exclude_config_id
) -> str | None:
    """Compare plaintext keys against the key rows of other API types and
    of every custom provider (a key must be stored in only one place)."""
    stored: set[str] = set()

    statement = select(AIModelApiKey)
    if exclude_config_id is not None:
        statement = statement.where(AIModelApiKey.config_id != exclude_config_id)
    result = await session.exec(statement)
    for key in result.scalars().all():
        try:
            stored.add(decrypt(key.key_enc))
        except Exception:
            continue

    result = await session.exec(select(AICustomProviderApiKey))
    for key in result.scalars().all():
        try:
            stored.add(decrypt(key.key_enc))
        except Exception:
            continue

    for key in api_keys:
        if key in stored:
            return key
    return None


def _create_key_rows(config_id, api_keys: list[str]) -> list[AIModelApiKey]:
    return [
        AIModelApiKey(
            id=uuid4(),
            config_id=config_id,
            key_enc=encrypt(key),
        )
        for key in api_keys
    ]


@router.get("", response_model=list[AITypeOut])
async def list_ai_types(session: AsyncSession = Depends(get_session)):
    """Every supported API type with its saved models and API keys.

    Types without saved settings are listed with empty lists.
    """
    result = await session.exec(select(AIModelConfig))
    configs = {c.api_type: c for c in result.scalars().all()}
    payloads = []
    for api_type in SUPPORTED_API_TYPES:
        config = configs.get(api_type)
        keys = await _load_keys(session, config.id) if config else []
        payloads.append(ai_type_payload(api_type, config, keys))
    return payloads


@router.get("/{api_type}", response_model=AITypeOut)
async def get_ai_type(
    api_type: str, session: AsyncSession = Depends(get_session)
):
    if api_type not in SUPPORTED_API_TYPES:
        raise HTTPException(404, "API type not supported")
    config = await _get_config(session, api_type)
    keys = await _load_keys(session, config.id) if config else []
    return ai_type_payload(api_type, config, keys)


@router.put("/{api_type}", response_model=AITypeOut)
async def upsert_ai_type(
    api_type: str,
    data: AITypeUpdate,
    session: AsyncSession = Depends(get_session),
):
    """Create or update the settings of one API type.

    Providing ``models`` or ``api_keys`` replaces the whole stored list;
    new key material starts with status ``unknown``.
    """
    if api_type not in SUPPORTED_API_TYPES:
        raise HTTPException(400, f"Unsupported API type: {api_type}")

    values = data.model_dump(exclude_unset=True)
    config = await _get_config(session, api_type)
    if "api_keys" in values:
        duplicate_key = await _find_duplicate_api_key(
            session,
            values["api_keys"],
            exclude_config_id=config.id if config else None,
        )
        if duplicate_key:
            raise HTTPException(
                409, "This API key is already used by another API type or provider"
            )

    if config is None:
        config = AIModelConfig(
            api_type=api_type,
            models_json=json.dumps(values.get("models") or []),
        )
        session.add(config)
        await session.flush()

    if "models" in values:
        config.models_json = json.dumps(values["models"])
    if "api_keys" in values:
        # Replacing the keys resets their health statuses: new key material
        # starts as unknown until the first request.
        for key in await _load_keys(session, config.id):
            await session.delete(key)
        await session.flush()
        for key in _create_key_rows(config.id, values["api_keys"]):
            session.add(key)
    if "is_active" in values:
        config.is_active = values["is_active"]

    config.updated_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(config)
    keys = await _load_keys(session, config.id)
    payload = ai_type_payload(api_type, config, keys)
    await notifications.broadcast(
        "ai_type.updated", {"ai_type": payload.model_dump(mode="json")}
    )
    return payload


@router.delete("/{api_type}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ai_type(
    api_type: str, session: AsyncSession = Depends(get_session)
):
    """Remove the saved settings and API keys of one API type.

    Channels referencing the type fall back to their templates until it is
    configured again.
    """
    if api_type not in SUPPORTED_API_TYPES:
        raise HTTPException(404, "API type not supported")
    config = await _get_config(session, api_type)
    if not config:
        raise HTTPException(404, "No settings saved for this API type")
    for key in await _load_keys(session, config.id):
        await session.delete(key)
    await session.delete(config)
    await session.commit()
    await notifications.broadcast("ai_type.deleted", {"api_type": api_type})
