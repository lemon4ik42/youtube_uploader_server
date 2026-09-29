from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.models.ai_model import (
    AIPrompt,
    PROMPT_TYPE_DESCRIPTION,
    PROMPT_TYPE_TITLE,
)
from app.schemas.ai_prompt import AIPromptsOut, AIPromptsUpdate
from app.services.ai_service import (
    DEFAULT_DESCRIPTION_PROMPT,
    DEFAULT_TITLE_PROMPT,
)
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


async def _get_prompt_content(session: AsyncSession, prompt_type: str) -> str | None:
    row = await session.get(AIPrompt, prompt_type)
    return row.content if row else None


@router.get("", response_model=AIPromptsOut)
async def get_ai_prompts(session: AsyncSession = Depends(get_session)):
    """Saved generation prompts; unset prompts fall back to built-in defaults."""
    return AIPromptsOut(
        title_prompt=await _get_prompt_content(session, PROMPT_TYPE_TITLE)
        or DEFAULT_TITLE_PROMPT,
        description_prompt=await _get_prompt_content(
            session, PROMPT_TYPE_DESCRIPTION
        )
        or DEFAULT_DESCRIPTION_PROMPT,
    )


@router.put("", response_model=AIPromptsOut)
async def update_ai_prompts(
    data: AIPromptsUpdate, session: AsyncSession = Depends(get_session)
):
    """Update one or both generation prompts. Omitted prompts are unchanged."""
    now = datetime.now(timezone.utc)
    for prompt_type, content in (
        (PROMPT_TYPE_TITLE, data.title_prompt),
        (PROMPT_TYPE_DESCRIPTION, data.description_prompt),
    ):
        if content is None:
            continue
        row = await session.get(AIPrompt, prompt_type)
        if row:
            row.content = content
            row.updated_at = now
        else:
            row = AIPrompt(
                prompt_type=prompt_type, content=content, created_at=now, updated_at=now
            )
        session.add(row)
    await session.commit()
    payload = AIPromptsOut(
        title_prompt=await _get_prompt_content(session, PROMPT_TYPE_TITLE)
        or DEFAULT_TITLE_PROMPT,
        description_prompt=await _get_prompt_content(
            session, PROMPT_TYPE_DESCRIPTION
        )
        or DEFAULT_DESCRIPTION_PROMPT,
    )
    await notifications.broadcast(
        "ai_prompts.updated", payload.model_dump(mode="json")
    )
    return payload
