"""AI metadata generation orchestration.

Each supported API type has its own provider implementation in
``app.services.ai_providers`` and exactly one saved settings row (models +
API keys). This module selects API types, keys and models, applies the
saved prompts and enforces YouTube metadata limits.

Selection logic (``channel.ai_model``):
- ``None`` — AI is disabled; the caller uses the channel templates.
- ``"auto"`` — try every active API type until one works; inside a type the
  models are tried from the most token-costly to the least (see the
  provider's ``order_models_for_auto``).
- an API type name — try that type's models and keys (even when the type
  is marked inactive).

The title and the description are requested in a single generation call:
the model answers one JSON object with both fields. A partial answer (only
a title, or only a description) is accepted — the missing part falls back
to the channel template. If the whole request fails, both parts fall back.

API keys are tried sequentially until one works. Keys are ordered by health
(``working`` first, then unknown, then quota-exceeded, then not-working).
A key that only produced model-level failures (e.g. the model was not found)
does not burn quota on the remaining keys: the same models would fail there
too, so the search moves on to the next API type instead. Key statuses are
refreshed on every request made with the key.

Retries: one pass over the keys of every connected provider is a round. When
a round ends without a working key, the search starts a new round after
``ai_key_retry_delay_seconds``; each key is attempted at most
``ai_key_max_attempts`` times per video. Only keys that hit the provider's
specific quota-exhausted error are skipped in later rounds — rate limits,
temporary overload, 5xx and network failures get their remaining attempts.
If nothing works the caller falls back to the channel templates.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from app.core.config import settings
from app.core.logging import logger
from app.core.security import decrypt
from app.models.ai_model import (
    AICustomProvider,
    AICustomProviderApiKey,
    AIModelApiKey,
    AIModelConfig,
    CUSTOM_PROVIDER_PREFIX,
    KEY_STATUS_NOT_WORKING,
    KEY_STATUS_QUOTA_EXCEEDED,
    KEY_STATUS_WORKING,
    PROMPT_TYPE_DESCRIPTION,
    PROMPT_TYPE_TITLE,
    AIPrompt,
)
from app.services.ai_providers import get_provider
from app.services.ai_providers.base import AIProvider, ProviderError
from app.services.ai_providers.openai_compatible import OpenAICompatibleProvider

# Official YouTube Data API limits for video metadata.
TITLE_MAX_CHARS = 100
DESCRIPTION_MAX_BYTES = 5000

AI_MODEL_AUTO = "auto"

DEFAULT_TITLE_PROMPT = (
    "Watch the attached video and generate a short, attention-grabbing "
    "YouTube title that accurately describes its content."
)

DEFAULT_DESCRIPTION_PROMPT = (
    "Watch the attached video and generate an informative YouTube description "
    "summarizing what happens in it."
)

# Key status priority when picking which key to try first. Lower = earlier.
_KEY_STATUS_ORDER = {
    KEY_STATUS_WORKING: 0,
    "unknown": 1,
    KEY_STATUS_QUOTA_EXCEEDED: 2,
    KEY_STATUS_NOT_WORKING: 3,
}


@dataclass
class _ConfigPlan:
    """One unit of the generation search: a provider implementation, the
    models to try and the API keys to try them with."""

    label: str
    provider: AIProvider
    models: list[str]
    keys: list


def _plan_sort_key(value) -> datetime:
    """Normalize a created_at value for ordering plans (SQLite returns
    naive datetimes)."""
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


# --- YouTube limit enforcement -------------------------------------------

def truncate_title(title: str) -> str:
    """Trim to YouTube's 100-character title limit."""
    title = " ".join(title.split())
    return title[:TITLE_MAX_CHARS]


def truncate_description(description: str) -> str:
    """Trim to YouTube's 5000-byte description limit without splitting a
    multi-byte UTF-8 character."""
    encoded = description.encode("utf-8")
    if len(encoded) <= DESCRIPTION_MAX_BYTES:
        return description
    return encoded[:DESCRIPTION_MAX_BYTES].decode("utf-8", errors="ignore")


# --- Prompt handling -------------------------------------------------------

async def get_saved_prompts(
    session, *, want_description: bool
) -> tuple[str, str | None]:
    """Return the saved title prompt and, optionally, description prompt."""
    title_row = await session.get(AIPrompt, PROMPT_TYPE_TITLE)
    description_row = (
        await session.get(AIPrompt, PROMPT_TYPE_DESCRIPTION)
        if want_description
        else None
    )
    return (
        (title_row.content if title_row else None) or DEFAULT_TITLE_PROMPT,
        (description_row.content if description_row else None) if want_description else None,
    )


def build_generation_prompt(title_prompt: str, description_prompt: str | None) -> str:
    if description_prompt:
        json_contract = (
            '{"title": "<generated video title>", "description": "<generated description>"}'
        )
        limits = (
            f"The title must be at most {TITLE_MAX_CHARS} characters and the "
            f"description at most {DESCRIPTION_MAX_BYTES} bytes. Respond in "
            "the same language as these instructions. Output only the JSON object, "
            "no other text."
        )
    else:
        json_contract = '{"title": "<generated video title>"}'
        limits = (
            f"The title must be at most {TITLE_MAX_CHARS} characters. Respond in "
            "the same language as these instructions. Output only the JSON "
            "object, no other text."
        )
    return "\n\n".join(
        part for part in (title_prompt, description_prompt) if part
    ) + (
        f"\n\nRespond with a single JSON object using exactly these fields: "
        f"{json_contract}. {limits}"
    )


def _extract_json(text: str) -> dict | None:
    text = text.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Some models wrap the JSON in markdown fences or prose.
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
    return None


def parse_generation_response(text: str) -> tuple[str, str] | None:
    """Extract (title, description) from a model text response.

    A partial answer is accepted: only a title or only a description is
    returned with the other part empty; the caller fills the missing part
    from the channel templates. Returns None when the text is not a JSON
    object or contains neither field.
    """
    obj = _extract_json(text)
    if not isinstance(obj, dict):
        return None
    title = str(obj.get("title") or "").strip()
    description_value = obj.get("description")
    description = (
        str(description_value).strip() if description_value is not None else ""
    )
    if title or description:
        return title, description
    return None


# --- Key status persistence -------------------------------------------------

async def record_key_result(
    key: AIModelApiKey | AICustomProviderApiKey, error: ProviderError | None
) -> None:
    """Persist the latest health status of an API key.

    ``key`` is the ORM object of the plan the caller is iterating; the row
    is re-loaded in a short-lived session because the network calls ran
    outside of it.
    """
    from app.core.db import async_session_maker

    key_model = (
        AICustomProviderApiKey
        if isinstance(key, AICustomProviderApiKey)
        else AIModelApiKey
    )
    status = error.key_status if error else KEY_STATUS_WORKING
    now = datetime.now(timezone.utc)
    try:
        async with async_session_maker() as session:
            key_row = await session.get(key_model, key.id)
            if key_row is None:
                return
            if status is not None:
                key_row.status = status
            key_row.last_checked_at = now
            key_row.last_error = error.message if error else None
            key_row.updated_at = now
            session.add(key_row)
            await session.commit()
    except Exception:
        logger.warning("Could not update AI key status for {}", key.id)


# --- Orchestration ----------------------------------------------------------

def _order_keys(keys: list[AIModelApiKey]) -> list[AIModelApiKey]:
    return sorted(keys, key=lambda k: _KEY_STATUS_ORDER.get(k.status, 1))


async def generate_video_metadata(
    selection: str,
    file_path: str,
    *,
    want_description: bool,
) -> dict | None:
    """Generate title/description for one video.

    Returns {"title": str, "description": str | None} (description is None when
    not requested) or None when no saved model worked — the caller then falls
    back to the channel templates.

    The keys of every connected provider are tried in rounds: when a round
    finds no working key, the search restarts after
    ``ai_key_retry_delay_seconds`` with at most ``ai_key_max_attempts``
    attempts per key, skipping only keys that hit the provider's specific
    quota-exhausted error.
    """
    from sqlalchemy import select as sa_select

    from app.core.db import async_session_maker

    resolved = settings.resolve_path(file_path)
    if resolved is None:
        return None
    video_path = Path(resolved)
    if not video_path.is_file():
        logger.warning("AI generation skipped: file {} not found", file_path)
        return None

    # Read types, keys and prompts in one short-lived session; no database
    # connection is held while the network calls run.
    async with async_session_maker() as session:
        is_custom_selection = selection.startswith(CUSTOM_PROVIDER_PREFIX)

        # Built-in API types.
        configs: list[AIModelConfig] = []
        if selection == AI_MODEL_AUTO:
            result = await session.exec(
                sa_select(AIModelConfig)
                .where(AIModelConfig.is_active == True)  # noqa: E712
                .order_by(AIModelConfig.created_at)
            )
            configs = list(result.scalars().all())
        elif not is_custom_selection:
            # A channel pinned to one API type uses it even when the type
            # is marked inactive.
            result = await session.exec(
                sa_select(AIModelConfig).where(AIModelConfig.api_type == selection)
            )
            config = result.scalars().first()
            configs = [config] if config else []

        plans: list[tuple[datetime, _ConfigPlan]] = []
        for config in configs:
            provider = get_provider(config.api_type)
            if provider is None:
                logger.warning(
                    "No provider implementation for api_type {}", config.api_type
                )
                continue
            result = await session.exec(
                sa_select(AIModelApiKey).where(AIModelApiKey.config_id == config.id)
            )
            keys = list(result.scalars().all())
            if not keys:
                continue
            try:
                models = json.loads(config.models_json)
            except (json.JSONDecodeError, TypeError):
                models = []
            if not models:
                continue
            if selection == AI_MODEL_AUTO:
                # Automatic selection searches for the first working model
                # with the highest token cost.
                models = provider.order_models_for_auto(models)
            plans.append(
                (
                    _plan_sort_key(config.created_at),
                    _ConfigPlan(
                        label=config.api_type,
                        provider=provider,
                        models=models,
                        keys=_order_keys(keys),
                    ),
                )
            )

        # Custom OpenAI-compatible providers.
        custom_rows: list[AICustomProvider] = []
        if selection == AI_MODEL_AUTO:
            result = await session.exec(
                sa_select(AICustomProvider)
                .where(AICustomProvider.is_active == True)  # noqa: E712
                .order_by(AICustomProvider.created_at)
            )
            custom_rows = list(result.scalars().all())
        elif is_custom_selection:
            try:
                provider_id = UUID(selection[len(CUSTOM_PROVIDER_PREFIX):])
            except (ValueError, AttributeError, TypeError):
                return None
            row = await session.get(AICustomProvider, provider_id)
            # A channel pinned to one provider uses it even when the
            # provider is marked inactive.
            custom_rows = [row] if row else []

        for row in custom_rows:
            provider = OpenAICompatibleProvider(row.base_url)
            result = await session.exec(
                sa_select(AICustomProviderApiKey).where(
                    AICustomProviderApiKey.provider_id == row.id
                )
            )
            keys = list(result.scalars().all())
            if not keys:
                continue
            try:
                models = json.loads(row.models_json)
            except (json.JSONDecodeError, TypeError):
                models = []
            if not models:
                continue
            if selection == AI_MODEL_AUTO:
                models = provider.order_models_for_auto(models)
            plans.append(
                (
                    _plan_sort_key(row.created_at),
                    _ConfigPlan(
                        label=f"custom:{row.name}",
                        provider=provider,
                        models=models,
                        keys=_order_keys(keys),
                    ),
                )
            )

        if not plans:
            return None

        title_prompt, description_prompt = await get_saved_prompts(
            session, want_description=want_description
        )
    plans.sort(key=lambda item: item[0])
    prompt = build_generation_prompt(title_prompt, description_prompt)
    ordered_plans = [plan for _, plan in plans]

    # Rounds: one pass over the keys of every provider. When a round ends
    # without a working key, the search starts over after
    # ai_key_retry_delay_seconds. Each key is attempted at most
    # ai_key_max_attempts times per video, and only keys that hit the
    # provider's specific quota-exceeded error are skipped in later rounds
    # — rate limits, temporary overload, 5xx and network errors get their
    # remaining attempts again.
    max_attempts = max(1, settings.ai_key_max_attempts)
    attempts_left = [[max_attempts] * len(plan.keys) for plan in ordered_plans]
    quota_dead = [set() for _ in ordered_plans]
    models_dead = [False] * len(ordered_plans)

    def _has_retryable_keys() -> bool:
        for idx, plan in enumerate(ordered_plans):
            if models_dead[idx]:
                continue
            for key_idx, key in enumerate(plan.keys):
                if key.id not in quota_dead[idx] and attempts_left[idx][key_idx] > 0:
                    return True
        return False

    round_no = 0
    while _has_retryable_keys():
        if round_no:
            logger.info(
                "No working AI key found; starting retry round {} in {}s",
                round_no + 1,
                settings.ai_key_retry_delay_seconds,
            )
            await asyncio.sleep(settings.ai_key_retry_delay_seconds)
        round_no += 1

        for idx, plan in enumerate(ordered_plans):
            if models_dead[idx]:
                continue
            label = plan.label
            for key_idx, key in enumerate(plan.keys):
                if key.id in quota_dead[idx] or attempts_left[idx][key_idx] <= 0:
                    continue
                attempts_left[idx][key_idx] -= 1
                api_key = decrypt(key.key_enc)
                try:
                    handle = await plan.provider.upload_file(api_key, video_path)
                    handle = await plan.provider.wait_file_ready(api_key, handle)
                except ProviderError as exc:
                    logger.warning(
                        "AI key upload failed for {}: {}",
                        label,
                        exc,
                    )
                    await record_key_result(key, exc)
                    if exc.key_status == KEY_STATUS_QUOTA_EXCEEDED:
                        quota_dead[idx].add(key.id)
                    # Key-level failure (quota/invalid key) or a transient
                    # error: continue with the next key of this provider.
                    continue

                transient_failure = False
                for model in plan.models:
                    try:
                        text = await plan.provider.generate(
                            api_key, model, handle, prompt
                        )
                    except ProviderError as exc:
                        logger.warning(
                            "AI model {} failed for {}: {}",
                            model,
                            label,
                            exc,
                        )
                        await record_key_result(key, exc)
                        if exc.key_status == KEY_STATUS_QUOTA_EXCEEDED:
                            # The key is out of quota: the remaining models
                            # make no sense with it, and later rounds skip it.
                            quota_dead[idx].add(key.id)
                            break
                        if exc.key_status is not None:
                            # Other key-level failure (auth): the remaining
                            # models make no sense with it, move to the next key.
                            break
                        if exc.transient:
                            # Rate limit / overload / 5xx / network: retryable
                            # in a later round; try the next model now.
                            transient_failure = True
                        # Model-level failure: try the next model with this key.
                        continue

                    parsed = parse_generation_response(text)
                    if parsed is None:
                        # The key works but this model answered with junk; try the
                        # next model. The key is marked working.
                        await record_key_result(key, None)
                        logger.warning(
                            "AI model {} returned unparseable metadata for {}",
                            model,
                            label,
                        )
                        continue

                    title, description = parsed
                    if not title and not (want_description and description):
                        # The answer contains none of the requested fields; try
                        # the next model. The key itself works.
                        await record_key_result(key, None)
                        logger.warning(
                            "AI model {} returned no usable metadata for {}",
                            model,
                            label,
                        )
                        continue

                    await record_key_result(key, None)
                    # A partial answer is accepted: the empty part stays empty
                    # and the caller falls back to the channel template for it.
                    logger.info(
                        "AI metadata generated with model {} ({}) for {}",
                        model,
                        label,
                        video_path.name,
                    )
                    return {
                        "title": truncate_title(title),
                        "description": (
                            truncate_description(description)
                            if want_description
                            else None
                        ),
                    }
                else:
                    if transient_failure:
                        # Every model failed transiently (rate limit, temporary
                        # overload, 5xx, network): the models may be fine, so
                        # the other keys are still worth trying in this round
                        # and this key keeps its remaining attempts for the
                        # next one.
                        continue
                    await record_key_result(key, None)
                    # Every model failed at model level while the key itself
                    # works. Uploading the same video with the remaining keys of
                    # this provider would only burn their quota for the same
                    # dead models, so move to the next provider.
                    models_dead[idx] = True
                    break

    logger.warning(
        "No working AI model found for {}; falling back to channel templates",
        video_path.name,
    )
    return None
