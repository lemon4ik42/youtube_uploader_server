"""Tests for the AI generation orchestration in app/services/ai_service.py.

The Gemini SDK client is not exercised here; a fake provider replaces the
provider registry so the tests cover the key/model/config iteration logic.
SDK error translation is tested against real google.genai error objects.
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from app.core.security import decrypt, encrypt
from app.models.ai_model import AIModelApiKey, AIModelConfig, AIPrompt
from app.services import ai_service
from app.services.ai_providers.google_gemini import translate_api_error
from app.services.ai_service import (
    DEFAULT_DESCRIPTION_PROMPT,
    DEFAULT_TITLE_PROMPT,
    build_generation_prompt,
    generate_video_metadata,
    parse_generation_response,
    truncate_description,
    truncate_title,
)

import app.services.ai_service as ai_module


# --- SDK error translation ---------------------------------------------------

def test_translate_quota_exceeded():
    from google.genai import errors

    exc = errors.ClientError(
        429,
        {
            "error": {
                "code": 429,
                "message": "Quota exceeded for quota metric 'GenerateRequestsPerDay'",
                "status": "RESOURCE_EXHAUSTED",
            }
        },
        None,
    )
    translated = translate_api_error(exc)
    assert translated.key_status == "quota_exceeded"


def test_translate_invalid_api_key():
    from google.genai import errors

    exc = errors.ClientError(
        400,
        {
            "error": {
                "code": 400,
                "message": "API key not valid. Please pass a valid API key.",
                "status": "INVALID_ARGUMENT",
            }
        },
        None,
    )
    translated = translate_api_error(exc)
    assert translated.key_status == "not_working"


def test_translate_auth_errors():
    from google.genai import errors

    for code in (401, 403):
        exc = errors.ClientError(
            code, {"error": {"code": code, "status": "PERMISSION_DENIED"}}, None
        )
        assert translate_api_error(exc).key_status == "not_working"


def test_translate_model_and_server_errors_leave_status():
    from google.genai import errors

    # 404 = model not found; 500 = server error; transient network errors.
    not_found = errors.ClientError(
        404, {"error": {"code": 404, "message": "Model x not found", "status": "NOT_FOUND"}}, None
    )
    assert translate_api_error(not_found).key_status is None
    server = errors.ServerError(
        500, {"error": {"code": 500, "status": "INTERNAL"}}, httpx.Response(500)
    )
    assert translate_api_error(server).key_status is None
    assert translate_api_error(httpx.ConnectError("boom")).key_status is None
    assert translate_api_error(TimeoutError()).key_status is None
    # A 400 that is not about the key (bad video format, etc.).
    other = errors.ClientError(
        400, {"error": {"code": 400, "message": "File size exceeds the limit", "status": "INVALID_ARGUMENT"}}, None
    )
    assert translate_api_error(other).key_status is None


# --- Truncation to YouTube limits -------------------------------------------

def test_truncate_title():
    assert truncate_title("a" * 150) == "a" * 100
    assert truncate_title("short") == "short"
    # Whitespace is collapsed before measuring.
    assert truncate_title("a  \n b   c") == "a b c"


def test_truncate_description_bytes_not_chars():
    # 5000 bytes, not 5000 characters: Cyrillic is 2 bytes per character.
    text = "й" * 3000  # 6000 bytes
    truncated = truncate_description(text)
    encoded = truncated.encode("utf-8")
    assert len(encoded) <= 5000
    # Never splits a multi-byte character.
    assert truncated.endswith("й")
    assert truncate_description("short") == "short"


# --- Prompt building and response parsing -----------------------------------

def test_build_prompt_title_only():
    prompt = build_generation_prompt("TITLE RULES", None)
    assert "TITLE RULES" in prompt
    assert '"title"' in prompt
    assert '"description"' not in prompt
    assert "100" in prompt


def test_build_prompt_with_description():
    prompt = build_generation_prompt("TITLE RULES", "DESC RULES")
    assert "TITLE RULES" in prompt
    assert "DESC RULES" in prompt
    assert '"title"' in prompt and '"description"' in prompt


def test_parse_generation_response():
    assert parse_generation_response('{"title": "My Title", "description": "My Desc"}') == (
        "My Title",
        "My Desc",
    )


def test_parse_generation_response_markdown_fenced():
    text = '```json\n{"title": "Fenced"}\n```'
    assert parse_generation_response(text) == ("Fenced", "")


def test_parse_generation_response_garbage():
    assert parse_generation_response("hello") is None
    assert parse_generation_response("") is None
    assert parse_generation_response("{}") is None
    assert parse_generation_response('{"title": "", "description": ""}') is None


def test_parse_generation_response_partial_answers():
    # Only a description is accepted: the title stays empty and the caller
    # falls back to the channel template for it.
    assert parse_generation_response('{"description": "no title"}') == (
        "",
        "no title",
    )
    # Only a title.
    assert parse_generation_response('{"title": "Only Title"}') == (
        "Only Title",
        "",
    )
    # Missing description key.
    assert parse_generation_response('{"title": "T", "tags": "x"}') == ("T", "")


# --- Automatic model ordering --------------------------------------------------

def test_auto_ordering_most_costly_models_first():
    from app.services.ai_providers.google_gemini import GoogleGeminiProvider

    provider = GoogleGeminiProvider()
    ordered = provider.order_models_for_auto(
        ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.5-flash-lite"]
    )
    assert ordered == [
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
    ]


def test_auto_ordering_unknown_models_rank_above_flash():
    from app.services.ai_providers.google_gemini import GoogleGeminiProvider

    provider = GoogleGeminiProvider()
    ordered = provider.order_models_for_auto(
        ["gemini-2.5-flash", "gemini-deepthink"]
    )
    assert ordered == ["gemini-deepthink", "gemini-2.5-flash"]


def test_auto_ordering_is_stable_within_tier():
    from app.services.ai_providers.google_gemini import GoogleGeminiProvider

    provider = GoogleGeminiProvider()
    ordered = provider.order_models_for_auto(
        ["flash-a", "pro-a", "flash-b", "flash-lite-a"]
    )
    assert ordered == ["pro-a", "flash-a", "flash-b", "flash-lite-a"]


# --- Orchestration with a fake provider ---------------------------------------


class FakeHandle:
    def __init__(self):
        self.uri = "files/fake"
        self.name = "files/fake"
        self.mime_type = "video/mp4"


class FakeProvider:
    """Records calls; failures are configured per key/model."""

    def __init__(self):
        self.uploads: list[str] = []  # api keys used for upload
        self.generations: list[tuple[str, str]] = []  # (api_key, model)
        self.upload_errors: dict[str, Exception] = {}
        self.upload_error_sequences: dict[str, list[Exception]] = {}
        self.generate_errors: dict[tuple[str, str], Exception] = {}
        self.generate_error_sequences: dict[tuple[str, str], list[Exception]] = {}
        self.generate_texts: dict[tuple[str, str], str] = {}
        self.last_prompt: str | None = None
        self.upload_limiter_calls: list[int] = []

    async def upload_file(self, api_key: str, file_path):
        self.uploads.append(api_key)
        sequence = self.upload_error_sequences.get(api_key)
        if sequence:
            error = sequence.pop(0)
            if not sequence:
                del self.upload_error_sequences[api_key]
            raise error
        error = self.upload_errors.get(api_key)
        if error:
            raise error
        return FakeHandle()

    async def wait_file_ready(self, api_key: str, handle):
        return handle

    async def generate(self, api_key: str, model: str, handle, prompt: str):
        self.last_prompt = prompt
        self.generations.append((api_key, model))
        sequence = self.generate_error_sequences.get((api_key, model))
        if sequence:
            error = sequence.pop(0)
            if not sequence:
                del self.generate_error_sequences[(api_key, model)]
            raise error
        error = self.generate_errors.get((api_key, model))
        if error:
            raise error
        return self.generate_texts.get((api_key, model), "{}")

    def order_models_for_auto(self, models: list[str]) -> list[str]:
        return list(reversed(models))


def _make_config(models, keys, api_type="google_gemini", is_active=True):
    config = AIModelConfig(
        id=uuid4(),
        api_type=api_type,
        models_json=json.dumps(models),
        is_active=is_active,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    key_rows = [
        AIModelApiKey(
            id=uuid4(),
            config_id=config.id,
            key_enc=encrypt(key),
            status="unknown",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        for key in keys
    ]
    return config, key_rows


def _generation_text(title="AI Title", description="AI Desc"):
    return json.dumps({"title": title, "description": description})


@pytest.fixture
async def ai_db(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ai_svc.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    TestSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    yield TestSession
    await engine.dispose()


@pytest.fixture
def fake_provider(monkeypatch):
    provider = FakeProvider()
    monkeypatch.setattr(ai_module, "get_provider", lambda api_type: provider)
    return provider


def _patch_statuses(monkeypatch, ai_db):
    monkeypatch.setattr("app.core.db.async_session_maker", lambda: ai_db())


async def _stored_keys(ai_db):
    from sqlmodel import select

    async with ai_db() as s:
        return {
            decrypt(r.key_enc): r
            for r in (await s.exec(select(AIModelApiKey))).all()
        }


@pytest.mark.asyncio
async def test_generate_success_first_try(ai_db, fake_provider, monkeypatch, tmp_path):
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text()
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result == {"title": "AI Title", "description": "AI Desc"}

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"
    assert stored["KEY-1"].last_error is None
    assert stored["KEY-1"].last_checked_at is not None


@pytest.mark.asyncio
async def test_model_failure_reuses_uploaded_file(ai_db, fake_provider, monkeypatch, tmp_path):
    """Model 404s -> next model of the same key; no second upload."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m-bad", "m-good"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_errors[("KEY-1", "m-bad")] = ProviderError(
        "404 model not found", None
    )
    fake_provider.generate_texts[("KEY-1", "m-good")] = _generation_text(
        title="From Good Model"
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=False
    )
    assert result["title"] == "From Good Model"
    # The video was uploaded exactly once: the failure was model-level, so
    # the already-uploaded file was reused for the next model.
    assert fake_provider.uploads == ["KEY-1"]
    assert fake_provider.generations == [("KEY-1", "m-bad"), ("KEY-1", "m-good")]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"


@pytest.mark.asyncio
async def test_key_level_failure_moves_to_next_key(ai_db, fake_provider, monkeypatch, tmp_path):
    """Quota on key 1 -> key 2 is used; both statuses refresh."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1", "KEY-2"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_errors[("KEY-1", "m1")] = ProviderError(
        "429 RESOURCE_EXHAUSTED", "quota_exceeded"
    )
    fake_provider.generate_texts[("KEY-2", "m1")] = _generation_text(
        title="From Key 2"
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "From Key 2"
    assert fake_provider.uploads == ["KEY-1", "KEY-2"]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "quota_exceeded"
    assert stored["KEY-1"].last_error is not None
    assert stored["KEY-2"].status == "working"


@pytest.mark.asyncio
async def test_all_models_dead_does_not_burn_other_keys(ai_db, fake_provider, monkeypatch, tmp_path):
    """All models fail model-level -> remaining keys are skipped (quota)."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1", "m2"], ["KEY-1", "KEY-2"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    for model in ("m1", "m2"):
        fake_provider.generate_errors[("KEY-1", model)] = ProviderError(
            "404 model not found", None
        )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result is None
    # Only the first key uploaded the video: the same models would fail with
    # the second key too, so its quota is not spent.
    assert fake_provider.uploads == ["KEY-1"]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"


@pytest.mark.asyncio
async def test_upload_failure_continues_with_next_key(ai_db, fake_provider, monkeypatch, tmp_path):
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1", "KEY-2"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.upload_errors["KEY-1"] = ProviderError(
        "400 API key not valid", "not_working"
    )
    fake_provider.generate_texts[("KEY-2", "m1")] = _generation_text(title="K2")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "K2"

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "not_working"
    assert stored["KEY-2"].status == "working"


# --- Retry rounds ---------------------------------------------------------------


def _patch_retry_settings(monkeypatch, *, attempts=3, delay=30.0):
    monkeypatch.setattr(ai_module.settings, "ai_key_max_attempts", attempts)
    monkeypatch.setattr(ai_module.settings, "ai_key_retry_delay_seconds", delay)


def _patch_sleep(monkeypatch):
    """Record (and skip) the delays between retry rounds."""
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return sleeps


@pytest.mark.asyncio
async def test_transient_model_error_retried_after_delay(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """A transient 429 (temporary overload / rate limit) does not mark the
    key dead: the whole key list is re-scanned after the retry delay."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_error_sequences[("KEY-1", "m1")] = [
        ProviderError(
            "OpenAI-compatible model m1 failed: 429 The service is "
            "temporarily overloaded",
            None,
            transient=True,
        )
    ]
    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(
        title="After Retry"
    )
    _patch_retry_settings(monkeypatch, attempts=3, delay=12.5)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "After Retry"
    assert fake_provider.generations == [("KEY-1", "m1"), ("KEY-1", "m1")]
    assert fake_provider.uploads == ["KEY-1", "KEY-1"]
    assert sleeps == [12.5]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"


@pytest.mark.asyncio
async def test_transient_upload_error_retried(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """A transient network failure during the upload is retried too."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.upload_error_sequences["KEY-1"] = [
        ProviderError("OpenAI-compatible network error: boom", None, transient=True)
    ]
    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(title="Back")
    _patch_retry_settings(monkeypatch, attempts=3, delay=5.0)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "Back"
    assert fake_provider.uploads == ["KEY-1", "KEY-1"]
    assert sleeps == [5.0]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"


@pytest.mark.asyncio
async def test_quota_key_rechecked_for_next_video(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """The quota-dead skip applies to one search only: the next video's
    generation attempts the key again (its stored quota_exceeded status
    only lowers the ordering priority, it never excludes the key)."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video1 = tmp_path / "v1.mp4"
    video1.write_bytes(b"one")
    video2 = tmp_path / "v2.mp4"
    video2.write_bytes(b"two")

    fake_provider.generate_error_sequences[("KEY-1", "m1")] = [
        ProviderError("429 insufficient_quota", "quota_exceeded")
    ]
    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(
        title="Next Video OK"
    )
    _patch_retry_settings(monkeypatch, attempts=3, delay=5.0)
    _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result1 = await generate_video_metadata(
        "google_gemini", str(video1), want_description=True
    )
    assert result1 is None
    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "quota_exceeded"

    result2 = await generate_video_metadata(
        "google_gemini", str(video2), want_description=True
    )
    assert result2["title"] == "Next Video OK"
    # The key was attempted again for the second video (fresh search state).
    assert fake_provider.uploads == ["KEY-1", "KEY-1"]
    stored = await _stored_keys(ai_db)
    assert stored["KEY-1"].status == "working"


@pytest.mark.asyncio
async def test_quota_key_skipped_in_retry_rounds(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """Only keys that hit the specific quota error are skipped when the key
    list is re-scanned; a rate-limited key keeps its attempts."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-QUOTA", "KEY-RL"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_errors[("KEY-QUOTA", "m1")] = ProviderError(
        "429 insufficient_quota: You exceeded your current quota, please "
        "check your plan and billing details",
        "quota_exceeded",
    )
    fake_provider.generate_error_sequences[("KEY-RL", "m1")] = [
        ProviderError("429 Rate limit reached", None, transient=True),
        ProviderError("429 Rate limit reached", None, transient=True),
    ]
    fake_provider.generate_texts[("KEY-RL", "m1")] = _generation_text(title="RL")
    _patch_retry_settings(monkeypatch, attempts=3, delay=1.0)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "RL"
    # The quota key was attempted once (round 1) and skipped afterwards.
    assert fake_provider.uploads == ["KEY-QUOTA", "KEY-RL", "KEY-RL", "KEY-RL"]
    assert sleeps == [1.0, 1.0]

    stored = await _stored_keys(ai_db)
    assert stored["KEY-QUOTA"].status == "quota_exceeded"
    assert stored["KEY-RL"].status == "working"


@pytest.mark.asyncio
async def test_key_attempts_capped_per_video(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """Every key gets at most ai_key_max_attempts attempts per video, then
    the search gives up (channel templates win)."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_error_sequences[("KEY-1", "m1")] = [
        ProviderError("429 The service is temporarily overloaded", None, transient=True)
    ] * 10
    _patch_retry_settings(monkeypatch, attempts=3, delay=2.0)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result is None
    assert fake_provider.uploads == ["KEY-1"] * 3
    assert fake_provider.generations == [("KEY-1", "m1")] * 3
    assert sleeps == [2.0, 2.0]


@pytest.mark.asyncio
async def test_permanent_model_failures_not_retried(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """All models fail at model level (404) while the key works: the
    provider is not re-scanned in later rounds — other keys are skipped so
    their quota is not burned for the same dead models."""
    from app.services.ai_providers.base import ProviderError

    config, keys = _make_config(["m1"], ["KEY-1", "KEY-2"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    for model in ("m1",):
        fake_provider.generate_errors[("KEY-1", model)] = ProviderError(
            "404 model not found", None
        )
    _patch_retry_settings(monkeypatch, attempts=3, delay=1.0)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result is None
    assert fake_provider.uploads == ["KEY-1"]
    assert sleeps == []


@pytest.mark.asyncio
async def test_no_sleep_when_first_round_succeeds(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(title="OK")
    _patch_retry_settings(monkeypatch, attempts=3, delay=30.0)
    sleeps = _patch_sleep(monkeypatch)
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "OK"
    assert sleeps == []


@pytest.mark.asyncio
async def test_auto_only_uses_active_configs(ai_db, fake_provider, monkeypatch, tmp_path):
    config, keys = _make_config(["m1"], ["KEY-A"], is_active=False)
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    _patch_statuses(monkeypatch, ai_db)
    result = await generate_video_metadata("auto", str(video), want_description=True)
    # An inactive API type is excluded from automatic selection.
    assert result is None
    assert fake_provider.uploads == []


@pytest.mark.asyncio
async def test_specific_selection_uses_inactive_type(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    config, keys = _make_config(["m1"], ["KEY-A"], is_active=False)
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-A", "m1")] = _generation_text(title="Pinned")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    # A channel pinned to one type uses it even when the type is inactive.
    assert result["title"] == "Pinned"


@pytest.mark.asyncio
async def test_auto_tries_types_in_order(ai_db, fake_provider, monkeypatch, tmp_path):
    first, first_keys = _make_config(["m1"], ["KEY-1"], api_type="type_a")
    second, second_keys = _make_config(["m1"], ["KEY-2"], api_type="type_b")
    # "type_b" is older -> tried first under auto selection.
    older = datetime(2020, 1, 1, tzinfo=timezone.utc)
    first.created_at = datetime.now(timezone.utc)
    second.created_at = older
    async with ai_db() as s:
        s.add(first)
        s.add_all(first_keys)
        s.add(second)
        s.add_all(second_keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    from app.services.ai_providers.base import ProviderError

    fake_provider.upload_errors["KEY-2"] = ProviderError(
        "400 API key not valid", "not_working"
    )
    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(
        title="From Second"
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata("auto", str(video), want_description=True)
    assert result["title"] == "From Second"


@pytest.mark.asyncio
async def test_auto_orders_models_by_provider(ai_db, fake_provider, monkeypatch, tmp_path):
    """Auto mode asks the provider for the cost-ordered model list."""
    config, keys = _make_config(["a", "b", "c"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "c")] = _generation_text(title="C")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata("auto", str(video), want_description=True)
    assert result["title"] == "C"
    # FakeProvider reverses the stored list -> tried in reversed order.
    assert [m for _, m in fake_provider.generations] == ["c"]


@pytest.mark.asyncio
async def test_specific_config_keeps_stored_model_order(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    config, keys = _make_config(["a", "b"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "a")] = _generation_text(title="A")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata("google_gemini", str(video), want_description=True)
    assert result["title"] == "A"
    # Stored order for a specific config: "a" first, no reordering.
    assert [m for _, m in fake_provider.generations] == ["a"]


@pytest.mark.asyncio
async def test_generate_prefers_working_keys(ai_db, fake_provider, monkeypatch, tmp_path):
    config, keys = _make_config(["m1"], ["KEY-1", "KEY-2"])
    keys[0].status = "not_working"  # tried last
    keys[1].status = "working"
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-2", "m1")] = _generation_text(title="Pref")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "Pref"
    # Only the healthy key was contacted.
    assert fake_provider.uploads == ["KEY-2"]


@pytest.mark.asyncio
async def test_generate_uses_saved_prompts(ai_db, fake_provider, monkeypatch, tmp_path):
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        s.add(AIPrompt(prompt_type="title", content="TITLE PROMPT TEXT"))
        s.add(AIPrompt(prompt_type="description", content="DESC PROMPT TEXT"))
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(title="P")
    _patch_statuses(monkeypatch, ai_db)

    await generate_video_metadata("google_gemini", str(video), want_description=True)
    assert "TITLE PROMPT TEXT" in fake_provider.last_prompt
    assert "DESC PROMPT TEXT" in fake_provider.last_prompt


@pytest.mark.asyncio
async def test_generate_truncates_model_output(ai_db, fake_provider, monkeypatch, tmp_path):
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = _generation_text(
        title="T" * 300, description="D" * 8000
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert len(result["title"]) == 100
    assert len(result["description"].encode("utf-8")) <= 5000


@pytest.mark.asyncio
async def test_generate_missing_file_returns_none(fake_provider, tmp_path):
    result = await generate_video_metadata(
        "auto", str(tmp_path / "nope.mp4"), want_description=True
    )
    assert result is None


@pytest.mark.asyncio
async def test_generate_unknown_selection_returns_none(fake_provider):
    result = await generate_video_metadata(
        "no_such_type", "whatever.mp4", want_description=True
    )
    assert result is None


@pytest.mark.asyncio
async def test_generate_unknown_provider_config_is_skipped(
    ai_db, monkeypatch, tmp_path
):
    """A type whose provider implementation is missing is skipped."""
    config, keys = _make_config(["m1"], ["KEY-1"], api_type="some_future_api")
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    monkeypatch.setattr(ai_module, "get_provider", lambda api_type: None)
    result = await generate_video_metadata(
        "some_future_api", str(video), want_description=True
    )
    assert result is None


# --- Partial (title-only / description-only) answers -------------------------
@pytest.mark.asyncio
async def test_generate_title_only_answer(ai_db, fake_provider, monkeypatch, tmp_path):
    """The model returns a title but no description: the description stays
    empty and the caller (coordinator) falls back to the channel template."""
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = json.dumps({"title": "Only Title"})
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == "Only Title"
    assert result["description"] == ""


@pytest.mark.asyncio
async def test_generate_description_only_answer(ai_db, fake_provider, monkeypatch, tmp_path):
    """The model returns a description but no title: the answer is accepted,
    the title stays empty and the caller falls back to the template."""
    config, keys = _make_config(["m1"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = json.dumps(
        {"description": "Only Description"}
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=True
    )
    assert result["title"] == ""
    assert result["description"] == "Only Description"


@pytest.mark.asyncio
async def test_generate_description_only_without_description_request(
    ai_db, fake_provider, monkeypatch, tmp_path
):
    """Description-only answer when only a title was requested: useless,
    the next model is tried instead."""
    config, keys = _make_config(["m1", "m2"], ["KEY-1"])
    async with ai_db() as s:
        s.add(config)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    fake_provider.generate_texts[("KEY-1", "m1")] = json.dumps(
        {"description": "Not requested"}
    )
    fake_provider.generate_texts[("KEY-1", "m2")] = _generation_text(title="T2")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        "google_gemini", str(video), want_description=False
    )
    assert result["title"] == "T2"
    assert result["description"] is None
    assert [m for _, m in fake_provider.generations] == ["m1", "m2"]


# --- Custom OpenAI-compatible providers --------------------------------------


class FakeCustomProvider:
    """Stands in for OpenAICompatibleProvider in orchestration tests."""

    instances: list["FakeCustomProvider"] = []

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.uploads: list[str] = []
        self.generations: list[tuple[str, str]] = []
        self.generate_texts: dict[tuple[str, str], str] = {}
        self.last_prompt: str | None = None
        FakeCustomProvider.instances.append(self)

    async def upload_file(self, api_key: str, file_path):
        self.uploads.append(api_key)
        return "handle"

    async def wait_file_ready(self, api_key: str, handle):
        return handle

    async def generate(self, api_key: str, model: str, handle, prompt: str):
        self.last_prompt = prompt
        self.generations.append((api_key, model))
        return self.generate_texts.get((api_key, model), "{}")

    def order_models_for_auto(self, models: list[str]) -> list[str]:
        return list(reversed(models))


def _make_custom_provider(
    models, keys, name="prov", base_url="https://api.example.com/v1", is_active=True,
    created_at=None,
):
    from app.models.ai_model import AICustomProvider, AICustomProviderApiKey

    provider = AICustomProvider(
        id=uuid4(),
        name=name,
        base_url=base_url,
        models_json=json.dumps(models),
        is_active=is_active,
        created_at=created_at or datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    key_rows = [
        AICustomProviderApiKey(
            id=uuid4(),
            provider_id=provider.id,
            key_enc=encrypt(key),
            status="unknown",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        for key in keys
    ]
    return provider, key_rows


@pytest.fixture
def fake_custom_provider_cls(monkeypatch):
    FakeCustomProvider.instances = []
    monkeypatch.setattr(ai_module, "OpenAICompatibleProvider", FakeCustomProvider)
    return FakeCustomProvider


async def _stored_custom_keys(ai_db):
    from sqlmodel import select

    from app.models.ai_model import AICustomProviderApiKey

    async with ai_db() as s:
        return {
            decrypt(r.key_enc): r
            for r in (await s.exec(select(AICustomProviderApiKey))).all()
        }


@pytest.mark.asyncio
async def test_custom_provider_success_path(ai_db, fake_custom_provider_cls, monkeypatch, tmp_path):
    """A custom provider with a working model produces the metadata and the
    key is marked working in the provider's own key table."""
    provider, keys = _make_custom_provider(["gpt-4o"], ["sk-1"])

    # Pre-configure responses on the class: instances copy this dict.
    responses = {("sk-1", "gpt-4o"): _generation_text(title="Custom Title")}

    original_init = fake_custom_provider_cls.__init__

    def patched_init(self, base_url):
        original_init(self, base_url)
        self.generate_texts = dict(responses)

    fake_custom_provider_cls.__init__ = patched_init

    async with ai_db() as s:
        s.add(provider)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        f"custom:{provider.id}", str(video), want_description=True
    )
    assert result == {"title": "Custom Title", "description": "AI Desc"}

    stored = await _stored_custom_keys(ai_db)
    assert stored["sk-1"].status == "working"
    assert stored["sk-1"].last_checked_at is not None


@pytest.mark.asyncio
async def test_custom_provider_inactive_used_when_pinned(
    ai_db, fake_custom_provider_cls, monkeypatch, tmp_path
):
    provider, keys = _make_custom_provider(["m1"], ["sk-1"], is_active=False)
    responses = {("sk-1", "m1"): _generation_text(title="Pinned")}
    original_init = fake_custom_provider_cls.__init__

    def patched_init(self, base_url):
        original_init(self, base_url)
        self.generate_texts = dict(responses)

    fake_custom_provider_cls.__init__ = patched_init

    async with ai_db() as s:
        s.add(provider)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        f"custom:{provider.id}", str(video), want_description=True
    )
    assert result["title"] == "Pinned"


@pytest.mark.asyncio
async def test_auto_includes_custom_providers(
    ai_db, fake_provider, fake_custom_provider_cls, monkeypatch, tmp_path
):
    """Auto selection tries custom providers alongside built-in types."""
    # No built-in config at all; only a custom provider.
    provider, keys = _make_custom_provider(["m1"], ["sk-1"], created_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    responses = {("sk-1", "m1"): _generation_text(title="From Custom")}
    original_init = fake_custom_provider_cls.__init__

    def patched_init(self, base_url):
        original_init(self, base_url)
        self.generate_texts = dict(responses)

    fake_custom_provider_cls.__init__ = patched_init

    async with ai_db() as s:
        s.add(provider)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata("auto", str(video), want_description=True)
    assert result["title"] == "From Custom"


@pytest.mark.asyncio
async def test_auto_orders_custom_after_older_type(
    ai_db, fake_provider, fake_custom_provider_cls, monkeypatch, tmp_path
):
    """Plans (built-in types and customs) are tried in creation order."""
    from app.services.ai_providers.base import ProviderError

    # Built-in type created in 2019, custom provider in 2020 -> type first.
    config, type_keys = _make_config(["m1"], ["KEY-T"], api_type="type_a")
    config.created_at = datetime(2019, 1, 1, tzinfo=timezone.utc)
    provider_row, custom_keys = _make_custom_provider(
        ["m1"], ["sk-1"], created_at=datetime(2020, 1, 1, tzinfo=timezone.utc)
    )
    async with ai_db() as s:
        s.add(config)
        s.add_all(type_keys)
        s.add(provider_row)
        s.add_all(custom_keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")

    # Built-in type fails on upload with a key-level error -> next plan.
    fake_provider.upload_errors["KEY-T"] = ProviderError(
        "401", "not_working"
    )
    responses = {("sk-1", "m1"): _generation_text(title="Fallback Order")}
    original_init = fake_custom_provider_cls.__init__

    def patched_init(self, base_url):
        original_init(self, base_url)
        self.generate_texts = dict(responses)

    fake_custom_provider_cls.__init__ = patched_init
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata("auto", str(video), want_description=True)
    assert result["title"] == "Fallback Order"


@pytest.mark.asyncio
async def test_custom_selection_with_unknown_provider_returns_none(
    ai_db, fake_custom_provider_cls, monkeypatch, tmp_path
):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")
    _patch_statuses(monkeypatch, ai_db)
    result = await generate_video_metadata(
        f"custom:{uuid4()}", str(video), want_description=True
    )
    assert result is None


@pytest.mark.asyncio
async def test_custom_selection_with_invalid_id_returns_none(
    ai_db, fake_custom_provider_cls, tmp_path
):
    result = await generate_video_metadata(
        "custom:not-a-uuid", "whatever.mp4", want_description=True
    )
    assert result is None


# --- Real OpenAICompatibleProvider: upload_file + generation ------------------


@pytest.mark.asyncio
async def test_real_custom_provider_oversize_video_skipped(
    ai_db, monkeypatch, tmp_path
):
    """A video above the inline limit is rejected at upload (key status
    unchanged); the orchestrator falls back to the channel templates."""
    provider, keys = _make_custom_provider(["gpt-4o"], ["sk-1"])
    async with ai_db() as s:
        s.add(provider)
        s.add_all(keys)
        await s.commit()

    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake-video")
    monkeypatch.setattr(
        "app.services.ai_providers.openai_compatible.settings.ai_openai_max_inline_video_bytes",
        4,
    )
    _patch_statuses(monkeypatch, ai_db)

    result = await generate_video_metadata(
        f"custom:{provider.id}", str(video), want_description=True
    )
    assert result is None

    stored = await _stored_custom_keys(ai_db)
    # The size limit says nothing about the key: status unchanged.
    assert stored["sk-1"].status == "unknown"
    assert stored["sk-1"].last_error is not None


def test_openai_provider_upload_raises_for_missing_file():
    import asyncio as _asyncio

    from app.services.ai_providers.base import ProviderError
    from app.services.ai_providers.openai_compatible import (
        OpenAICompatibleProvider,
    )

    provider = OpenAICompatibleProvider("https://api.example.com/v1/")
    assert provider.base_url == "https://api.example.com/v1"

    async def run():
        return await provider.upload_file("sk", Path("does-not-exist.mp4"))

    with pytest.raises(ProviderError):
        _asyncio.run(run())


# --- OpenAI error classification ----------------------------------------------


def test_classify_openai_errors():
    from app.services.ai_providers.openai_compatible import classify_openai_error

    # 429 is only quota when OpenAI's billing/quota error is present;
    # rate limits and temporary overload are transient (None).
    assert classify_openai_error(429, "Rate limit reached for requests") is None
    assert classify_openai_error(429, "The service is temporarily overloaded") is None
    assert (
        classify_openai_error(
            429, "You exceeded your current quota, please check your plan and billing details."
        )
        == "quota_exceeded"
    )
    assert (
        classify_openai_error(429, "x", error_code="insufficient_quota")
        == "quota_exceeded"
    )
    assert (
        classify_openai_error(429, "x", error_type="insufficient_quota")
        == "quota_exceeded"
    )
    assert (
        classify_openai_error(
            429, "Rate limit reached", error_code="credit_balance_exhausted"
        )
        == "quota_exceeded"
    )
    assert classify_openai_error(401, "Incorrect API key provided") == "not_working"
    assert classify_openai_error(403, "Permission denied") == "not_working"
    assert (
        classify_openai_error(400, "You didn't provide an API key") == "not_working"
    )
    # Model not found / server errors / unrelated 400: not the key's fault.
    assert classify_openai_error(404, "The model gpt-x does not exist") is None
    assert classify_openai_error(500, "internal error") is None
    assert classify_openai_error(400, "File size exceeds the limit") is None


def test_openai_auto_ordering_most_costly_first():
    from app.services.ai_providers.openai_compatible import (
        OpenAICompatibleProvider,
    )

    provider = OpenAICompatibleProvider("https://api.example.com")
    ordered = provider.order_models_for_auto(
        ["gpt-4o-mini", "gpt-4o", "o3", "llama-3"]
    )
    assert ordered[0] == "o3"
    assert ordered[-1] == "gpt-4o-mini"
    # Unknown names rank between premium and cheap tiers.
    assert ordered == ["o3", "gpt-4o", "llama-3", "gpt-4o-mini"]
