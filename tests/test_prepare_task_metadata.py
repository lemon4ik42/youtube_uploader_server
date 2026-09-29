"""Tests for AI metadata integration in the coordinator batch preparation."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.models.channel import Channel
from app.models.upload_task import UploadTask
from app.services.coordinator import _prepare_task_metadata


class DummySession:
    """Cache functions are monkeypatched in tests; this stands in for the
    coordinator's real database session."""

    async def flush(self):
        return None

    async def commit(self):
        return None

    async def rollback(self):
        return None


def make_channel(**overrides) -> Channel:
    base = dict(
        youtube_channel_id=f"yt-{uuid4().hex[:8]}",
        title="Test channel",
        refresh_token_enc="r",
        access_token_enc="a",
        access_token_expires_at=datetime.now(timezone.utc),
        title_template="{filename}",
        description_template="desc for {filename}",
    )
    base.update(overrides)
    return Channel(**base)


def make_task(**overrides) -> UploadTask:
    base = dict(
        file_path="/videos/x/my video.mp4",
        file_hash="hash-123",
        title=None,
        description=None,
    )
    base.update(overrides)
    return UploadTask(**base)


@pytest.fixture
def generate(monkeypatch):
    calls = []

    async def fake_generate(selection, file_path, *, want_description):
        calls.append((selection, want_description))
        result = dict(fake_generate.result)
        if not want_description:
            result["description"] = None
        return result

    fake_generate.calls = calls
    fake_generate.result = {"title": "AI Title", "description": "AI Desc"}
    monkeypatch.setattr(
        "app.services.coordinator.generate_video_metadata", fake_generate
    )
    return fake_generate


@pytest.fixture
def cache(monkeypatch):
    """Patched cache functions; configure .picked to simulate a full cache."""
    state = {"picked": None, "saved": []}

    async def fake_pick(session, file_hash):
        state["pick_calls"] = state.get("pick_calls", 0) + 1
        return state["picked"]

    async def fake_save(session, file_hash, title, description):
        state["saved"].append((file_hash, title, description))
        return True

    state["pick"] = fake_pick
    state["save"] = fake_save
    monkeypatch.setattr(
        "app.services.coordinator.pick_cached_metadata", fake_pick
    )
    monkeypatch.setattr(
        "app.services.coordinator.save_cached_metadata", fake_save
    )
    return state


@pytest.mark.asyncio
async def test_ai_disabled_uses_templates(generate, cache):
    channel = make_channel(ai_model=None)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert generate.calls == []  # AI never invoked
    assert cache["saved"] == []
    assert task.title == "my video"
    assert task.description == "desc for my video"


@pytest.mark.asyncio
async def test_ai_success_replaces_both(generate, cache):
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "AI Title"
    assert task.description == "AI Desc"
    # Successful generations are stored in the cache.
    assert cache["saved"] == [("hash-123", "AI Title", "AI Desc")]


@pytest.mark.asyncio
async def test_ai_title_only_when_description_flag_off(generate, cache):
    channel = make_channel(ai_model="auto", ai_generate_description=False)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "AI Title"
    # Description still comes from the template when generation is disabled.
    assert task.description == "desc for my video"
    # Only the title part is cached (empty description).
    assert cache["saved"] == [("hash-123", "AI Title", "")]


@pytest.mark.asyncio
async def test_ai_failure_falls_back_to_templates(generate, cache):
    generate.result = None  # no working model anywhere
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "my video"
    assert task.description == "desc for my video"
    assert cache["saved"] == []  # nothing to save


@pytest.mark.asyncio
async def test_ai_exception_falls_back_to_templates(monkeypatch, generate, cache):
    async def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.services.coordinator.generate_video_metadata", boom)
    channel = make_channel(ai_model="google_gemini", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "my video"
    assert task.description == "desc for my video"


@pytest.mark.asyncio
async def test_client_supplied_values_win_over_ai(generate, cache):
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task(title="Client Title", description="Client Desc")
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert generate.calls == []
    assert cache["saved"] == []
    assert task.title == "Client Title"
    assert task.description == "Client Desc"


# --- Cached metadata reuse (channel flag ai_use_cached) -----------------------

@pytest.mark.asyncio
async def test_cache_flag_off_always_generates(generate, cache):
    cache["picked"] = {"title": "Cached", "description": "Cached Desc"}
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=False)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    # Even with a full cache the flag off means: always generate new metadata.
    assert cache.get("pick_calls", 0) == 0
    assert len(generate.calls) == 1
    assert task.title == "AI Title"
    assert task.description == "AI Desc"


@pytest.mark.asyncio
async def test_cache_flag_on_uses_cached_when_full(generate, cache):
    cache["picked"] = {"title": "Cached Title", "description": "Cached Desc"}
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert generate.calls == []  # generation skipped entirely
    assert task.title == "Cached Title"
    assert task.description == "Cached Desc"
    assert cache["saved"] == []  # nothing new saved


@pytest.mark.asyncio
async def test_cache_flag_on_generates_below_maximum(generate, cache):
    cache["picked"] = None  # below the maximum: nothing to reuse
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert len(generate.calls) == 1
    assert task.title == "AI Title"
    assert task.description == "AI Desc"
    assert cache["saved"] == [("hash-123", "AI Title", "AI Desc")]


@pytest.mark.asyncio
async def test_cache_without_file_hash_skips_cache(generate, cache):
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=True)
    task = make_task(file_hash=None)
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert cache.get("pick_calls", 0) == 0
    assert len(generate.calls) == 1
    assert cache["saved"] == []  # nothing to save without a hash


@pytest.mark.asyncio
async def test_cached_empty_description_falls_back_to_template(generate, cache):
    cache["picked"] = {"title": "Cached Title", "description": ""}
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "Cached Title"
    # The cached entry has no description: the channel template fills it.
    assert task.description == "desc for my video"
    assert generate.calls == []


@pytest.mark.asyncio
async def test_cached_title_only_with_description_flag_off(generate, cache):
    cache["picked"] = {"title": "Cached Title", "description": "Cached Desc"}
    channel = make_channel(
        ai_model="auto", ai_generate_description=False, ai_use_cached=True
    )
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "Cached Title"
    # Description generation is off: the template always fills it.
    assert task.description == "desc for my video"


# --- Partial AI answers --------------------------------------------------------


@pytest.mark.asyncio
async def test_ai_title_only_description_from_template(generate, cache):
    generate.result = {"title": "AI Title", "description": ""}
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "AI Title"
    assert task.description == "desc for my video"


@pytest.mark.asyncio
async def test_ai_description_only_title_from_template(generate, cache):
    generate.result = {"title": "", "description": "AI Desc"}
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "my video"
    assert task.description == "AI Desc"


# --- AI generation timeout -----------------------------------------------------


@pytest.mark.asyncio
async def test_ai_timeout_falls_back_to_templates(monkeypatch, generate, cache):
    import asyncio

    async def slow_generate(*args, **kwargs):
        await asyncio.sleep(30)

    monkeypatch.setattr(
        "app.services.coordinator.generate_video_metadata", slow_generate
    )
    monkeypatch.setattr(
        "app.services.coordinator.settings.ai_generation_timeout_seconds", 0.05
    )
    channel = make_channel(ai_model="auto", ai_generate_description=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    # The bounded AI call fell back to the channel templates.
    assert task.title == "my video"
    assert task.description == "desc for my video"
    assert cache["saved"] == []
    assert generate.calls == []  # the real generate was replaced by the slow stub


@pytest.mark.asyncio
async def test_ai_timeout_still_applies_cached_entry(monkeypatch, generate, cache):
    """The cache is consulted before the (bounded) generation call."""
    cache["picked"] = {"title": "Cached Title", "description": "Cached Desc"}
    channel = make_channel(ai_model="auto", ai_generate_description=True, ai_use_cached=True)
    task = make_task()
    await _prepare_task_metadata(DummySession(), channel, task, "/videos/x/my video.mp4")

    assert task.title == "Cached Title"
    assert task.description == "Cached Desc"
