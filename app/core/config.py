from pathlib import Path
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_secret_key: str
    database_url: str | None = None
    api_token: str

    host: str = "0.0.0.0"
    port: int = 8000

    youtube_api_service_base: str = "https://www.googleapis.com/youtube/v3"

    youtube_quota_limit: int = 10_000
    youtube_upload_quota_cost: int = 1_600

    upload_chunk_size_bytes: int = 5 * 1024 * 1024
    upload_read_chunk_size: int = 5 * 1024 * 1024
    max_concurrent_uploads_per_channel: int = 200
    max_upload_memory_bytes: int = 1024 * 1024 * 1024  # 1 GiB global upload memory limit
    upload_max_file_bytes: int = 0  # 0 means no limit; otherwise reject larger uploads
    upload_partial_file_cleanup: bool = True  # remove partial files when the client disconnects
    channel_scan_interval_seconds: int = 10
    channel_snippet_refresh_interval_seconds: int = 300
    channel_worker_grace_seconds: int = 10
    # How often a channel that opted into copyright-blocked video skipping
    # polls YouTube after a successful transfer.  The poll is asynchronous and
    # continues until YouTube reports a terminal processing state.
    youtube_processing_poll_seconds: float = 10.0
    pause_on_upload_limit_minutes: int = 60
    pause_on_manual_minutes: int = 60
    # Lifetime of a pending OAuth state created by authorization and
    # re-authorization flows. The client receives the absolute expiry time
    # (and the remaining seconds) together with each authorization URL.
    oauth_state_ttl_minutes: int = 60
    # Deadline for the best-effort "is the channel session still active?"
    # pre-check inside POST /channels/auth/start. The check talks to YouTube
    # through the channel's proxy; without a deadline a dead proxy held the
    # whole request for up to the HTTP timeout (minutes) before the user saw
    # the authorization link. When the pre-check cannot answer in time, the
    # link is issued anyway (re-authorization is harmless). 0 disables the
    # pre-check entirely.
    oauth_session_precheck_timeout_seconds: float = 8.0

    # HTTP client pools. Every cached proxy client holds its own connection
    # pool, so a large proxy list multiplies these numbers. A proxy serves a few
    # channels at a time, so small per-proxy pools keep idle memory and socket
    # usage low without limiting throughput.
    http_timeout_seconds: float = 300.0
    http_max_connections: int = 100
    http_max_keepalive_connections: int = 20
    http_proxy_max_connections: int = 20
    http_proxy_max_keepalive_connections: int = 5
    # Idle keepalive sockets are dropped after this many seconds instead of
    # being held for the lifetime of the cached client.
    http_keepalive_expiry_seconds: float = 30.0
    # Proxy clients are cached for reuse but not forever: an entry unused for
    # this long is closed, and the cache never exceeds the cap.
    http_proxy_client_idle_seconds: int = 120
    http_proxy_client_cache_size: int = 32

    # Idle maintenance loop: closes unused proxy clients and returns freed
    # allocator arenas to the OS. 0 disables it.
    memory_maintenance_interval_seconds: int = 60

    # Maximum number of proxy health checks performed simultaneously. Without a
    # bound, checking a large proxy list (e.g. 1500 entries) opens that many
    # sockets at once, exhausting file descriptors/ephemeral ports and causing
    # healthy proxies to be misreported as failed. Each in-flight check also
    # holds a TLS session and a response buffer, so this value is the main knob
    # for how much memory a "check all" wave costs.
    proxy_check_concurrency: int = 30
    # Seconds a single proxy health check may take before it is treated as failed.
    # Dead SOCKS endpoints only reveal themselves by timing out, so this bounds
    # the worst case: total time ≈ (proxies / concurrency) * timeout.
    proxy_check_timeout_seconds: int = 20
    # How many channels may search for a replacement proxy at the same time.
    # Each search scans the proxy list, so without a bound 100 channels waiting
    # on a dead proxy multiply one scan by 100.
    proxy_recovery_concurrency: int = 2
    # Upload attempts per task when the transport fails mid-upload (dead or
    # flapping proxy). Between attempts the proxy is switched and the upload
    # resumes through the new route immediately — it does not wait the channel's
    # ``inter_upload_delay_seconds`` again.
    proxy_retry_attempts: int = 5
    # How long an interrupted upload may pause waiting for a usable proxy route
    # (automatic switch, or a manual fix: another proxy, or no-proxy mode)
    # before the affected tasks are released and retried on a later pass.
    proxy_switch_wait_timeout_seconds: int = 600

    # Gemini metadata generation. One request (generateContent, file state
    # poll) may take this long before it counts as failed.
    ai_request_timeout_seconds: float = 300.0
    # Uploading the video file to the Gemini Files API may take this long.
    ai_file_upload_timeout_seconds: float = 1800.0
    # How long to wait for the Gemini Files API to finish processing an
    # uploaded video before giving up on that API key.
    ai_file_processing_timeout_seconds: int = 300
    ai_file_processing_poll_seconds: float = 5.0
    # Total budget for generating metadata of ONE video (every API key and
    # model attempt together). On expiry the title/description fall back to
    # the channel templates instead of stalling the channel's upload worker.
    ai_generation_timeout_seconds: float = 900.0
    # Retries for the AI metadata search: when a full pass over the keys of
    # every connected provider found no working one, the search starts over
    # after this delay. Only keys whose quota is exhausted (the provider's
    # specific quota error, e.g. OpenAI's insufficient_quota) are skipped in
    # later passes; rate limits, temporary overload, 5xx and network errors
    # are retried. The whole search is still bounded by
    # ai_generation_timeout_seconds.
    ai_key_retry_delay_seconds: float = 30.0
    # Attempts per API key of every connected provider for one video's
    # metadata generation (the first pass counts as one attempt).
    ai_key_max_attempts: int = 3
    # OpenAI-compatible providers inline the video into the request body as a
    # base64 data URL. Above this size the request is not attempted at all
    # (the metadata falls back to the channel templates): most public
    # endpoints reject such payloads anyway. 0 disables the check.
    ai_openai_max_inline_video_bytes: int = 64 * 1024 * 1024

    base_dir: Path = Path(".")
    data_dir: Path = Path(".")
    video_base_dir: Path = Path("./videos")

    @field_validator("base_dir", mode="before")
    @classmethod
    def _resolve_base_dir(cls, value: str | Path | None) -> Path | None:
        if value is None:
            return None
        return Path(cls._normalize_separators(value)).expanduser().resolve()

    @field_validator("data_dir", "video_base_dir", mode="before")
    @classmethod
    def _resolve_relative_path(cls, value: str | Path | None) -> Path | None:
        if value is None:
            return None
        p = Path(cls._normalize_separators(value)).expanduser()
        # Keep relative paths relative to base_dir; only resolve absolute ones.
        if p.is_absolute():
            return p.resolve()
        return p

    @staticmethod
    def _normalize_separators(path: str | Path) -> str:
        """Return a path string with forward slashes only.

        Stored paths are always posix-style so they stay portable between
        Windows and Unix environments. A backslash is interpreted as a path
        separator, never as a literal filename character, which matches how
        the server uses folder paths.
        """
        return str(path).replace("\\", "/")

    @model_validator(mode="after")
    def _normalize_database_url(self) -> "Settings":
        prefix = "sqlite+aiosqlite:///"

        if self.database_url is None:
            db_path = (self.base_dir / self.data_dir / "app.db").expanduser().resolve()
        elif self.database_url.startswith(prefix):
            raw_path = self.database_url[len(prefix):]
            p = Path(raw_path).expanduser()
            if not p.is_absolute():
                p = (self.base_dir / p).resolve()
            else:
                p = p.resolve()
            db_path = p
        else:
            # Non-file database URL; leave it untouched.
            return self

        self.database_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
        return self

    def relative_path(self, path: str | Path) -> str:
        """Return a path relative to base_dir when possible.

        Always uses forward slashes so stored paths are portable between
        Windows and Unix environments. Relative paths are interpreted from
        base_dir, not from the current working directory.
        """
        p = Path(self._normalize_separators(path)).expanduser()
        if not p.is_absolute():
            p = self.base_dir / p
        resolved = p.resolve()
        try:
            return resolved.relative_to(self.base_dir).as_posix()
        except ValueError:
            return resolved.as_posix()

    def resolve_path(self, path: str | Path | None) -> Path | None:
        """Return an absolute path. Relative paths are interpreted from base_dir."""
        if path is None:
            return None
        p = Path(self._normalize_separators(path))
        if p.is_absolute():
            return p
        return self.base_dir / p


settings = Settings()
print(f"[CONFIG] BASE_DIR={settings.base_dir}", flush=True)
print(f"[CONFIG] DATA_DIR={settings.data_dir}", flush=True)
print(f"[CONFIG] DATABASE_URL={settings.database_url}", flush=True)
