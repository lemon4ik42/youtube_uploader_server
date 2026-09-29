# YouTube Uploader Server — API Reference

Base URL: `http://<host>:8000`

All endpoints require the header:

```http
Authorization: Bearer <API_TOKEN>
```

`API_TOKEN` is configured via the `API_TOKEN` environment variable.

---

## Table of contents

- [Environment variables](#environment-variables)
- [Common types](#common-types)
- [Projects](#projects)
- [Channel tags](#channel-tags)
- [Proxies](#proxies)
- [AI types](#ai-types)
- [AI custom providers (OpenAI-compatible)](#ai-custom-providers-openai-compatible)
- [AI prompts](#ai-prompts)
- [AI metadata generation during uploads](#ai-metadata-generation-during-uploads)
- [Channels](#channels)
- [Uploads](#uploads)
- [Tasks](#tasks)
- [Updates (SSE)](#updates-sse)

---

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `APP_SECRET_KEY` | — | Required. Secret key used by the server. |
| `API_TOKEN` | — | Required. Bearer token used by the client to authenticate API requests. |
| `HOST` | `0.0.0.0` | Interface the HTTP server binds to. |
| `PORT` | `8000` | HTTP server port. |
| `BASE_DIR` | project root | Absolute base directory for all relative paths, the database and video folders. On a remote host set this to the dedicated data directory, e.g. `/data`. |
| `DATA_DIR` | `.` | Subdirectory inside `BASE_DIR` where `app.db` is stored. Use `.` to place the database directly in `BASE_DIR`. |
| `DATABASE_URL` | auto | Optional explicit SQLite URL. If omitted, the server uses `sqlite+aiosqlite:///{BASE_DIR}/{DATA_DIR}/app.db`. If set with a relative path, it is normalized to an absolute path under `BASE_DIR`. |
| `VIDEO_BASE_DIR` | `./videos` | Directory where channel video folders are created. Relative to `BASE_DIR`. |
| `UPLOAD_CHUNK_SIZE_BYTES` | `5242880` | Chunk size for YouTube resumable uploads. |
| `MAX_CONCURRENT_UPLOADS_PER_CHANNEL` | `200` | Max concurrent uploads per channel. |
| `MAX_UPLOAD_MEMORY_BYTES` | `1073741824` | Global memory budget for all active uploads (1 GiB). |
| `CHANNEL_SCAN_INTERVAL_SECONDS` | `10` | How often the server scans channel folders for new videos. |
| `CHANNEL_SNIPPET_REFRESH_INTERVAL_SECONDS` | `300` | Period between asynchronous metadata refresh attempts for each saved, non-deleted channel. Existing channels are not refreshed immediately at startup; their first scheduled attempt is spread across the interval. A newly connected channel is fetched during OAuth before it is saved. |
| `MIN_BATCH_FLUSH_IDLE_SECONDS` | `60` | How long a channel's queue may sit below `min_batch_size` without new client uploads before the remaining files are flushed. Set to `0` to always flush, or a large value for a strict threshold. |
| `PAUSE_ON_UPLOAD_LIMIT_MINUTES` | `60` | Pause duration when YouTube upload-limit is hit. |
| `OAUTH_STATE_TTL_MINUTES` | `60` | Lifetime of a pending OAuth state created by authorization and re-authorization. The expiry time (`expires_at`, `expires_in_seconds`) is sent to the client together with every authorization URL. |
| `PROXY_CHECK_CONCURRENCY` | `30` | Maximum number of proxy health checks performed simultaneously. Each in-flight check holds a TLS session and a response buffer, so this bounds the memory a "check all" wave costs and prevents socket/fd exhaustion that would misreport healthy proxies as failed. |
| `PROXY_CHECK_TIMEOUT_SECONDS` | `20` | Seconds a single proxy health check may take before it is treated as failed. Dead SOCKS endpoints only reveal themselves by timing out, so worst-case scan duration ≈ (proxies / concurrency) × timeout. |
| `PROXY_RECOVERY_CONCURRENCY` | `2` | How many channels may search for a replacement proxy at the same time. Each search scans the proxy list; an unbounded value multiplies one scan by the number of channels waiting on a dead proxy. |
| `PROXY_RETRY_ATTEMPTS` | `5` | Upload attempts per task when the transport fails mid-upload (dead or flapping proxy). Between attempts the proxy is switched and the upload resumes through the new route immediately; it does not wait the channel's `inter_upload_delay_seconds` again. |
| `PROXY_SWITCH_WAIT_TIMEOUT_SECONDS` | `600` | How long an interrupted upload may pause waiting for a usable proxy route (automatic switch, or a manual fix: another proxy or no-proxy mode) before the affected tasks are released and retried on a later pass. |
| `AI_REQUEST_TIMEOUT_SECONDS` | `300` | How long a single Gemini request (generateContent, file state poll) may take before it counts as failed and the next model/key is tried. |
| `AI_FILE_UPLOAD_TIMEOUT_SECONDS` | `1800` | How long uploading the video file to the Gemini Files API may take. |
| `AI_FILE_PROCESSING_TIMEOUT_SECONDS` | `300` | How long to wait for the Gemini Files API to finish processing an uploaded video before giving up on that API key. |
| `AI_FILE_PROCESSING_POLL_SECONDS` | `5` | Interval between file state polls while the video is being processed. |
| `AI_GENERATION_TIMEOUT_SECONDS` | `900` | Total budget for generating the metadata of one video (all API key and model attempts together). On expiry the title/description fall back to the channel templates instead of stalling the channel's upload worker. |
| `AI_KEY_RETRY_DELAY_SECONDS` | `30` | Delay before the AI key search starts a new round over every provider's key list when the previous round found no working key. Only keys whose quota is exhausted (the provider's specific quota error) are skipped in later rounds; rate limits, temporary overload, 5xx and network errors are retried. |
| `AI_KEY_MAX_ATTEMPTS` | `3` | Attempts per API key of every connected provider for one video's metadata generation (the first pass counts as one attempt). |
| `AI_OPENAI_MAX_INLINE_VIDEO_BYTES` | `67108864` | OpenAI-compatible providers inline the video into the request body as a base64 data URL; above this size the request is not attempted (the metadata falls back to the channel templates). `0` disables the check. |

---

## Common types

### `ProjectOut`

```json
{
  "id": "uuid",
  "name": "string",
  "client_id": "string",
  "redirect_uri": "string",
  "scope": "https://www.googleapis.com/auth/youtube",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "api_upload": "https://www.googleapis.com/upload/youtube/v3/videos",
  "quota_used": 0,
  "quota_reset_at": "2026-01-01T00:00:00Z",
  "is_active": true,
  "created_at": "2026-01-01T00:00:00Z"
}
```

### `ChannelOut`

```json
{
  "id": "uuid",
  "youtube_channel_id": "UC...",
  "title": "Channel Title",
  "youtube_title": "YouTube Channel Title",
  "handle": "@channelHandle",
  "thumbnail_url": "https://yt3.googleusercontent.com/...",
  "subscriber_count": 125000,
  "view_count": 9876543,
  "metadata_updated_at": "2026-01-01T00:00:00Z",
  "project_id": "uuid",
  "folder_path": "videos/5f20e776-0d2b-4083-8e85-cac0c2771bf2",
  "title_template": "{filename}",
  "description_template": "",
  "privacy_status": "public",
  "default_category_id": "22",
  "auto_upload_enabled": true,
  "status": "active",
  "session_status": "active",
  "session_status_changed_at": "2026-01-01T00:00:00Z",
  "paused_until": null,
  "pause_reason": null,
  "error_message": null,
  "last_upload_at": null,
  "upload_limit_enabled": false,
  "upload_limit_count": 50,
  "upload_limit_period_hours": 24,
  "max_batch_size": 200,
  "min_batch_size": 1,
  "inter_upload_delay_seconds": 2,
  "shuffle_upload_order": false,
  "skip_blocked_videos": false,
  "proxy_id": "uuid-or-null",
  "proxy_auto_switch": false,
  "proxy_recovery_pending": false,
  "ai_model": null,
  "ai_generate_description": false,
  "ai_use_cached": false,
  "tags": [{"id": "uuid", "name": "Gaming"}],
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z"
}
```

- `status` — `active` | `paused` | `error` | `deleted`.
- `tags` — saved channel tags as `{ "id": "uuid", "name": "Gaming" }` objects, sorted by name; `[]` when none are assigned. Included in channel list/detail responses and SSE events carrying `ChannelOut`.
- `created_at` — the date/time this channel record was created after its first successful OAuth authorization on this server. Already persisted and returned by the API; reauthorization, token refresh, project changes and tag changes do not reset it. It is not the YouTube channel creation date. Deleting and reconnecting a channel creates a new record with a new date. For legacy databases whose original timestamp was absent, earlier migrations may have recovered it from `updated_at`.
- `pause_reason` — `null` | `"upload_limit"` | `"youtube_rate_limit"` | `"manual"` | `"error"`. Reason why the channel was paused. `"youtube_rate_limit"` covers YouTube-side upload and quota limits.
- `session_status` — `active` | `expired` | `reauth_required`. `expired` is set when the refresh token stops working; `reauth_required` is set when the current project has exhausted its quota and another project must be authorized.
- When `POST /channels/auth/start` is requested for an existing channel whose cached status is `active`, the server first performs a real token refresh when needed and calls YouTube `channels.list(mine=true)`, bounded by `OAUTH_SESSION_PRECHECK_TIMEOUT_SECONDS` (default 8s). A successful response returns `409` because re-authorization is not needed. A real authorization failure starts the OAuth flow; when YouTube is unreachable within the deadline (network, proxy, timeout failures), the authorization URL is issued anyway instead of blocking until the HTTP timeout — the callback re-validates everything. If the channel is waiting for a working proxy (`proxy_recovery_pending: true`), the server returns `503` immediately instead of blocking or falling back to a direct connection.
- `youtube_title`, `handle`, `thumbnail_url`, `subscriber_count`, and `view_count` are real values returned by YouTube Data API and refreshed every `CHANNEL_SNIPPET_REFRESH_INTERVAL_SECONDS` (default 300 seconds). Each saved channel has its own asyncio task; no OS thread is created.
- `subscriber_count` is `null` when YouTube does not expose the subscriber count, including channels that hide it.
- `view_count` is the channel's total view count reported by YouTube.
- `metadata_updated_at` is the time of the last successful metadata refresh. A changed value emits `channel.metadata_updated`; avatar changes also emit `channel.avatar_updated` for compatibility. Separate asyncio tasks start for all saved non-deleted channels at application startup, but their first requests are delayed and spread across the configured interval. Each iteration checks `session_status`; when it is not `active`, that iteration makes no YouTube request and the task continues. An iteration is also skipped while the channel is waiting for a working proxy (`proxy_recovery_pending: true`), so metadata refresh never falls back to a direct connection. A newly connected channel is queried during OAuth before it is saved, then its periodic task starts. Tasks stop only when a channel is deleted or the application shuts down.
- `folder_path` — path to the channel's video folder, stored relative to the server's `base_dir` (or the project root if `BASE_DIR` is not configured). Channels that share a folder (via `folder_from_channel_id`) form one upload queue: each file is uploaded exactly once — no matter which sharing channel's worker picks it up — and the uploaded-file hash is recorded for every sharer. Pacing, however, is strictly per channel: the inter-upload delay anchors to the channel's OWN last completed upload, so one sharer waiting out its delay window never holds another sharer back (a channel whose own window is open skips claiming files until it elapses, leaving them for a ready sharer). Transfers on a shared folder are serialized so two sharers never upload simultaneously.
- `max_batch_size` — maximum number of videos the server will upload in one batch (1–200, default 200).
- `min_batch_size` — minimum number of videos that should accumulate in the folder while the client is actively uploading (1–200, default 1). Once the client has been idle for `MIN_BATCH_FLUSH_IDLE_SECONDS` (default 60) without pushing new files, the remaining queue is flushed anyway so the tail of the folder never stalls. If files appear in the folder without going through the client upload API, the threshold is not applied. `min_batch_size` must not exceed `max_batch_size`.
- `inter_upload_delay_seconds` — seconds to wait between uploads (default 2, maximum 604800 = one week). Pacing is anchored to the last *completed* upload, so it also spaces out consecutive batches: the next batch's uploads start no earlier than `last completed upload + inter_upload_delay_seconds`, including when the configurable upload limit trims the batch size. With `max_batch_size=1` every video is its own batch, so the delay spaces individual videos: after the first video completes, each next video waits the full delay before its upload session is even opened. The wait happens before any YouTube upload session is opened for the batch — a task never sits in `session_created` for the delay window — and the access token is re-validated after the wait (the delay can exceed the token's ~1h lifetime), so the upload never starts with expired credentials. Pacing values are re-read from the database right before the wait, and the wait is interruptible: saving the channel's pacing or limit settings through `PUT /channels/{channel_id}`, `PUT /channels/{channel_id}/batch-settings`, `PUT /channels/{channel_id}/upload-limit`, or `PUT /channels/bulk-settings` wakes it and the server recomputes. A delay lowered mid-wait below the already-elapsed time starts the batch immediately; a raised delay extends the wait; an upload limit that became exhausted pauses the channel instead of uploading; a lowered limit or `max_batch_size` trims the waiting batch (the trimmed tasks stay `pending` for later batches). Disabling `auto_upload_enabled` mid-wait drops the batch entirely. The wait survives a server restart: `last_upload_at` is persisted right after each completed upload, so a restart during the delay recomputes the remaining time instead of resetting or losing the wait — with both graceful shutdowns (pending tasks are re-collected) and hard kills (pending task rows are picked up directly, and a task that was mid-transfer resumes its resumable session from the committed offset). A batch consisting only of retry tasks (left over from a transient failure) skips the wait so a recovered route is used immediately; a batch with at least one fresh task is paced normally. While a batch waits, the server logs `Channel {id} pacing: waiting Ns before the next batch`. During the transfer itself the task's `progress_percent` is updated and broadcast as `task.updated` (at most once every 2 seconds per task).
- `shuffle_upload_order` — when `true`, the server randomly shuffles the waiting videos before forming each upload batch, so videos are uploaded in a random order instead of the folder order (default `false`). For a folder shared by several channels, it first chooses videos not yet uploaded by another sharer; after those are exhausted, it chooses randomly within the least-used-by-other-sharers group. A channel never re-uploads a video it has already completed itself.
- `skip_blocked_videos` — when `true`, after each successful upload the server asynchronously waits for YouTube processing to complete and checks the owner-visible video status. A file is added to the global blocked-video registry only when YouTube reports copyrighted/licensed content and an all-country block (`regionRestriction.allowed: []`), or a terminal copyright/claim rejection. Subsequent scans on channels with this option enabled skip that file hash and select another eligible video through the normal upload queue. The default is `false`.
- `proxy_id` — selected proxy from the global proxy list, or `null` for direct connections.
- `proxy_auto_switch` — when `true`, the server checks the global proxy list and switches to a working proxy with the fewest assigned channels after a proxy/network failure.
- `proxy_recovery_pending` — `true` while the channel is waiting for a working replacement proxy. During this state new YouTube requests wait and do not fall back to a direct connection.
- `ai_model` — AI metadata generation setting: `null` (AI disabled, titles/descriptions come from the templates), `"auto"` (the server picks the first working active API type or custom provider), the name of a supported [API type](#ai-types) (e.g. `"google_gemini"`), or `custom:<provider_id>` referencing an [AI custom provider](#ai-custom-providers-openai-compatible). A type/provider without saved keys/models falls back to the templates at upload time.
- `ai_generate_description` — when `true` and an AI model is selected, the description prompt is also sent and the model's description is used; otherwise only the title is generated and the description comes from the channel template. See [AI metadata generation during uploads](#ai-metadata-generation-during-uploads).
- `ai_use_cached` — when `true`, uploads reuse the [generation cache](#generation-cache) for the video once it holds the maximum of 10 unique entries (a random saved entry is used instead of calling the AI); below the maximum, new metadata is generated and stored. When `false` (default), every upload generates new metadata (results are still stored).

### `ProxyOut`

```json
{
  "id": "uuid",
  "url": "socks5://user:password@example.com:1080",
  "is_active": true,
  "assigned_channels": 2,
  "last_checked_at": "2026-01-01T00:00:00Z",
  "last_check_ok": true,
  "status": "working",
  "failure_count": 0,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z"
}
```

- Proxy URLs may use `http://`, `https://`, or `socks5://`.
- Proxy credentials may be included in the URL.
- `url` is returned only through the authenticated API and is stored encrypted in the database.
- `status` is `unknown`, `working`, or `failed`. It is updated by explicit health checks and by real Google/YouTube requests made through the proxy.
- If an assigned proxy fails and the channel has `proxy_auto_switch: true`, the server checks active proxies (most-likely-working first) in waves bounded by `PROXY_CHECK_CONCURRENCY` and switches to the least-used working candidate, stopping the scan as soon as one is found. While the search runs, the failed proxy is re-checked on every recovery pass; if it starts working again, the channel keeps it and the replacement search is cancelled instead of moving the channel to another proxy. If no proxy is working, the channel enters `proxy_recovery_pending` and waits; it does not switch to direct mode or send YouTube requests until a working proxy is found. The upload worker is not stopped.
- Mid-upload proxy failures do not fail the task. The upload pauses for up to `PROXY_SWITCH_WAIT_TIMEOUT_SECONDS` while the route is repaired — automatically via `proxy_auto_switch`, or manually (choosing another proxy, or no-proxy mode); `PUT /channels/{id}` clears `proxy_recovery_pending` immediately, so a manual fix unblocks the waiting upload at once. Once a usable route exists the same task resumes through it, and retries do not re-pay the channel's `inter_upload_delay_seconds`: pacing is anchored to the last *completed* upload and skipped entirely for batches that only contain leftover retry tasks (`attempt > 0`). If no route appears in time, the affected tasks are released as retryable `pending` (event `task.failed` with `will_retry: true`) and scheduled for another pass, not permanently failed.
- Resumable session URLs are persisted on the task row in the database the moment they are created, so an interrupted pass (dead proxy, restart, manual route fix) does not waste the bytes YouTube already committed. Before each upload the server checks whether the task already has a saved session: a saved session is first queried for its committed offset (empty `PUT` with `Content-Range: bytes */total`) and the upload resumes from that offset instead of re-sending the whole file. If the saved session is dead (404/410 — sessions expire after ~24h), a new one is created and the task uploads through it. When a task reaches a terminal state — `completed` or a terminal `failed` — the session URL is cleared from the row. The `session_url` is internal and is not exposed through the task API.
- Creating, importing, or re-enabling proxies schedules a background recovery pass. Channels with `proxy_auto_switch: true` whose current proxy is missing, disabled, or failed are automatically assigned a newly available working proxy and receive `channel.proxy_switched`.
- Concurrent channel recoveries share the same bounded pool of proxy health checks (limited by `PROXY_CHECK_CONCURRENCY`) and serialize only the final load calculation and assignment. Each assignment is committed before the next channel selects a proxy, preventing all simultaneously recovering channels from choosing the same stale least-used candidate.
- Deleting an assigned proxy returns `204` immediately after the proxy is removed. Upload workers and existing resumable sessions are not stopped or reset. Affected channels get `proxy_id: null` plus `proxy_recovery_pending: true`; new YouTube requests wait rather than use direct mode. A background asyncio loop repeatedly checks active candidates in bounded waves until a working proxy is found. When a working proxy is found, it is assigned and the channel resumes. Manual proxy changes, explicit direct mode, channel deletion, and application shutdown cancel or invalidate the recovery loop.

### `AITypeOut`

```json
{
  "api_type": "google_gemini",
  "models": ["gemini-2.5-pro", "gemini-2.5-flash"],
  "api_keys": [
    {
      "id": "uuid",
      "key": "AIza...DDDD",
      "status": "working",
      "last_checked_at": "2026-01-01T00:00:00Z",
      "last_error": null,
      "created_at": "2026-01-01T00:00:00Z",
      "updated_at": "2026-01-01T00:00:00Z"
    }
  ],
  "is_active": true
}
```

- `api_type` identifies the provider. Currently only `google_gemini` is supported; each supported type has exactly one settings row. Types without saved settings are listed with empty `models`/`api_keys` and `is_active: false`.
- `models` is the list of concrete model names configured for the type.
- `api_keys` are stored encrypted in the database (Fernet, derived from `APP_SECRET_KEY`) and are never returned in full — only a masked preview (`first4...last4`). To change the keys, submit the full new list to `PUT /ai-types/{api_type}`, which replaces the stored list entirely (new keys start with status `unknown`).
- `api_keys[].status` is the live health of the key, refreshed automatically on every request the server makes with it: `working` (last request succeeded), `not_working` (invalid key / permission denied), `quota_exceeded` (the provider's specific quota-exhausted error — for OpenAI-compatible providers a 429 with an `insufficient_quota`-style billing code such as `insufficient_quota`/`credit_balance_exhausted`/`*_spend_limit_exceeded`/`*_usage_limit_exceeded`, for Gemini HTTP 429 `RESOURCE_EXHAUSTED`), `unknown` (never used). Other 429s (rate limits, slow down, "service temporarily overloaded"), 5xx, network and model-not-found errors are transient: they leave the previous status unchanged, are recorded in `last_error` and are retried in later rounds (see `AI_KEY_MAX_ATTEMPTS`/`AI_KEY_RETRY_DELAY_SECONDS`).
- The same API key cannot be stored under two different API types or custom providers.
- `is_active` excludes the type from automatic (`"auto"`) channel selection; a channel pinned to the type still uses it.

### `AICustomProviderOut`

```json
{
  "id": "uuid",
  "name": "My OpenRouter",
  "base_url": "https://openrouter.ai/api/v1",
  "models": ["gpt-4o", "gpt-4o-mini"],
  "api_keys": [
    {
      "id": "uuid",
      "key": "sk-A...DDDD",
      "status": "unknown",
      "last_checked_at": null,
      "last_error": null,
      "created_at": "2026-01-01T00:00:00Z",
      "updated_at": "2026-01-01T00:00:00Z"
    }
  ],
  "is_active": true,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z"
}
```

Same semantics as `AITypeOut` (masked encrypted keys with live statuses, wholesale list replacement, active flag), plus `name` (unique) and `base_url` — generation calls go to `{base_url}/chat/completions`. Channels reference the provider as `custom:<id>`. See [AI custom providers](#ai-custom-providers-openai-compatible).

### `ChannelSessionStatusOut`

```json
{
  "channel_id": "uuid",
  "session_status": "active",
  "session_status_changed_at": "2026-01-01T00:00:00Z"
}
```

### `ChannelAvatarOut`

```json
{
  "channel_id": "uuid",
  "thumbnail_url": "https://yt3.googleusercontent.com/..."
}
```

### `ChannelUploadedCount`

```json
{
  "total_uploaded": 42,
  "uploads_in_current_period": 12,
  "period_hours": 24,
  "is_limited": false,
  "remaining": 38,
  "reset_at": "2026-01-02T00:00:00Z"
}
```

- `total_uploaded` — total number of `completed` uploads for this channel.
- `uploads_in_current_period` — completed uploads inside the current sliding window.
- `period_hours` — configured window size.
- `is_limited` — true when the configurable upload limit is currently reached.
- `remaining` — uploads still allowed in the current window, or `null` when the limit is disabled.
- `reset_at` — when the channel will resume after a consumed limit: the most
  recent upload in the consumed quota plus the configured period. This keeps
  the channel paused until the whole quota is available again (rather than
  resuming for just one video); `null` when the limit is disabled.

### `FolderVideoOut`

```json
{
  "file_name": "video.mp4",
  "size_bytes": 123456789,
  "modified_at": "2026-01-01T00:00:00Z"
}
```

### `TaskOut`

```json
{
  "id": "uuid",
  "batch_id": "uuid-or-null",
  "channel_id": "uuid",
  "file_path": "/path/to/video.mp4",
  "file_hash": "hash",
  "title": "video title",
  "description": "description",
  "status": "pending",
  "youtube_video_id": null,
  "progress_percent": 0,
  "attempt": 0,
  "error_message": null,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z"
}
```

---

## Projects

A project stores Google Cloud OAuth credentials and API endpoints used by one or more YouTube channels.

The project is the only place that keeps the OAuth `client_id`/`client_secret`. Several projects can be saved; the server can switch between them if a channel hits a quota limit, but the user must re-authorize the channel on the new project because refresh tokens are tied to a `client_id`.

### Create project

```http
POST /projects
Content-Type: application/json
```

Body:

```json
{
  "name": "my-gcp-project",
  "client_id": "123456789-xxx.apps.googleusercontent.com",
  "client_secret": "GOCSPX-xxx",
  "redirect_uri": "http://localhost:8080/callback",
  "scope": "https://www.googleapis.com/auth/youtube",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "api_upload": "https://www.googleapis.com/upload/youtube/v3/videos"
}
```

Only `name`, `client_id`, `client_secret`, and `redirect_uri` are required. The other fields default to standard Google endpoints. `redirect_uri` must be registered in the Google Cloud Console for this `client_id`, otherwise Google returns `400 redirect_uri_mismatch`.

Response: `201 Created` → `ProjectOut`. If another project already has the same `client_id` and `client_secret`, the server returns `409 Conflict` and does not create a duplicate.

### List projects

```http
GET /projects
```

Response: `200 OK` → `ProjectOut[]`

### Get project

```http
GET /projects/{project_id}
```

Response: `200 OK` → `ProjectOut`

### Update project

```http
PUT /projects/{project_id}
Content-Type: application/json
```

Body (all optional):

```json
{
  "name": "new name",
  "client_id": "...",
  "client_secret": "...",
  "redirect_uri": "http://localhost:8080/callback",
  "scope": "...",
  "token_uri": "...",
  "auth_uri": "...",
  "api_upload": "...",
  "is_active": true
}
```

Response: `200 OK` → `ProjectOut`

### Delete project

```http
DELETE /projects/{project_id}
```

Response: `204 No Content`. A project assigned to one or more channels cannot be deleted. The server returns `409 Conflict` until those channels are re-authorized or moved to another project.

---

## Proxies

Proxies are stored in a global database list and can be assigned independently to channels. A channel uses its selected proxy for every YouTube and Google OAuth HTTP request it makes: upload session creation, each uploaded chunk, access-token refresh, channel metadata/statistics fetches, and OAuth token exchange. If no proxy is selected, requests are sent directly.

While a channel is in `proxy_recovery_pending` (waiting for a working replacement proxy), none of these requests fall back to a direct connection. Uploads, token refreshes, and metadata refreshes resolve the proxy fresh from the database and wait until a working proxy is assigned; the periodic metadata refresh simply skips that iteration instead of sending a direct request. Starting an authorization for an active channel that is waiting for a proxy returns `503` rather than blocking or going direct. The only exception is the interactive OAuth callback itself: if the user completes authorization while no proxy is available, the one-time code exchange proceeds so the user is not locked out of connecting the channel.

### Create proxy

```http
POST /proxies
Content-Type: application/json
```

```json
{
  "url": "socks5://user:password@example.com:1080"
}
```

Supported URL schemes are `http`, `https`, and `socks5`.

Response: `201 Created` -> `ProxyOut`. If the same proxy URL already exists, the server returns `409 Conflict` and does not create a duplicate. New proxy records receive a UUID `id` automatically.

### Import a proxy list

```http
POST /proxies/import
Content-Type: application/json
```

```json
{
  "proxies": [
    {"url": "http://user:password@host1:8080"},
    {"url": "socks5://host2:1080"}
  ]
}
```

The server assigns a UUID `id` to every newly added proxy. Proxy names are not supported; proxy selection, updates, deletion, checks, and channel assignment always use this UUID. The server compares every URL with the existing encrypted proxy list and also removes duplicates within the submitted list. Existing proxies are not changed.

Response: `200 OK`:

```json
{
  "received": 2,
  "added": 1,
  "duplicates": 1
}
```

### List proxies

```http
GET /proxies
```

Response: `200 OK` -> `ProxyOut[]`.

`assigned_channels` is the current number of channels using the proxy.

### Check all proxies

```http
POST /proxies/check
```

All active proxies are checked through the YouTube Data API. Checks run in waves bounded by `PROXY_CHECK_CONCURRENCY` (default 30), so a very large proxy list does not exhaust file descriptors/ephemeral ports or misreport healthy proxies as failed; each check times out after `PROXY_CHECK_TIMEOUT_SECONDS`. Results are written in one database transaction per wave, and recovery loops for failed proxies are started only after the whole scan has finished — while waves are still running, an unrelated endpoint (for example `PUT /channels/bulk-settings`) keeps being served between waves. If a complete mass check is already running, another request waits for that same operation. Per-proxy checks are also shared with automatic selection, channel auto-switching, deletion replacement, and individual check requests.

Response: `200 OK`:

```json
{
  "checked": 10,
  "working": 8,
  "failed": 2,
  "proxies": [
    {
      "id": "uuid",
      "url": "socks5://host:1080",
      "status": "working"
    }
  ]
}
```

### Check one proxy

```http
POST /proxies/{proxy_id}/check
```

The request checks connectivity through the YouTube Data API and returns the updated `ProxyOut`. If this proxy is already being checked anywhere in the server, the endpoint waits for the running check and does not start another request.

### Update proxy

```http
PUT /proxies/{proxy_id}
Content-Type: application/json
```

All fields are optional:

```json
{
  "url": "https://user:password@example.com:8443",
  "is_active": true
}
```

Changing a proxy URL resets neither its channel assignments nor the channel auto-switch setting. Set `is_active` to `false` to exclude it from automatic selection.

### Delete proxy

```http
DELETE /proxies/{proxy_id}
```

Response: `204 No Content`. The proxy is deleted immediately without stopping workers or discarding resumable sessions. Affected channels enter `proxy_recovery_pending`; each subsequent YouTube request waits for the latest proxy setting. Replacement selection runs continuously in the background, checks candidates concurrently, and assigns the least-used working proxy. Manual proxy/direct-mode changes or channel deletion cancel the search. `channel.proxy_switched` and `proxy.deleted` events describe the transition.

### Automatically select a working proxy

```http
POST /proxies/auto-select
```

The server checks active proxies through the YouTube Data API and returns a working proxy with the fewest assigned channels. The selected proxy is not assigned to a channel by this endpoint. A short in-memory reservation is created for the response, so concurrent auto-select requests do not all return the same least-used proxy. The reservation is released when the proxy is assigned to a channel or expires after 30 seconds. Checks are coalesced per proxy: if another request or channel worker is already checking the same proxy, this endpoint waits for that check to finish and returns its result instead of starting another network request.

Response: `200 OK` -> `ProxyOut`.

### Configure a channel proxy

```http
PUT /channels/{channel_id}/proxy
Content-Type: application/json
```

```json
{
  "proxy_id": "uuid-or-null",
  "auto_switch": true
}
```

- `proxy_id` — selected proxy, or `null` to use direct requests.
- `auto_switch` — enables automatic switching after a network/proxy failure. The replacement is selected from the global active proxy list by the same least-assigned-channel algorithm as `/proxies/auto-select`. Switching is triggered only when a real YouTube request or a health check proves the proxy is not working; a stored `failed` status alone does not trigger it, because the stored status may be stale.

Response: `200 OK` -> `ChannelOut`.

The same `proxy_id` and `proxy_auto_switch` fields can also be changed with `PUT /channels/{channel_id}`.

---

## AI types

API types group the settings used for AI metadata generation: each supported type has its own list of concrete models and API keys. Each API type is served by its own provider implementation — currently `google_gemini` (built on the official `google-genai` SDK, asynchronous client). Adding a new API type means adding its provider implementation; unknown `api_type` values are rejected. There is exactly one settings row per type.

### List API types

```http
GET /ai-types
```

Response: `200 OK` -> `AITypeOut[]`. Every supported type is listed; types without saved settings show empty `models`/`api_keys` lists and `is_active: false`.

### Get one API type

```http
GET /ai-types/{api_type}
```

Response: `200 OK` -> `AITypeOut` (empty lists when nothing is saved for the type), or `404 Not Found` for an unsupported type.

### Save / update an API type

```http
PUT /ai-types/{api_type}
Content-Type: application/json
```

All fields are optional; omitted fields keep their stored values. The first request for a type creates its settings row:

```json
{
  "models": ["gemini-2.5-pro", "gemini-2.5-flash"],
  "api_keys": ["AIzaSy...", "AIzaSy..."],
  "is_active": true
}
```

- `api_type` (path) — provider identifier; only `google_gemini` is supported (`400` otherwise).
- `models` — at least one non-empty model name; duplicates and blank entries are dropped.
- `api_keys` — at least one non-empty key. Keys are stored encrypted, never returned in full, and start with status `unknown`.
- Providing `models` or `api_keys` replaces the whole stored list. Replacing `api_keys` resets every key status to `unknown`; a key already saved under another API type returns `409 Conflict`. Re-submitting the same keys for the same type is allowed.
- `is_active` — excludes the type from automatic (`"auto"`) channel selection when `false`. Default `true` on creation.

Response: `200 OK` -> `AITypeOut`. Emits `ai_type.updated`.

### Delete an API type

```http
DELETE /ai-types/{api_type}
```

Response: `204 No Content`, or `404 Not Found` (unsupported type or nothing saved). The settings and all stored API keys of the type are removed immediately. Channels referencing the type fall back to their templates until it is configured again. Emits `ai_type.deleted`.

---

## AI custom providers (OpenAI-compatible)

Custom providers target any OpenAI-compatible chat completion API with its own base URL (OpenAI, OpenRouter, DeepSeek, a local vLLM/ollama endpoint, etc.). Unlike the built-in [API types](#ai-types) — one settings row per type — providers are multi-instance: every provider has its own name, base URL, model list and API keys. Channels reference a provider as `custom:<provider_id>` in `ai_model`.

Provider behavior mirrors the built-in types: API keys are tried sequentially with live health statuses (`working` / `not_working` / `quota_exceeded` / `unknown`, refreshed on every request), `models`/`api_keys` are replaced wholesale on update, and the same prompts produce one JSON answer with both the title and the description. Error mapping: HTTP 429 (or 402) is `quota_exceeded` **only** when the error body is one of OpenAI's billing/quota errors — `error.code`/`error.type` of `insufficient_quota`, `credit_balance_exhausted`, `organization_spend_limit_exceeded`, `project_spend_limit_exceeded`, `organization_usage_limit_exceeded`, or a message about quota/billing/credits — every other 429 (rate limits, `slow_down`, a gateway's "service is temporarily overloaded") is transient and leaves the status unchanged; 401/403 (or a 400 about the API key) → `not_working`; model-not-found (404), 5xx and network errors are transient and leave the status unchanged. Transient failures are retried in later rounds like for the built-in types.

**Video transfer.** OpenAI-compatible APIs have no resumable file upload like the Gemini Files API — the video is inlined into the single `POST {base_url}/chat/completions` request body as a base64 data URL. The request body is assembled and streamed chunk-by-chunk, so the video is never held in memory in full: the server reads the file in ~5 MB chunks (aligned to a multiple of 3 bytes so the base64 encoding of each chunk has no interior padding), encodes each chunk with C-level `base64.b64encode` and yields it to the HTTP client immediately. The exact `Content-Length` is precomputed arithmetically, so the body is streamed instead of buffered. The whole request is booked in the global upload memory budget (`MAX_UPLOAD_MEMORY_BYTES`) with the weight of the raw file size — the same accounting a YouTube or Gemini upload uses. Two protocol consequences worth knowing:

- every model attempt re-sends the whole video (Gemini uploads the file once per key and then calls each model against it) — keep provider model lists short;
- the inline payload has a hard limit `AI_OPENAI_MAX_INLINE_VIDEO_BYTES` (default 64 MiB, 0 disables): above it the request is not attempted at all, the key status is not damaged, and the metadata falls back to the channel templates.

### List providers

```http
GET /ai-providers
```

Response: `200 OK` -> `AICustomProviderOut[]`, ordered by creation time.

### Create a provider

```http
POST /ai-providers
Content-Type: application/json
```

```json
{
  "name": "My OpenRouter",
  "base_url": "https://openrouter.ai/api/v1",
  "models": ["gpt-4o", "gpt-4o-mini"],
  "api_keys": ["sk-...", "sk-..."],
  "is_active": true
}
```

- `name` — unique, non-empty.
- `base_url` — `http`/`https` URL with a host; a trailing slash is stripped. Generation calls go to `{base_url}/chat/completions`.
- `models` — at least one non-empty model name; duplicates and blank entries are dropped.
- `api_keys` — at least one non-empty key. Keys are stored encrypted, never returned in full, and start with status `unknown`. A key already stored on another provider or API type returns `409 Conflict`.
- `is_active` — default `true`. Inactive providers are excluded from automatic (`"auto"`) selection; a channel pinned to the provider still uses it.

Response: `201 Created` -> `AICustomProviderOut`. Emits `ai_provider.created`.

### Get one provider

```http
GET /ai-providers/{provider_id}
```

Response: `200 OK` -> `AICustomProviderOut`, or `404 Not Found`.

### Update a provider

```http
PUT /ai-providers/{provider_id}
Content-Type: application/json
```

All fields are optional; omitted fields keep their stored values:

```json
{
  "name": "Renamed",
  "base_url": "https://api.deepseek.com/v1",
  "models": ["deepseek-chat"],
  "api_keys": ["sk-..."],
  "is_active": false
}
```

Providing `models` or `api_keys` replaces the whole stored list. Replacing `api_keys` resets every key status to `unknown`. Renaming to an existing name, or a key already stored elsewhere, returns `409 Conflict`.

Response: `200 OK` -> `AICustomProviderOut`. Emits `ai_provider.updated`.

### Delete a provider

```http
DELETE /ai-providers/{provider_id}
```

Response: `204 No Content`, or `404 Not Found`. The provider and all its stored API keys are removed immediately. Channels referencing it (`custom:<id>`) fall back to their templates until reconfigured. Emits `ai_provider.deleted`.

---

## AI prompts

The two prompts used for AI metadata generation during uploads: one for the video title and one for the description. They are stored server-side and used for every channel that has an AI model configured.

### Get the generation prompts

```http
GET /ai-prompts
```

Response: `200 OK`:

```json
{
  "title_prompt": "...",
  "description_prompt": "..."
}
```

Unset prompts fall back to built-in defaults.

### Update the generation prompts

```http
PUT /ai-prompts
Content-Type: application/json
```

```json
{
  "title_prompt": "Watch the video and generate a catchy title about cats.",
  "description_prompt": "Describe the video content in detail."
}
```

Both fields are optional; omitted prompts keep their stored values. Prompts must be non-empty and at most 20000 characters. Emits `ai_prompts.updated`.

---

## AI metadata generation during uploads

Each API type has its own provider implementation. For `google_gemini` the server uses the official `google-genai` SDK through its asynchronous client; the video upload to the Gemini Files API is a resumable upload streamed in 8 MB chunks, and the transfer is booked in the global upload memory budget (`MAX_UPLOAD_MEMORY_BYTES`) exactly like a YouTube upload, so concurrent AI and YouTube transfers share one limit.

When a channel has an AI model configured (`ai_model` in [Update channel](#update-channel)), the server generates the video title (and description, if enabled) with the saved prompts before creating the YouTube upload session:

1. The video file is uploaded to the Gemini Files API and processed.
2. A `generateContent` request is sent with the video plus the saved title prompt (and the saved description prompt when description generation is enabled for the channel).
3. The model's answer is parsed as JSON. The title is truncated to 100 characters and the description to 5000 bytes (YouTube's official limits); the description is truncated without splitting multi-byte UTF-8 characters.

The title and the description are requested in a **single** generation call (one JSON answer with both fields), so one field cannot fail while the other succeeds at the request level. A partial answer is accepted: if the model returns only a title, or only a description, the AI part is used and the missing part falls back to the channel template. If the whole request fails, both parts fall back. The same applies to a cached entry with an empty part.

API keys are tried **sequentially until one works**, ordered by health (`working` → `unknown` → `quota_exceeded` → `not_working`). The video is uploaded once per key and every model of the API type is then tried against that uploaded file — a model-level failure (for example the model was not found) does not re-upload the video. When all models fail at model level while the key itself works, the remaining keys of the type are skipped: the same models would fail there too, and their quota is not wasted; the search continues with the next API type instead.

The search runs in **rounds**: one pass over the key lists of every connected provider. When a round ends without a working key, the search starts a new round after `AI_KEY_RETRY_DELAY_SECONDS` (default 30 s), and each key is attempted at most `AI_KEY_MAX_ATTEMPTS` times (default 3, the first pass counts as one). Only keys that hit the provider's specific quota-exhausted error are skipped in later rounds; rate limits, temporary overload, 5xx and network failures keep their remaining attempts. Everything is still bounded by the per-video budget of `AI_GENERATION_TIMEOUT_SECONDS`.

In `auto` selection the API types and custom providers are tried in creation order (unified) and their models are ordered by token cost, most costly first (built-in Gemini: `pro` > other/unknown premium names > `flash` > `flash-lite`; OpenAI-compatible: premium markers like `pro`/`ultra`/`max`/`o1`/`o3` > unknown names > `mini`/`nano`/`lite`/`small`/`flash`): automatic mode looks for the first working model with the highest token expenditure. A channel pinned to one type or provider uses the stored model order.

Key statuses are refreshed on every request made with the key. Gemini errors map as follows: HTTP 429 `RESOURCE_EXHAUSTED` → `quota_exceeded`; 400 with an "API key not valid" message, 401 `UNAUTHENTICATED` and 403 `PERMISSION_DENIED` → `not_working`; a successful request → `working`. For OpenAI-compatible providers a 429 maps to `quota_exceeded` only for the billing/quota error codes (`insufficient_quota`, `credit_balance_exhausted`, the spend/usage-limit codes) or a quota/billing message — other 429s are transient. Model-not-found (404), server errors (5xx) and network errors are transient: the key status is left unchanged and the message is recorded in the key's `last_error`. If no saved model works — or the per-video budget of `AI_GENERATION_TIMEOUT_SECONDS` (default 900 s) expires — the title and description fall back to the channel templates exactly as for a channel without AI settings; the upload itself never fails or stalls indefinitely because of the AI layer. Titles or descriptions supplied by the client with the upload always win over AI generation.

### Generation cache

Every successful generation is stored in a per-video cache keyed by the file hash: at most 10 unique (title, description) pairs per video. With the channel's `ai_use_cached` flag enabled, uploads first check the cache — once all 10 entries exist for the video, a random saved entry is used and no AI request is made; below the maximum, new metadata is generated (and stored). Without the flag the server always generates new metadata (still storing it, so the cache fills up). A cached entry with an empty description leaves the description to the channel template.

---

## Channels

### Start OAuth authorization

The server checks that at least one project exists. If not, it returns `400 Bad Request`. If `channel_id` is passed, the server verifies the channel exists and its session is **not** already `active`; otherwise it returns `409 Conflict`.

```http
POST /channels/auth/start
Content-Type: application/json
```

Body:

```json
{
  "project_id": null,
  "redirect_uri": "http://localhost:8080/callback",
  "channel_id": null,
  "channel_title": "My Channel",
  "folder_from_channel_id": null,
  "proxy_id": "uuid-or-null",
  "proxy_auto_switch": false
}
```

- `project_id` — optional. If omitted, the server selects an active project.
- `redirect_uri` — optional. If omitted, the project's default `redirect_uri` is used. **Must be registered in the Google Cloud Console for this client_id**, otherwise Google returns `400 redirect_uri_mismatch`.
- `channel_id` — optional. Pass this to re-authorize an existing channel (for example after `session_status` becomes `expired` or `reauth_required`).
- `channel_title` — optional free-form display name used only while the channel is not yet connected.
- `folder_from_channel_id` — optional. If provided, the new channel uses the same folder as the referenced channel. Otherwise the server creates a dedicated folder under `VIDEO_BASE_DIR`. The referenced channel's own folder is never modified. A circular reference (the referenced channel's folder leading back to the channel being re-authorized) is rejected with `400`; the same check re-runs at the callback, where a cycle that appeared in the meantime skips the folder adoption instead of failing the authorization.
- `proxy_id` — optional proxy to use for OAuth, token refresh, channel metadata requests, and uploads.
- `proxy_auto_switch` — enables automatic replacement of a failed proxy using the least-used working proxy from the global list.

Response: `200 OK`

```json
{
  "state": "random-state-string",
  "authorization_url": "https://accounts.google.com/o/oauth2/auth?...",
  "project_id": "uuid",
  "channel_id": "uuid-or-null",
  "expires_at": "2026-01-01T01:00:00Z",
  "expires_in_seconds": 3600,
  "proxy_settings": {
    "proxy_id": "uuid-or-null",
    "auto_switch": false,
    "proxy": {
      "id": "uuid",
      "url": "socks5://user:password@example.com:1080",
      "is_active": true,
      "assigned_channels": 2,
      "last_checked_at": "2026-01-01T00:00:00Z",
      "last_check_ok": true,
      "failure_count": 0,
      "created_at": "2026-01-01T00:00:00Z",
      "updated_at": "2026-01-01T00:00:00Z"
    }
  }
}
```

The OAuth state is valid until `expires_at` (UTC, ISO 8601) — 60 minutes by default (`OAUTH_STATE_TTL_MINUTES`). `expires_in_seconds` is the remaining validity in seconds at response time. The callback returns `400 Invalid or expired state` once the state has expired.

Starting an authorization is always fast. When `channel_id` refers to a channel whose local session status is `active`, the server performs a best-effort verification first (token refresh when needed + YouTube `channels.list`) bounded by `OAUTH_SESSION_PRECHECK_TIMEOUT_SECONDS` (default 8s): if YouTube confirms the session, the request answers `409 Conflict` (re-authorization is not needed); if the refresh token is rejected, the session is marked `expired` and the authorization URL is issued. If YouTube cannot be reached in time (dead/slow proxy, timeout) or the check is disabled (`OAUTH_SESSION_PRECHECK_TIMEOUT_SECONDS=0`), the authorization URL is issued anyway after the deadline — re-authorization is harmless, and the callback re-validates the channel and replaces the tokens. If the channel is waiting for a working proxy (`proxy_recovery_pending`), the server returns `503` immediately. In every case where the URL is issued, the channel's own project is preferred (re-authorization semantics), so the flow works even when it is the only configured project.

The same payload is also broadcast via SSE as `channel.auth_url`. For re-authorization, `channel.reauth_required` includes the same `proxy_settings` object together with the authorization URL and the same `expires_at` / `expires_in_seconds` fields. The client should use these settings when displaying the re-authorization action and must not assume that re-authorization uses a direct connection.

### Complete OAuth authorization

After the user authorizes in the browser, Google redirects to `redirect_uri` with `?code=...&state=...`. The client sends the code and state back to the server.

```http
POST /channels/auth/callback
Content-Type: application/json
```

Body:

```json
{
  "state": "random-state-string",
  "code": "authorization-code-from-google",
  "tag_ids": ["uuid-of-existing-tag"]
}
```

`tag_ids` is optional and is supplied at completion, after browser authorization,
not in `/channels/auth/start`. Obtain IDs from `GET /tags` or create tags with
`POST /tags`. A supplied list replaces the channel's tags; duplicates are ignored,
`[]` clears them, and omitted/null preserves existing tags on reauthorization.
Unknown IDs return `404` before the OAuth code is exchanged; malformed UUIDs
return `422`. Tags are saved only after successful authorization and included in
the response and `channel.connected` event.

The server:

1. Exchanges the code for tokens.
2. Fetches the real YouTube channel ID, title, handle, avatar, subscriber count, and total view count from the YouTube Data API.
3. Checks for an existing channel with the same `youtube_channel_id` (ignoring deleted channels). If it exists, the operation fails with `409 Conflict`.
4. If `channel_id` was provided in the original state, updates the existing channel; otherwise creates a new one.
5. Starts the upload worker for the channel.

Response: `200 OK` → `ChannelOut`

### List channels

```http
GET /channels
```

Response: `200 OK` → `ChannelOut[]` (includes channels with `status: deleted`)

### Get channel

```http
GET /channels/{channel_id}
```

Response: `200 OK` → `ChannelOut`

### Get channel session status

```http
GET /channels/{channel_id}/session-status
```

Response: `200 OK` → `ChannelSessionStatusOut`

### Get channel avatar

Returns the current avatar URL. The server refreshes all channel metadata automatically every `CHANNEL_SNIPPET_REFRESH_INTERVAL_SECONDS` (default 300) and emits `channel.avatar_updated` when the avatar changes.

```http
GET /channels/{channel_id}/avatar
```

Response: `200 OK` → `ChannelAvatarOut`

### List videos in the channel folder

Returns file names and sizes of all video files in the channel's `folder_path`. Files that have already been uploaded (known by hash) are still listed here.

```http
GET /channels/{channel_id}/folder-videos
```

Response: `200 OK` → `FolderVideoOut[]`

### Update channel

```http
PUT /channels/{channel_id}
Content-Type: application/json
```

Body (all optional):

```json
{
  "title": "New Channel Title",
  "folder_from_channel_id": "uuid-of-another-channel",
  "title_template": "{filename} - my channel",
  "description_template": "Uploaded on {date}\nOriginal file: {file}",
  "privacy_status": "public",
  "default_category_id": "22",
  "auto_upload_enabled": true,
  "status": "active",
  "max_batch_size": 200,
  "min_batch_size": 1,
  "inter_upload_delay_seconds": 2,
  "shuffle_upload_order": false,
  "skip_blocked_videos": false,
  "ai_model": "auto",
  "ai_generate_description": true,
  "ai_use_cached": false
}
```

Valid `ai_model` values: `null`, `"auto"`, a supported API type name such as `"google_gemini"`, or `custom:<provider_id>`.

- `folder_from_channel_id` — optional. A channel id switches the channel's folder to the referenced channel's folder (uploaded-video hashes are preserved). Passing an explicit `null` switches the channel back to its own dedicated folder under `VIDEO_BASE_DIR` (created on demand). An absent key leaves the folder unchanged. Only the updated channel's folder changes — the referenced channel keeps its folder exactly as it was. Circular references are rejected with `400`: the request fails when the referenced channel's folder leads back to this channel (directly — A uses B's folder while B uses A's — through a longer chain, or when the channel references itself), and the error lists the ids forming the cycle. The same guard runs in `POST /channels/auth/start` (immediate `400`) and re-checks at `POST /channels/auth/callback` — if the folder graph changed between start and callback, the adoption is skipped (the channel keeps its current folder) instead of failing the already-exchanged authorization.
- `max_batch_size` — optional. Number of videos uploaded in one batch. Must be between 1 and 200 (default 200).
- `min_batch_size` — optional. Minimum number of eligible videos to accumulate while the client is actively uploading (1–200, default 1). Once the client has been idle for `MIN_BATCH_FLUSH_IDLE_SECONDS` without new uploads, the remaining queue is flushed anyway. Must be `<= max_batch_size`.
- `inter_upload_delay_seconds` — optional. Delay between consecutive uploads, in seconds. Must be 0–604800, i.e. at most one week (default 2).
- `shuffle_upload_order` — optional. When `true`, waiting videos are randomized before each batch. For a shared folder, videos unused by other sharers are preferred, then the least-used group is randomized (default `false`).
- `skip_blocked_videos` — optional. Enables asynchronous post-upload processing/status checks and skips file hashes already recorded as copyright-blocked in all countries (default `false`).
- `ai_model` — optional. `null` disables AI metadata generation, `"auto"` lets the server pick the first working active [API type](#ai-types) or custom provider, a type name (e.g. `"google_gemini"`) pins the channel to that type, `custom:<provider_id>` pins it to an [AI custom provider](#ai-custom-providers-openai-compatible) (`400` if the provider does not exist). Unsupported values return `422`. Default `null`.
- `ai_generate_description` — optional. When `true`, the saved description prompt is also used and the model's description is applied to uploads (default `false`). See [AI metadata generation during uploads](#ai-metadata-generation-during-uploads).
- `ai_use_cached` — optional. When `true`, uploads reuse a random cached generation for the video once its hash has the maximum of 10 unique saved entries; otherwise (default `false`) every upload generates new metadata. See [Generation cache](#generation-cache).
- Available template variables: `{filename}`, `{file}`, `{ext}`, `{date}`, `{datetime}`.

Response: `200 OK` → `ChannelOut`

Saving stays fast even while a large upload batch is running: the upload path resolves the channel's proxy route from a short-lived cache (a few seconds) instead of opening a database session before every uploaded chunk, and every proxy assignment change (`PUT /channels/{channel_id}` with `proxy_id`, automatic proxy switching, recovery scheduling, channel deletion) invalidates the cached route immediately, so in-flight uploads pick up the new proxy on their very next chunk.

The same guarantee holds for work unrelated to YouTube uploads. Read transactions are never held across slow non-database operations — OAuth token refreshes and YouTube metadata requests (which can hang on a dead proxy for up to the HTTP timeout), streaming client-uploaded video files to disk, and hashing newly discovered files — because an open SQLite WAL snapshot blocks WAL checkpoints for its whole lifetime, grows the WAL without bound, and slows every subsequent database session and write. Periodic task cleanup (`cleanup_old_tasks`, max 500 rows kept per channel) runs as a single bulk delete instead of one DELETE per row, so it never holds SQLite's single write lock for long.

### Batch settings

`GET /channels/{channel_id}/batch-settings` returns the current batch configuration:

```json
{
  "max_batch_size": 200,
  "min_batch_size": 1,
  "inter_upload_delay_seconds": 2,
  "shuffle_upload_order": false,
  "skip_blocked_videos": false
}
```

`PUT /channels/{channel_id}/batch-settings` updates the batch configuration. All fields are optional; values that are not provided stay unchanged. `shuffle_upload_order` (default `false`) randomly shuffles the waiting videos before forming each upload batch when enabled. `skip_blocked_videos` (default `false`) enables skipping videos previously identified as copyright-blocked in all countries.

```http
PUT /channels/{channel_id}/batch-settings
Content-Type: application/json

{
  "max_batch_size": 100,
  "min_batch_size": 10,
  "inter_upload_delay_seconds": 5,
  "shuffle_upload_order": true,
  "skip_blocked_videos": true
}
```

Validation: `min_batch_size` must be `<= max_batch_size` (otherwise the server responds with `400`).

Response: `200 OK` → the same `ChannelBatchSettings` JSON.

### Skip copyright-blocked videos

Set `skip_blocked_videos` to `true` in `PUT /channels/{channel_id}`, `PUT /channels/{channel_id}/batch-settings`, or `PUT /channels/bulk-settings` to enable this behavior for a channel. The default is `false`.

For every successfully transferred video, the worker asynchronously polls YouTube until processing reaches a terminal state. It then recognizes a global copyright block only if YouTube reports copyrighted/licensed content that is unavailable in every country (`contentDetails.regionRestriction.allowed` is an empty array), or a terminal copyright/claim rejection. A restriction affecting only some countries does **not** mark the file as blocked.

When such a block is confirmed, the worker first deletes that YouTube video using `videos.delete`. HTTP `404` is accepted as success because the video is already absent. Transient and API errors are retried asynchronously, so the channel does not select another video before deletion has completed. Only then is the file hash written to the global blocked-video registry.

If a batch uploads several videos concurrently, their processing polls and deletions run concurrently as asyncio coroutines; token refreshes and database writes are serialized to avoid races. The worker waits for every post-upload check in the batch before it returns to normal file selection. For a shared folder, a lightweight folder gate also prevents another channel with an already-collected batch from starting a new transfer until these checks and required deletions finish; the transfer lock and upload-memory reservation themselves are not held during polling. Future folder scans for channels with this option enabled exclude hashes in the registry and select the next eligible file through the normal queue and upload pipeline; no duplicate selection path is used. When a file is added to the registry, the server emits `video.blocked`.

### Upload limit

Each channel can be configured with a sliding-window upload limit. The server counts only `completed` tasks whose `completed_at` timestamp falls inside the current window. If the limit is reached, the channel is paused with `pause_reason: "upload_limit"`. Changing the limit settings recalculates the window immediately and unpauses the channel only if it was paused because of the limit (`pause_reason == "upload_limit"`). Pauses caused by YouTube-side limits (`pause_reason == "youtube_rate_limit"`) or manual pauses are not affected.

`GET /channels/{channel_id}/upload-limit` returns the current state:

```json
{
  "enabled": true,
  "count": 50,
  "period_hours": 24,
  "uploads_in_current_period": 12,
  "period_started_at": "2026-01-01T00:00:00Z",
  "period_ends_at": "2026-01-02T00:00:00Z",
  "is_limited": false,
  "remaining": 38,
  "reset_at": "2026-01-02T00:00:00Z"
}
```

- `enabled` — whether the per-channel limit is active.
- `count` — maximum uploads inside the window.
- `period_hours` — window size in hours.
- `uploads_in_current_period` — completed uploads inside the current window.
- `period_started_at` / `period_ends_at` — current window boundaries.
- `is_limited` — true when `uploads_in_current_period >= count`.
- `remaining` — how many uploads are still allowed in the current window.
- `reset_at` — same as `period_ends_at`.

`PUT /channels/{channel_id}/upload-limit` updates the limit:

```http
PUT /channels/{channel_id}/upload-limit
Content-Type: application/json
```

Body:

```json
{
  "enabled": true,
  "count": 100,
  "period_hours": 24
}
```

All fields are optional. Setting `enabled` to `false` disables the limit and removes the `upload_limit` pause reason. Response: `200 OK` → `ChannelOut`.

### Bulk update channel settings

Applies the same settings to several channels in one request. Only the fields you provide are changed; everything else stays as-is for each channel.

```http
PUT /channels/bulk-settings
Content-Type: application/json
```

Body:

```json
{
  "channel_ids": ["uuid-1", "uuid-2", "uuid-3"],
  "max_batch_size": 100,
  "min_batch_size": 5,
  "inter_upload_delay_seconds": 4,
  "shuffle_upload_order": true,
  "skip_blocked_videos": true,
  "upload_limit_enabled": true,
  "upload_limit_count": 50,
  "upload_limit_period_hours": 24,
  "proxy_auto_switch": true,
  "ai_model": "google_gemini",
  "ai_generate_description": true,
  "ai_use_cached": false
}
```

- `channel_ids` — required, non-empty list of channel UUIDs. Duplicates are ignored. Deleted channels are treated as not found.
- Batch upload: `max_batch_size` (1–200), `min_batch_size` (1–200), `inter_upload_delay_seconds` (0–604800, at most one week), `shuffle_upload_order` (bool), `skip_blocked_videos` (bool). Same meaning and defaults as `PUT /channels/{channel_id}/batch-settings`.
- Upload limit: `upload_limit_enabled` (bool), `upload_limit_count` (>= 1), `upload_limit_period_hours` (>= 1). Same meaning as `PUT /channels/{channel_id}/upload-limit`. Changing these recalculates each channel's limit window and may pause/unpause it exactly like the single-channel endpoint.
- `proxy_auto_switch` — bool. Turns automatic proxy switching after a proxy failure on or off for each channel.
- AI metadata: `ai_model` (`null`, `"auto"`, a supported [API type](#ai-types) name, or `custom:<provider_id>` — here an explicit `null` **disables** AI generation for the listed channels, unlike other null fields which are ignored), `ai_generate_description` (bool), `ai_use_cached` (bool). Same meaning as in `PUT /channels/{channel_id}`.

All settings fields are optional; at least one must be provided (otherwise `400`). Validation is all-or-nothing: `min_batch_size` must be `<= max_batch_size` for every listed channel after merging with its current values, otherwise the whole request is rejected with `400` and nothing is applied. The request is applied in one short transaction and stays fast even while upload batches or proxy scans are running (see the proxy-route caching note under `PUT /channels/{channel_id}`).

Response: `200 OK`

```json
{
  "updated": [ChannelOut, ChannelOut],
  "not_found": ["uuid-that-was-missing"]
}
```

- `updated` — the channels that were changed, as full `ChannelOut` objects.
- `not_found` — ids that do not exist or are deleted. If no channel from the list exists, the server responds with `404`.

### Bulk update channel video settings

Applies the video metadata defaults to several channels in one request: video title template, description template, privacy, and category. Only the fields you provide are changed; everything else stays as-is for each channel. The channel's own display title cannot be changed here — it stays per-channel (use `PUT /channels/{channel_id}` for that).

```http
PUT /channels/bulk-video-settings
Content-Type: application/json
```

Body:

```json
{
  "channel_ids": ["uuid-1", "uuid-2", "uuid-3"],
  "title_template": "{filename}",
  "description_template": "Uploaded on {date}",
  "privacy_status": "public",
  "default_category_id": "22"
}
```

- `channel_ids` — required, non-empty list of channel UUIDs. Duplicates are ignored. Deleted channels are treated as not found.
- `title_template` — video title template. Same rules as the single-channel endpoint; available template variables: `{filename}`, `{file}`, `{ext}`, `{date}`, `{datetime}`.
- `description_template` — video description template. Same rules as the single-channel endpoint; available template variables: `{filename}`, `{file}`, `{ext}`, `{date}`, `{datetime}`.
- `privacy_status` — `public` | `unlisted` | `private` (default `public`).
- `default_category_id` — YouTube category id as a string, e.g. `"22"`. Pass `""` to clear it.

All fields are optional; at least one must be provided (otherwise `400`). Explicit `null` values are ignored. Validation is all-or-nothing: the title and description templates are checked against each channel's merged values (a template with an unknown placeholder, an empty resulting video title, or content that exceeds YouTube's title/description limits rejects the whole request with `400` and nothing is applied).

Response: `200 OK`

```json
{
  "updated": [ChannelOut, ChannelOut],
  "not_found": ["uuid-that-was-missing"]
}
```

Same result shape as `PUT /channels/bulk-settings`: `updated` contains the changed channels as full `ChannelOut` objects; `not_found` lists ids that do not exist or are deleted. If no channel from the list exists, the server responds with `404`. Each changed channel also triggers a `channel.updated` SSE event.

### Bulk update channel video folders

Switches the video folder of several channels at once — the bulk twin of the `folder_from_channel_id` field on `PUT /channels/{channel_id}`.

```http
PUT /channels/bulk-folder-settings
Content-Type: application/json
```

Body:

```json
{
  "channel_ids": ["uuid-1", "uuid-2", "uuid-3"],
  "folder_from_channel_id": "uuid-source-or-null"
}
```

- `channel_ids` — required, non-empty list of channel UUIDs. Duplicates are ignored. Deleted channels are treated as not found.
- `folder_from_channel_id` — required but nullable. A channel id switches every listed channel to that channel's folder (uploaded-video hashes are preserved per channel). An explicit `null` switches every listed channel back to its own dedicated folder under `VIDEO_BASE_DIR` (created on demand). The key must be present — omitting it is a `422`.

Only the listed channels' folders change; the source channel keeps its folder exactly as it was.

Validation is all-or-nothing and runs for **every** listed channel before anything is applied, so one bad choice saves nobody:

- the source channel must exist (`404` otherwise), must not be deleted, and must have a folder configured;
- a channel cannot reference its own folder (`400`, the error names the channel);
- the adoption must not create a circular folder reference — directly or through a chain (`400`, the error names the offending channel and lists the cycle).

Any rejected request leaves every channel's folder untouched. Folder changes apply in one transaction, and each changed channel triggers a `channel.updated` SSE event.

Response: `200 OK`

```json
{
  "updated": [ChannelOut, ChannelOut],
  "not_found": ["uuid-that-was-missing"]
}
```

If no channel from the list exists, the server responds with `404`.

### Get uploaded count

Returns the total number of completed uploads for the channel and the current configurable-limit state.

```http
GET /channels/{channel_id}/uploaded-count
```

Response: `200 OK` → `ChannelUploadedCount`

### Get uploaded videos count

Returns only the number of successfully uploaded (`completed`) videos for the channel.

```http
GET /channels/{channel_id}/videos/count
```

Response: `200 OK`

```json
{
  "channel_id": "uuid",
  "uploaded_count": 42
}
```

### Pause channel

Manually pause the channel. Sets `status` to `paused` and `pause_reason` to `"manual"`. The channel will not auto-resume; use `POST /channels/{channel_id}/unpause` to resume. The active worker for the channel is stopped.

```http
POST /channels/{channel_id}/pause
```

Response: `200 OK` → `ChannelOut`

### Auto-resume

Non-manual pauses (for example `pause_reason == "upload_limit"` or `"youtube_rate_limit"`) are automatically cleared once their `paused_until` time passes, or immediately if no `paused_until` is set. This happens during the regular folder scan and inside the channel worker, so a channel that was paused by a YouTube upload limit will resume uploading as soon as the limit window expires. Manual pauses are never touched automatically.

### Unpause channel

Clears `paused_until` and wakes the upload worker.

```http
POST /channels/{channel_id}/unpause
```

Response: `200 OK` → `ChannelOut`

### Clear error

Same as unpause; resets `status` from `error` to `active` and wakes the worker.

```http
POST /channels/{channel_id}/clear-error
```

Response: `200 OK` → `ChannelOut`

### Delete channel (hard delete)

Permanently removes the channel, all its upload tasks, uploaded-video hashes, saved project auth data, and stops the worker. YouTube videos are **not** deleted.

```http
DELETE /channels/{channel_id}
```

Response: `204 No Content`. A project assigned to one or more channels cannot be deleted. Therefore, deleting all projects while active channels still exist is rejected with `409 Conflict`; the channels keep working with their current credentials. The server returns `409 Conflict` until those channels are re-authorized or moved to another project. Pending OAuth states and saved per-project channel credentials for a deletable project are removed together with the project.

---

## Uploads

### Manual upload

Saves the video file into the channel's `folder_path` and returns information about the saved file. The endpoint does **not** upload the video to YouTube immediately and does **not** create an upload task; the channel worker picks up the file during its regular scan.

Before the file is kept, its hash is checked against the channel's `UploadedVideoHash` and the global `RejectedVideoHash`. If the exact file was already uploaded, or was previously rejected by YouTube as invalid, the file is discarded and `skipped` is returned as `true`.

If a file with the same name already exists in the channel folder it is overwritten.

File writes participate in a global memory-based backpressure limiter: if too many large files are being received at the same time, additional uploads are queued until memory is available.

```http
POST /uploads/{channel_id}/upload
Content-Type: multipart/form-data
```

Form fields:

- `file` — video file (required)
- `title` — optional, ignored at save time (the channel templates are used when the upload task is created)
- `description` — optional, ignored at save time

Response: `201 Created`

```json
{
  "file_name": "video.mp4",
  "size_bytes": 123456789,
  "modified_at": "2026-01-01T00:00:00Z",
  "skipped": false,
  "reason": null,
  "title": null,
  "description": null
}
```

### Batch upload

Upload multiple video files at once. Each file is saved into the channel folder, checked against `UploadedVideoHash` and the global `RejectedVideoHash`, and duplicates or previously rejected files are discarded. Existing files with the same name are overwritten. No upload tasks are created; the channel worker creates tasks during its scheduled scan.

The endpoint returns as soon as files are saved so that long-running batches do not keep the HTTP connection open longer than necessary.

```http
POST /uploads/{channel_id}/upload-batch
Content-Type: multipart/form-data
```

Form fields:

- `files` — list of video files (required)

Response: `202 Accepted`

```json
[
  {
    "file_name": "video1.mp4",
    "size_bytes": 123456789,
    "modified_at": "2026-01-01T00:00:00Z",
    "skipped": false,
    "reason": null
  },
  {
    "file_name": "video2.mp4",
    "size_bytes": null,
    "modified_at": null,
    "skipped": true,
    "reason": "Video already uploaded"
  }
]
```

### Reverse-proxy and timeout notes

Uploading many large files in a single batch keeps the HTTP request open until every file is written to disk and hashed. If a reverse proxy (for example nginx) sits in front of the server, its default read/send timeouts may close the connection before the server can respond, which the client sees as a network error such as `winerror 10053`.

Recommended mitigations:

- Increase the proxy timeout for the upload routes. For nginx set `proxy_read_timeout 300s;` and `proxy_send_timeout 300s;` (or higher) inside the `location` that proxies `/uploads/`.
- Set `client_max_body_size` high enough for the largest batch you intend to upload, or use `0` to disable the limit.
- Split very large batches into smaller ones. The batch endpoint saves up to three files in parallel, but each extra file still adds to the total request time.
- When possible prefer the single-file `POST /uploads/{channel_id}/upload` endpoint and send files sequentially from the client; this gives one short request per file and is the most reliable pattern through restrictive proxies.

### Delete file from the channel folder

Removes a file from the channel's `folder_path`. If there is an active upload task for this file, it is cancelled first. The uploaded-video hash is **not** removed, so the file will not be re-uploaded automatically if it appears again.

```http
DELETE /uploads/{channel_id}/files/{file_name}
```

`file_name` must be the exact file name with extension, URL-encoded if necessary (e.g. `my%20video.mp4`).

Response: `200 OK`

```json
{"ok": true, "channel_id": "uuid", "file_name": "video.mp4", "task_id": "uuid-or-null"}
```

---

## Tasks

### List tasks

```http
GET /tasks
```

Query parameters:

- `channel_id` — filter by channel UUID
- `active=true` — show only active tasks (`pending`, `session_created`, `uploading`)
- `limit` — optional. Maximum number of tasks to return (1–1000), newest first. Without it the full task history is returned, which can be megabytes of JSON — clients that display a bounded list should always pass a `limit`.
- `status` — comma-separated list of statuses, e.g. `status=completed,failed`

Response: `200 OK` → `TaskOut[]`

### Get task

```http
GET /tasks/{task_id}
```

Response: `200 OK` → `TaskOut`

---

## Updates (SSE)

Real-time server-sent events stream. Every mutation of projects, channels, tasks, or uploaded-video hashes is pushed to all connected clients.

```http
GET /updates/sse?subscriber_id=<optional-existing-id>
Accept: text/event-stream
```

- `subscriber_id` — optional. Pass the id from a previous `connection.opened` event to resume that session and replay unacknowledged events.

### Connection handshake

Immediately after connecting, the server sends:

```text
event: connection.opened
data: {"subscriber_id":"<uuid>","pending_count":0}
```

- `subscriber_id` — required for `POST /updates/ack`.
- `pending_count` — number of unacknowledged events already waiting for this client (for example after a reconnect).

If there are pending events, they are replayed before new events. Each event has a unique `id`:

```text
id: <event-id>
event: project.created
data: {"project":{...}}
```

### Acknowledge events

Events stay buffered on the server until the client explicitly acknowledges them. This prevents updates from being lost when the server is on a remote host with an unstable connection.

```http
POST /updates/ack
Content-Type: application/json
```

Body:

```json
{
  "subscriber_id": "<uuid-from-connection.opened>",
  "event_ids": ["<event-id-1>", "<event-id-2>"]
}
```

Response: `200 OK`

```json
{"acknowledged": 2}
```

The client should ack events after successfully processing them. If it disconnects before acking, the events are replayed on the next connection.

### Event reference

| Event | Payload | Description |
|-------|---------|-------------|
| `connection.opened` | `{"subscriber_id":"...","pending_count":0}` | Sent once per connection. |
| `project.created` | `{"project": ProjectOut}` | New GCP project saved. |
| `project.updated` | `{"project": ProjectOut}` | GCP project edited. |
| `project.deleted` | `{"project_id":"..."}` | GCP project removed. |
| `channel.auth_url` | `{"state":"...","authorization_url":"...","channel_id":"...","project_id":"...","expires_at":"2026-01-01T01:00:00Z","expires_in_seconds":3600,"proxy_settings":{...}}` | OAuth URL generated. `proxy_settings` describes the proxy that will be used for OAuth and subsequent channel requests. The OAuth state is valid until `expires_at` (UTC) — 60 minutes by default; `expires_in_seconds` is the remaining validity in seconds. |
| `channel.connected` | `{"channel": ChannelOut}` | New YouTube channel connected. |
| `channel.updated` | `{"channel": ChannelOut}` | Channel settings, snippet, or session status updated. Also emitted after each completed upload inside a batch (with `last_upload_at`); the payload is always re-read from the database, so changes saved while a batch runs (for example the video folder) are never reverted by this event. |
| `channel.metadata_updated` | `{"channel_id":"...","changed_fields":["subscriber_count","view_count"],"metadata":{"youtube_title":"...","handle":"@...","thumbnail_url":"...","subscriber_count":125000,"view_count":9876543,"metadata_updated_at":"..."},"channel":ChannelOut}` | One or more real YouTube metadata fields changed during periodic synchronization. `changed_fields` identifies exactly which values changed. |
| `channel.deleted` | `{"channel_id":"..."}` or `{"channel": ChannelOut}` | Hard-delete or the channel was removed on YouTube (`status` becomes `deleted`). |
| `channel.paused` | `{"channel_id":"...","reason":"youtube_rate_limit","minutes":60}` or `{"channel_id":"...","reason":"upload_limit"}` or `{"channel_id":"...","reason":"manual","minutes":60}` | Channel paused. `youtube_rate_limit` is emitted by the YouTube upload/quota limit test session; `upload_limit` is emitted when the per-channel upload-limit window is exhausted; `manual` is emitted by `POST /channels/{id}/pause`. Only the *test* (first) upload session of a batch triggers the YouTube rate-limit pause. If the limit is hit later during the batch, those tasks fail without retry and the next batch starts with a fresh test session. |
| `channel.unpaused` | `{"channel": ChannelOut}` | Channel manually unpaused. |
| `channel.error` | `{"channel_id":"...","error":"..."}` | Channel moved to `error` state. |
| `channel.session_expired` | `{"channel": ChannelOut, "message":"..."}` | Refresh token stopped working; re-authorization is required. |
| `channel.reauth_required` | `{"channel": ChannelOut, "pending_project_id":"...","authorization_url":"...","expires_at":"2026-01-01T01:00:00Z","expires_in_seconds":3600,"proxy_settings":{...}}` | Re-authorization is required after a token refresh, metadata request, or upload request detects an authorization failure. Only one pending OAuth state is created until the client completes or restarts the flow. The payload includes the proxy settings that apply to the authorization URL and channel requests, plus the pending state's expiry time (`expires_at`, UTC) and remaining seconds (`expires_in_seconds`). |
| `channel.proxy_updated` | `{"channel": ChannelOut}` | Channel proxy or automatic switching setting changed. |
| `channel.proxy_switched` | `{"channel_id":"...","previous_proxy_id":"...","proxy_id":"...","proxy":ProxyOut}` | Automatic proxy replacement selected a working proxy with the fewest assigned channels. |
| `proxy.created` | `{"proxy":ProxyOut}` | Proxy added to the global list. |
| `proxy.updated` | `{"proxy":ProxyOut}` | Proxy settings or health state changed. Emitted after explicit checks and real requests through the proxy. |
| `proxy.deleted` | `{"proxy_id":"..."}` | Proxy removed from the global list. |
| `proxies.imported` | `{"received":N,"added":N,"duplicates":N}` | A client proxy list was imported. |
| `proxies.checked` | `{"checked":N,"working":N,"failed":N,"proxies":[ProxyOut]}` | A shared mass proxy check completed. |
| `ai_type.updated` | `{"ai_type": AITypeOut}` | API type settings (models / keys / active flag) saved or changed. |
| `ai_type.deleted` | `{"api_type":"..."}` | API type settings and its API keys removed. |
| `ai_provider.created` | `{"ai_provider": AICustomProviderOut}` | Custom OpenAI-compatible provider saved. |
| `ai_provider.updated` | `{"ai_provider": AICustomProviderOut}` | Custom provider edited. |
| `ai_provider.deleted` | `{"ai_provider_id":"..."}` | Custom provider and its API keys removed. |
| `ai_prompts.updated` | `{"title_prompt":"...","description_prompt":"..."}` | Generation prompts changed. |
| `channel.avatar_updated` | `{"channel_id":"...","thumbnail_url":"..."}` | Channel avatar URL changed. |
| `tasks.created` | `{"channel_id":"...","count":N,"files":["..."],"tasks":[TaskOut]}` | New upload tasks created by the channel worker from files found during a scheduled folder scan. |
| `task.updated` | `{"task": TaskOut}` | Task changed status (`pending` → `session_created`, etc.). |
| `task.completed` | `{"task": TaskOut,"video_id":"..."}` | Video successfully uploaded. |
| `video.blocked` | `{"task": TaskOut,"video_id":"...","reason":"copyright_blocked_all_countries"}` | The uploaded file was copyright-blocked in all countries and was added to the global blocked-video registry. Channels with `skip_blocked_videos` enabled skip its hash on subsequent scans. |
| `task.failed` | `{"task": TaskOut,"will_retry":false}` | Upload failed. The task is not retried automatically. |
| `video.rejected` | `{"task": TaskOut,"reason":"..."}` | The video file is permanently invalid (bad format, codec, etc.). It is added to the global rejected list and will be skipped in future scans and uploads. |
| `task.cancelled` | `{"task_id":"...","channel_id":"..."}` | Task was cancelled by client request. |
| `tasks.cancelled` | `{"channel_id":"...","count":N,"reason":"channel_deleted"}` | All active tasks cancelled (for example when a channel is deleted). |
| `video.deleted` | `{"channel_id":"...","video_id":"..."}` | Video removed from YouTube. |
| `uploaded_hash.deleted` | `{"channel_id":"...","video_id":"...","file_hash":"...","file_path":"..."}` | Local hash record removed. |
| `file.deleted` | `{"channel_id":"...","file_name":"...","task_id":"..."}` | File removed from the channel folder. |

`ProjectOut`, `ChannelOut`, `TaskOut`, `AITypeOut`, and `AICustomProviderOut` are the full model objects documented in [Common types](#common-types).

---

## Error responses

The API returns standard HTTP status codes:

- `400 Bad Request` — invalid input, no projects saved, expired OAuth state, etc.
- `401 Unauthorized` — missing or invalid `Authorization` header.
- `404 Not Found` — channel/project/proxy/AI model/task/file does not exist.
- `409 Conflict` — duplicate channel, channel already authorized, duplicate proxy name, AI provider name or API key, etc.
- `422 Unprocessable Entity` — request body failed validation (unknown `api_type`, empty model/key list, out-of-range values, etc.).
- `500 Internal Server Error` — unexpected server error.

Error body:

```json
{"detail": "Human readable message"}
```

---

## Channel tags

All endpoints below require the same Bearer token as the other API endpoints.
Tags are a shared catalog and may be assigned to multiple channels. Unassigned
tags remain in the catalog, including after a channel is deleted.

### `GET /tags`

Returns `200 OK` with all saved tags sorted by name:

```json
[{"id": "uuid", "name": "Gaming"}, {"id": "uuid", "name": "Shorts"}]
```

### `POST /tags`

Body: `{ "name": "Gaming" }`. Returns `200 OK` with `{ "id": "uuid", "name": "Gaming" }`.
Leading/trailing whitespace is trimmed. Names must contain 1–100 characters
after trimming; invalid input returns `422`. Names are case-sensitive.
Creating an existing name returns the existing tag without creating a duplicate.
Use the returned ID to attach the tag to a channel.

### `PUT /channels/{channel_id}/tags/{tag_id}`

No body. Assigns an existing tag to the channel and returns `200 OK` with
the updated `ChannelOut`, including its `tags` array. Repeating the request
does not duplicate the assignment.

### `DELETE /channels/{channel_id}/tags/{tag_id}`

No body. Removes the assignment and returns `200 OK` with the updated
`ChannelOut`. The tag remains in the shared catalog and on other channels.
Removing an already absent assignment also succeeds.

Both assignment endpoints return `404` if the channel or catalog tag does not
exist, and `422` for invalid UUIDs. They emit `channel.updated` with the full
updated channel. Tags are managed through these endpoints, not `ChannelUpdate`.
They are organizational channel labels, not YouTube video metadata tags.

The new `tag` and `channel_tag` tables are created automatically at server
startup for both new and existing databases. Existing channels initially have
an empty tag list; their saved authorization dates (`created_at`) are unchanged.

