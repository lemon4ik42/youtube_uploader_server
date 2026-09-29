import json
import logging
from pathlib import Path
from uuid import UUID

from sqlalchemy import inspect, text

from app.core.config import settings
from app.core.security import decrypt, encrypt

logger = logging.getLogger(__name__)


def _relative_or_original(path: str) -> str:
    """Convert an absolute path under base_dir to a relative one; otherwise keep it.

    Relative paths are interpreted from base_dir. Returns forward-slash paths so
    stored values are portable across platforms.
    """
    if not path:
        return path
    p = Path(str(path).replace("\\", "/")).expanduser()
    if not p.is_absolute():
        p = settings.base_dir / p
    try:
        return p.resolve().relative_to(settings.base_dir).as_posix()
    except ValueError:
        return p.resolve().as_posix()


def _table_exists(conn, table: str) -> bool:
    return inspect(conn).has_table(table)


def _column_exists(conn, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    return column in [c["name"] for c in inspect(conn).get_columns(table)]


def _drop_unique_indexes_on_column(conn, table: str, column: str) -> None:
    """Drop unique indexes/constraints that cover only the given column."""
    if not _table_exists(conn, table):
        return
    idx_info = inspect(conn).get_indexes(table)
    for idx in idx_info:
        if idx.get("unique") and column in idx.get("column_names", []):
            name = idx["name"]
            logger.info("Dropping unique index %s on %s.%s", name, table, column)
            conn.execute(text(f"DROP INDEX IF EXISTS {name}"))

    # SQLite implements unique constraints via indexes as well; get_indexes usually
    # catches them, but try to drop any auto-generated names too.
    for idx_name in ("ix_channel_youtube_channel_id", "uq_channel_youtube_channel_id"):
        conn.execute(text(f"DROP INDEX IF EXISTS {idx_name}"))


def upgrade_database(conn) -> None:
    """Run lightweight schema migrations for SQLite.

    Called synchronously via ``await engine.begin().run_sync(upgrade_database)``.
    """
    if not _table_exists(conn, "oauth_state"):
        # First run: SQLModel will create the schema afterwards.
        return

    if not _table_exists(conn, "proxy"):
        conn.execute(text("""
            CREATE TABLE proxy (
                id CHAR(36) PRIMARY KEY,
                url_enc VARCHAR NOT NULL,
                is_active BOOLEAN DEFAULT 1, last_checked_at DATETIME,
                last_check_ok BOOLEAN, status VARCHAR(20) DEFAULT 'unknown',
                failure_count INTEGER DEFAULT 0,
                created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL
            )
        """))
        conn.execute(text("CREATE INDEX ix_proxy_is_active ON proxy(is_active)"))

    if _table_exists(conn, "proxy") and not _column_exists(conn, "proxy", "status"):
        conn.execute(text("ALTER TABLE proxy ADD COLUMN status VARCHAR(20) DEFAULT 'unknown'"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_proxy_status ON proxy(status)"))
        conn.execute(text(
            "UPDATE proxy SET status = CASE "
            "WHEN last_check_ok = 1 THEN 'working' "
            "WHEN last_check_ok = 0 THEN 'failed' ELSE 'unknown' END"
        ))

    # Proxy records are identified exclusively by UUID. Remove the retired
    # display-name column from databases created by earlier releases.
    if _table_exists(conn, "proxy") and _column_exists(conn, "proxy", "name"):
        conn.execute(text("ALTER TABLE proxy DROP COLUMN name"))

    if not _column_exists(conn, "oauth_state", "proxy_id"):
        conn.execute(text("ALTER TABLE oauth_state ADD COLUMN proxy_id CHAR(36)"))
    if not _column_exists(conn, "oauth_state", "proxy_auto_switch"):
        conn.execute(text("ALTER TABLE oauth_state ADD COLUMN proxy_auto_switch BOOLEAN DEFAULT 0"))

    # --- OAuthState: optional channel_id for re-authorization flows ---
    if not _column_exists(conn, "oauth_state", "channel_id"):
        conn.execute(text("ALTER TABLE oauth_state ADD COLUMN channel_id CHAR(36)"))

    if not _column_exists(conn, "oauth_state", "folder_from_channel_id"):
        conn.execute(text("ALTER TABLE oauth_state ADD COLUMN folder_from_channel_id CHAR(36)"))

    # --- Channel soft-delete / session status / metadata ---
    if _table_exists(conn, "channel"):
        channel_columns = {
            "session_status": "VARCHAR(20) DEFAULT 'active'",
            "session_status_changed_at": "DATETIME",
            "youtube_title": "VARCHAR(255)",
            "handle": "VARCHAR(255)",
            "thumbnail_url": "VARCHAR(1024)",
            "subscriber_count": "INTEGER",
            "view_count": "INTEGER",
            "metadata_updated_at": "DATETIME",
            "pending_project_id": "CHAR(36)",
            "proxy_id": "CHAR(36)",
            "proxy_auto_switch": "BOOLEAN DEFAULT 0",
            "proxy_recovery_pending": "BOOLEAN DEFAULT 0",
            "ai_model": "VARCHAR(64)",
            "ai_generate_description": "BOOLEAN DEFAULT 0",
            "ai_use_cached": "BOOLEAN DEFAULT 0",
        }
        for column, ddl in channel_columns.items():
            if not _column_exists(conn, "channel", column):
                conn.execute(text(f"ALTER TABLE channel ADD COLUMN {column} {ddl}"))
                logger.info("Added column %s to channel", column)

        # Remove old unconditional unique constraint on youtube_channel_id so that
        # deleted channels do not block reconnecting the same YouTube channel.
        _drop_unique_indexes_on_column(conn, "channel", "youtube_channel_id")

        # Add a partial unique index: one active/paused/error record per YouTube ID.
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_youtube_id_active "
                "ON channel(youtube_channel_id) WHERE status != 'deleted'"
            )
        )

        # Ensure existing rows have a session_status value.
        conn.execute(
            text(
                "UPDATE channel SET session_status = 'active' "
                "WHERE session_status IS NULL OR session_status = ''"
            )
        )

        # Configurable upload rate limits.
        limit_columns = {
            "upload_limit_enabled": "BOOLEAN DEFAULT 0",
            "upload_limit_count": "INTEGER DEFAULT 50",
            "upload_limit_period_hours": "INTEGER DEFAULT 24",
            "pause_reason": "VARCHAR(64)",
            "max_batch_size": "INTEGER DEFAULT 200",
            "min_batch_size": "INTEGER DEFAULT 1",
            "inter_upload_delay_seconds": "INTEGER DEFAULT 2",
            "shuffle_upload_order": "BOOLEAN DEFAULT 0",
            "skip_blocked_videos": "BOOLEAN DEFAULT 0",
        }
        for column, ddl in limit_columns.items():
            if not _column_exists(conn, "channel", column):
                conn.execute(text(f"ALTER TABLE channel ADD COLUMN {column} {ddl}"))
                logger.info("Added column %s to channel", column)

        # Timestamp tracking for channels.
        if not _column_exists(conn, "channel", "created_at"):
            conn.execute(
                text(
                    "ALTER TABLE channel ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
                )
            )
            logger.info("Added column created_at to channel")
        if _column_exists(conn, "channel", "updated_at"):
            conn.execute(
                text(
                    "UPDATE channel SET created_at = updated_at "
                    "WHERE created_at IS NULL OR created_at = ''"
                )
            )

    if _table_exists(conn, "upload_task"):
        if not _column_exists(conn, "upload_task", "completed_at"):
            conn.execute(text("ALTER TABLE upload_task ADD COLUMN completed_at DATETIME"))
            logger.info("Added column completed_at to upload_task")

        # Backfill completed_at for tasks that were completed before the column
        # existed so that configurable upload limits start from accurate data.
        conn.execute(
            text(
                "UPDATE upload_task SET completed_at = updated_at "
                "WHERE status = 'completed' AND completed_at IS NULL"
            )
        )

    if _table_exists(conn, "channel"):
        # Normalize stored paths to be relative to base_dir so the database is
        # portable across environments.
        if _column_exists(conn, "channel", "folder_path"):
            rows = conn.execute(
                text(
                    "SELECT id, folder_path FROM channel "
                    "WHERE folder_path IS NOT NULL AND folder_path != ''"
                )
            ).mappings()
            for row in rows:
                rel = _relative_or_original(row["folder_path"])
                if rel != row["folder_path"]:
                    conn.execute(
                        text("UPDATE channel SET folder_path = :path WHERE id = :id"),
                        {"path": rel, "id": row["id"]},
                    )
                    logger.info(
                        "Migrated channel %s folder_path to relative: %s", row["id"], rel
                    )

        if _table_exists(conn, "oauth_state") and _column_exists(conn, "oauth_state", "folder_path"):
            rows = conn.execute(
                text(
                    "SELECT state, folder_path FROM oauth_state "
                    "WHERE folder_path IS NOT NULL AND folder_path != ''"
                )
            ).mappings()
            for row in rows:
                rel = _relative_or_original(row["folder_path"])
                if rel != row["folder_path"]:
                    conn.execute(
                        text("UPDATE oauth_state SET folder_path = :path WHERE state = :state"),
                        {"path": rel, "state": row["state"]},
                    )

        if _table_exists(conn, "upload_task") and _column_exists(conn, "upload_task", "file_path"):
            rows = conn.execute(
                text(
                    "SELECT id, file_path FROM upload_task "
                    "WHERE file_path IS NOT NULL AND file_path != ''"
                )
            ).mappings()
            for row in rows:
                rel = _relative_or_original(row["file_path"])
                if rel != row["file_path"]:
                    conn.execute(
                        text("UPDATE upload_task SET file_path = :path WHERE id = :id"),
                        {"path": rel, "id": row["id"]},
                    )

        if _table_exists(conn, "uploaded_video_hash") and _column_exists(conn, "uploaded_video_hash", "file_path"):
            rows = conn.execute(
                text(
                    "SELECT id, file_path FROM uploaded_video_hash "
                    "WHERE file_path IS NOT NULL AND file_path != ''"
                )
            ).mappings()
            for row in rows:
                rel = _relative_or_original(row["file_path"])
                if rel != row["file_path"]:
                    conn.execute(
                        text("UPDATE uploaded_video_hash SET file_path = :path WHERE id = :id"),
                        {"path": rel, "id": row["id"]},
                    )

        # Normalize Windows-style backslashes to forward slashes so paths stay
        # portable between Windows and Unix deployments.
        for table, column in (
            ("channel", "folder_path"),
            ("oauth_state", "folder_path"),
            ("upload_task", "file_path"),
            ("uploaded_video_hash", "file_path"),
        ):
            if _table_exists(conn, table) and _column_exists(conn, table, column):
                conn.execute(
                    text(
                        f"UPDATE {table} SET {column} = REPLACE({column}, '\\', '/') "
                        f"WHERE {column} LIKE '%\\%'"
                    )
                )

    # --- ChannelProjectAuth: per-project credentials ---
    if not _table_exists(conn, "channel_project_auth"):
        conn.execute(
            text(
                """
                CREATE TABLE channel_project_auth (
                    id CHAR(36) PRIMARY KEY,
                    channel_id CHAR(36) NOT NULL,
                    project_id CHAR(36) NOT NULL,
                    refresh_token_enc VARCHAR NOT NULL,
                    access_token_enc VARCHAR NOT NULL,
                    access_token_expires_at DATETIME NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE(channel_id, project_id)
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_channel_project_auth_channel_id "
                "ON channel_project_auth(channel_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_channel_project_auth_project_id "
                "ON channel_project_auth(project_id)"
            )
        )
        logger.info("Created table channel_project_auth")

        # Migrate existing tokens from channel table.
        conn.execute(
            text(
                """
                INSERT INTO channel_project_auth (
                    id, channel_id, project_id, refresh_token_enc,
                    access_token_enc, access_token_expires_at, created_at, updated_at
                )
                SELECT
                    lower(hex(randomblob(16))),
                    id,
                    project_id,
                    refresh_token_enc,
                    access_token_enc,
                    access_token_expires_at,
                    created_at,
                    created_at
                FROM channel
                WHERE project_id IS NOT NULL
                """
            )
        )
        logger.info("Migrated existing channel tokens into channel_project_auth")

    # --- RejectedVideoHash: global list of permanently invalid files ---
    if not _table_exists(conn, "rejected_video_hash"):
        conn.execute(
            text(
                """
                CREATE TABLE rejected_video_hash (
                    id CHAR(36) PRIMARY KEY,
                    file_hash VARCHAR NOT NULL UNIQUE,
                    reason VARCHAR NOT NULL,
                    created_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_rejected_video_hash_file_hash "
                "ON rejected_video_hash(file_hash)"
            )
        )
        logger.info("Created table rejected_video_hash")

    # --- BlockedVideoHash: copyright-claimed videos unavailable worldwide ---
    if not _table_exists(conn, "blocked_video_hash"):
        conn.execute(
            text(
                """
                CREATE TABLE blocked_video_hash (
                    id CHAR(36) PRIMARY KEY,
                    file_hash VARCHAR NOT NULL UNIQUE,
                    youtube_video_id VARCHAR,
                    reason VARCHAR NOT NULL,
                    blocked_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_blocked_video_hash_file_hash "
                "ON blocked_video_hash(file_hash)"
            )
        )
        logger.info("Created table blocked_video_hash")

    # --- AIModelConfig: per-API-type settings (models) ---
    if not _table_exists(conn, "ai_model_config"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_model_config (
                    id CHAR(36) PRIMARY KEY,
                    api_type VARCHAR(30) NOT NULL,
                    models_json VARCHAR NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX uq_ai_model_config_api_type "
                "ON ai_model_config(api_type)"
            )
        )
        logger.info("Created table ai_model_config")

    # --- AIModelApiKey: per-key rows with live health status ---
    if not _table_exists(conn, "ai_model_api_key"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_model_api_key (
                    id CHAR(36) PRIMARY KEY,
                    config_id CHAR(36) NOT NULL,
                    key_enc VARCHAR NOT NULL,
                    status VARCHAR(30) DEFAULT 'unknown',
                    last_checked_at DATETIME,
                    last_error VARCHAR,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_ai_model_api_key_config_id "
                "ON ai_model_api_key(config_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_ai_model_api_key_status "
                "ON ai_model_api_key(status)"
            )
        )
        logger.info("Created table ai_model_api_key")

        # Earlier releases kept one encrypted JSON blob of keys on the config
        # row. Move those keys into per-key rows (status starts as unknown) and
        # drop the retired column.
        if _column_exists(conn, "ai_model_config", "api_keys_enc"):
            rows = conn.execute(
                text("SELECT id, api_keys_enc FROM ai_model_config")
            ).mappings()
            for row in rows:
                try:
                    keys = json.loads(decrypt(row["api_keys_enc"]))
                except Exception:
                    keys = []
                for key in keys:
                    if not isinstance(key, str) or not key.strip():
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO ai_model_api_key "
                            "(id, config_id, key_enc, status, created_at, updated_at) "
                            "VALUES (lower(hex(randomblob(16))), :config_id, :key_enc, "
                            "'unknown', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                        ),
                        {"config_id": row["id"], "key_enc": encrypt(key)},
                    )
            conn.execute(text("ALTER TABLE ai_model_config DROP COLUMN api_keys_enc"))
            logger.info("Migrated ai_model_config keys into ai_model_api_key rows")

    # --- AIPrompt: saved title/description generation prompts ---
    if not _table_exists(conn, "ai_prompt"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_prompt (
                    prompt_type VARCHAR(30) PRIMARY KEY,
                    content VARCHAR NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        logger.info("Created table ai_prompt")

    # --- AIGenerationCache: generated metadata cached per video hash ---
    if not _table_exists(conn, "ai_generation_cache"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_generation_cache (
                    id CHAR(36) PRIMARY KEY,
                    file_hash VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description VARCHAR NOT NULL DEFAULT '',
                    created_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_ai_generation_cache_file_hash "
                "ON ai_generation_cache(file_hash)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX uq_ai_generation_cache_entry "
                "ON ai_generation_cache(file_hash, title, description)"
            )
        )
        logger.info("Created table ai_generation_cache")

    # --- AI custom providers (OpenAI-compatible, custom base URL) ---
    if not _table_exists(conn, "ai_custom_provider"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_custom_provider (
                    id CHAR(36) PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    base_url VARCHAR NOT NULL,
                    models_json VARCHAR NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX uq_ai_custom_provider_name "
                "ON ai_custom_provider(name)"
            )
        )
        logger.info("Created table ai_custom_provider")

    if not _table_exists(conn, "ai_custom_provider_api_key"):
        conn.execute(
            text(
                """
                CREATE TABLE ai_custom_provider_api_key (
                    id CHAR(36) PRIMARY KEY,
                    provider_id CHAR(36) NOT NULL,
                    key_enc VARCHAR NOT NULL,
                    status VARCHAR(30) DEFAULT 'unknown',
                    last_checked_at DATETIME,
                    last_error VARCHAR,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_ai_custom_provider_api_key_provider_id "
                "ON ai_custom_provider_api_key(provider_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_ai_custom_provider_api_key_status "
                "ON ai_custom_provider_api_key(status)"
            )
        )
        logger.info("Created table ai_custom_provider_api_key")

    # --- AI types: exactly one ai_model_config row per api_type ---
    if _table_exists(conn, "ai_model_config"):
        # Map every config id to its api_type BEFORE any merge deletes rows,
        # so channel references to merged-away configs still resolve to the
        # (shared) api_type instead of being cleared.
        config_api_type_by_id = {
            row["id"]: row["api_type"]
            for row in conn.execute(
                text("SELECT id, api_type FROM ai_model_config")
            ).mappings()
        }
        if _column_exists(conn, "ai_model_config", "name"):
            # Earlier releases allowed several named configurations of the
            # same api_type. Merge every group into its oldest row: models
            # are united in order, key rows are moved over (duplicate
            # plaintext keys are dropped), the merged-away rows are deleted
            # and the retired name column is removed.
            rows = conn.execute(
                text(
                    "SELECT id, api_type, models_json FROM ai_model_config "
                    "ORDER BY created_at, id"
                )
            ).mappings()
            grouped: dict[str, list] = {}
            for row in rows:
                grouped.setdefault(row["api_type"], []).append(row)
            for api_type, group in grouped.items():
                if len(group) == 1:
                    continue
                keep = group[0]
                merged_models: list[str] = []
                for row in group:
                    try:
                        models = json.loads(row["models_json"])
                    except (json.JSONDecodeError, TypeError):
                        models = []
                    for model in models:
                        if model not in merged_models:
                            merged_models.append(model)
                conn.execute(
                    text(
                        "UPDATE ai_model_config SET models_json = :models "
                        "WHERE id = :id"
                    ),
                    {"models": json.dumps(merged_models), "id": keep["id"]},
                )
                seen_keys: set[str] = set()
                for (key_enc,) in conn.execute(
                    text("SELECT key_enc FROM ai_model_api_key WHERE config_id = :id"),
                    {"id": keep["id"]},
                ).fetchall():
                    try:
                        seen_keys.add(decrypt(key_enc))
                    except Exception:
                        continue
                for row in group[1:]:
                    for key_id, key_enc in conn.execute(
                        text(
                            "SELECT id, key_enc FROM ai_model_api_key "
                            "WHERE config_id = :id"
                        ),
                        {"id": row["id"]},
                    ).fetchall():
                        try:
                            plain = decrypt(key_enc)
                        except Exception:
                            plain = None
                        if plain is not None and plain in seen_keys:
                            conn.execute(
                                text("DELETE FROM ai_model_api_key WHERE id = :id"),
                                {"id": key_id},
                            )
                        else:
                            if plain is not None:
                                seen_keys.add(plain)
                            conn.execute(
                                text(
                                    "UPDATE ai_model_api_key SET config_id = :config_id "
                                    "WHERE id = :id"
                                ),
                                {"config_id": keep["id"], "id": key_id},
                            )
                    conn.execute(
                        text("DELETE FROM ai_model_config WHERE id = :id"),
                        {"id": row["id"]},
                    )
            conn.execute(text("ALTER TABLE ai_model_config DROP COLUMN name"))
            logger.info("Merged AI model configurations into per-type rows")

        # api_type is now the identity of a row: enforce uniqueness.
        conn.execute(text("DROP INDEX IF EXISTS ix_ai_model_config_api_type"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_model_config_api_type "
                "ON ai_model_config(api_type)"
            )
        )

        # Channels used to reference a configuration by its UUID. Remap
        # those values to the api_type of that configuration — including
        # merged-away rows, which shared the api_type with the survivor.
        # Unknown ids are cleared (generation then falls back to the
        # channel templates).
        if _table_exists(conn, "channel") and _column_exists(conn, "channel", "ai_model"):
            rows = conn.execute(
                text(
                    "SELECT id, ai_model FROM channel "
                    "WHERE ai_model IS NOT NULL AND ai_model != 'auto'"
                )
            ).mappings()
            for row in rows:
                value = row["ai_model"]
                try:
                    UUID(value)
                except (ValueError, AttributeError, TypeError):
                    continue  # already an api_type name
                conn.execute(
                    text("UPDATE channel SET ai_model = :ai_model WHERE id = :id"),
                    {
                        "ai_model": config_api_type_by_id.get(value),
                        "id": row["id"],
                    },
                )
