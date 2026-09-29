from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import Response
from sqlmodel import SQLModel

from app.api.routes import api_router
from app.core.config import settings
from app.core.db import engine
from app.core.logging import logger
from app.core.memory import current_rss_mb, freeze_after_startup
from app.core.migrations import upgrade_database
from app.services.ai_providers import close_ai_providers
from app.services.coordinator import coordinator
from app.services.http_client import close_client
from app.services.maintenance import start_maintenance, stop_maintenance
from app.services.proxy_service import start_proxy_recovery, stop_proxy_recovery


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the directory that actually holds the SQLite file exists, whether
    # the database URL is explicit or derived from base_dir/data_dir.
    db_dir: Path
    if settings.database_url and settings.database_url.startswith("sqlite+aiosqlite:///"):
        db_path = Path(settings.database_url[len("sqlite+aiosqlite:///"):])
        db_dir = db_path.parent
    else:
        db_dir = settings.base_dir / settings.data_dir

    print(f"[LIFESPAN] Database directory: {db_dir}", flush=True)
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"Could not create database directory {db_dir}: {exc}") from exc

    if not db_dir.is_dir():
        raise RuntimeError(f"Database directory does not exist: {db_dir}")

    import os
    if not os.access(db_dir, os.W_OK):
        raise RuntimeError(f"Database directory is not writable: {db_dir}")

    logger.info(f"Base directory: {settings.base_dir}")
    logger.info(f"Data directory: {settings.data_dir}")
    logger.info(f"Database directory: {db_dir}")
    logger.info(f"Database URL: {settings.database_url}")
    logger.info("Running database migrations if needed")
    async with engine.begin() as conn:
        await conn.run_sync(upgrade_database)

    logger.info("Creating database tables if needed")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    await coordinator.start()
    await start_proxy_recovery()
    start_maintenance()
    # Everything above lives for the whole process: keep the GC from rescanning
    # it and hand the import-time allocations back to the OS.
    freeze_after_startup()
    rss = current_rss_mb()
    logger.info("Server started{}", f" (RSS {rss} MB)" if rss else "")
    yield
    logger.info("Shutting down server")
    await coordinator.stop()
    await stop_proxy_recovery()
    await stop_maintenance()
    await close_ai_providers()
    await close_client()


app = FastAPI(
    title="YouTube Uploader Server",
    version="0.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log HTTP requests so client calls are visible in the console.

    Skip the long-lived SSE stream and the health check to keep the logs tidy.
    """
    method = request.method
    path = request.url.path
    if path in ("/health",) or path.startswith("/updates/sse"):
        return await call_next(request)

    logger.info("{} {} started", method, path)
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        logger.exception("{} {} failed", method, path)
        raise
    logger.info("{} {} completed {}", method, path, response.status_code)
    return response


app.include_router(api_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
