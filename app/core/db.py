from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import settings

# ``timeout`` (seconds) lets SQLite wait on a locked database instead of
# raising ``OperationalError: database is locked`` immediately. 30s covers the
# worst-case fsync latency of a WAL checkpoint on a slow Docker volume.
_connect_args: dict = {}
_pool_kwargs: dict = {}
if settings.database_url and settings.database_url.startswith("sqlite"):
    _connect_args = {"timeout": 30}
    # SQLite connections are cheap (a local file handle) and the server runs many
    # concurrent tasks (channel workers, proxy health checks, recovery loops,
    # HTTP requests). A bounded QueuePool is exhausted when several of them hold
    # a connection at once, raising "QueuePool limit ... reached". NullPool opens
    # a fresh connection per session and closes it on release, so there is no
    # pool to exhaust. WAL mode keeps concurrent readers/writers working.
    _pool_kwargs = {"poolclass": NullPool}

engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    connect_args=_connect_args,
    **_pool_kwargs,
)


@event.listens_for(engine.sync_engine, "connect")
def _configure_sqlite_connection(dbapi_connection, connection_record):
    """Enable WAL and bound the per-connection memory SQLite may retain.

    ``NullPool`` opens a fresh connection per session, so every unbounded
    per-connection buffer is paid many times over. SQLite defaults to a 2 MB
    page cache per connection; capping it keeps concurrent workers from holding
    tens of megabytes of cache for queries that read a handful of rows.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        # Negative values are a KiB budget rather than a page count.
        cursor.execute("PRAGMA cache_size=-2000")
        # Scratch memory for sorting/temp tables is freed back on release.
        cursor.execute("PRAGMA temp_store=FILE")
        # Checkpoint sooner so the -wal file does not grow into the tens of MB
        # that then have to be mapped by every reader.
        cursor.execute("PRAGMA wal_autocheckpoint=256")
    finally:
        cursor.close()


async_session_maker = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session
