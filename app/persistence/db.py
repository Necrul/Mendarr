from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings


def _default_sqlite_url(data_dir: Path) -> str:
    db_path = data_dir / "mendarr.db"
    # SQLAlchemy requires forward slashes for SQLite URL on Windows
    return f"sqlite+aiosqlite:///{db_path.as_posix()}"


def get_database_url() -> str:
    s = get_settings()
    url = s.database_url
    if url.startswith("sqlite+aiosqlite:///./data/") or url == "sqlite+aiosqlite:///./data/mendarr.db":
        return _default_sqlite_url(s.data_dir)
    return url


engine = create_async_engine(
    get_database_url(),
    echo=False,
    pool_pre_ping=True,
    connect_args={"timeout": 30},
)


if get_database_url().startswith("sqlite+aiosqlite:///"):
    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    from app.persistence.models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
