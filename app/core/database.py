from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,          # Set True to log all SQL queries (useful in dev)
    # Supabase transaction pooler (pgbouncer) is not compatible with asyncpg
    # prepared statements unless statement cache is disabled.
    connect_args={"statement_cache_size": 0},
    pool_size=10,
    max_overflow=20,
    # NOTE: pool_pre_ping can trigger MissingGreenlet with asyncpg on some setups.
    # Prefer app-level retries/reconnects instead.
    pool_pre_ping=False,
    pool_timeout=30,
    pool_recycle=1800,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """FastAPI dependency: yields an async DB session per request."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            # Rollback should never mask the original exception (e.g. if DB is down).
            try:
                await session.rollback()
            except Exception:
                pass
            raise