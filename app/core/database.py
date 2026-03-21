from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from app.core.config import settings


def _uses_external_pooler(url: str) -> bool:
    """Supabase pooler / pgBouncer transaction mode breaks asyncpg + SQLAlchemy pooling."""
    u = (url or "").lower()
    return "pooler.supabase.com" in u or "pgbouncer=true" in u


# asyncpg + pgBouncer (transaction mode): must disable server-side statement cache.
# Also use NullPool so each checkout gets a clean connection through the pooler
# (avoids DuplicatePreparedStatementError when the same pooled slot hits different backends).
_connect_args = {"statement_cache_size": 0}

if _uses_external_pooler(settings.DATABASE_URL):
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        connect_args=_connect_args,
        poolclass=NullPool,
        pool_pre_ping=False,
    )
else:
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        connect_args=_connect_args,
        pool_size=10,
        max_overflow=20,
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