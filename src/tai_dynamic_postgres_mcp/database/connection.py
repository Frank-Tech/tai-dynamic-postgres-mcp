import logging
from contextlib import asynccontextmanager

from async_lru import alru_cache
from psycopg import AsyncConnection, AsyncCursor
from psycopg_pool import AsyncConnectionPool

from tai_dynamic_postgres_mcp.config.settings import pg_settings

logger = logging.getLogger(__name__)

dsn = (
    f"host={pg_settings.host} "
    f"port={pg_settings.port} "
    f"dbname={pg_settings.db} "
    f"user={pg_settings.user} "
    f"password={pg_settings.password}"
)


@alru_cache(maxsize=1)
async def get_connection_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=pg_settings.pool_min_size,
        max_size=pg_settings.pool_max_size,
        timeout=pg_settings.pool_timeout,
        max_lifetime=pg_settings.pool_max_lifetime,
        open=False
    )
    await pool.open()
    return pool


async def close_connection_pool():
    pool = await get_connection_pool()
    await pool.close()


# Wait until the pool is ready
async def wait_for_pool():
    try:
        async_connection_pool = await get_connection_pool()
        await async_connection_pool.wait()
    except Exception as e:
        logger.critical(e)
        raise


@asynccontextmanager
async def get_async_connection() -> AsyncConnection:
    """Async context manager to get a pooled DB connection."""
    pool: AsyncConnectionPool | None = None
    conn: AsyncConnection | None = None

    try:
        pool = await get_connection_pool()
        conn = await pool.getconn()
        yield conn
    except Exception as e:
        logger.error(e)
        raise
    finally:
        if conn:
            await pool.putconn(conn)


@asynccontextmanager
async def cursor() -> AsyncCursor:
    async with get_async_connection() as conn:
        async with conn.cursor() as cur:
            yield cur
