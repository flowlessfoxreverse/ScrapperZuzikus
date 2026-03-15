"""
FastAPI dependency injection.

Provides:
  - get_db()       async DB session per request
  - get_redis()    shared Redis connection
  - get_cache()    domain result cache (Redis-backed, 24h TTL)
"""

from __future__ import annotations

import os
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AsyncSessionLocal

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("RESULT_CACHE_TTL", str(24 * 3600)))  # 24h default

# Module-level Redis pool — shared across all requests
_redis: aioredis.Redis | None = None


def get_redis_client() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _redis


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session, commit on success, rollback on error."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """Yield the shared Redis client."""
    yield get_redis_client()


async def cache_get(redis: aioredis.Redis, key: str) -> str | None:
    """Get a cached value. Returns None on miss or Redis error."""
    try:
        return await redis.get(key)
    except Exception:
        return None


async def cache_set(redis: aioredis.Redis, key: str, value: str, ttl: int = CACHE_TTL) -> None:
    """Set a cached value with TTL. Silent on Redis error."""
    try:
        await redis.setex(key, ttl, value)
    except Exception:
        pass
