"""
Proxy pool singleton for Celery worker processes.

Initialised once per worker process — not per task — so the rotation
state (round-robin index, daily counters) persists across tasks in the
same worker process.
"""

from __future__ import annotations

from app.validators.proxy import ProxyPool, pool_from_env

_pool: ProxyPool | None = None


def get_pool() -> ProxyPool:
    """Return the process-level proxy pool, initialising it on first call."""
    global _pool
    if _pool is None:
        _pool = pool_from_env()
    return _pool


def reset_pool() -> None:
    """Reset the pool — used in tests to inject a mock pool."""
    global _pool
    _pool = None
