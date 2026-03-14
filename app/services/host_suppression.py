from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

import redis

from app.config import get_settings


settings = get_settings()


def normalize_host_key(url_or_host: str) -> str:
    candidate = (url_or_host or "").strip().lower()
    if not candidate:
        return ""
    parsed = urlparse(candidate if "://" in candidate else f"http://{candidate}")
    hostname = (parsed.hostname or parsed.path or "").lower().removeprefix("www.")
    if not hostname:
        return ""
    port = parsed.port
    if port in {80, 443, None}:
        return hostname
    return f"{hostname}:{port}"


@lru_cache(maxsize=1)
def _redis_client() -> redis.Redis | None:
    try:
        return redis.Redis.from_url(settings.redis_url, decode_responses=True)
    except Exception:
        return None


def _count_key(host_key: str) -> str:
    return f"host_suppression:{host_key}:count"


def _blocked_key(host_key: str) -> str:
    return f"host_suppression:{host_key}:blocked"


def is_host_suppressed(url_or_host: str) -> bool:
    host_key = normalize_host_key(url_or_host)
    if not host_key:
        return False
    client = _redis_client()
    if client is None:
        return False
    try:
        return bool(client.exists(_blocked_key(host_key)))
    except Exception:
        return False


def register_host_failure(url_or_host: str) -> None:
    host_key = normalize_host_key(url_or_host)
    if not host_key:
        return
    client = _redis_client()
    if client is None:
        return
    ttl_seconds = max(60, settings.host_failure_cache_ttl_minutes * 60)
    try:
        count = client.incr(_count_key(host_key))
        client.expire(_count_key(host_key), ttl_seconds)
        if count >= settings.host_failure_threshold:
            client.setex(_blocked_key(host_key), ttl_seconds, "1")
    except Exception:
        return


def suppress_host(url_or_host: str) -> None:
    host_key = normalize_host_key(url_or_host)
    if not host_key:
        return
    client = _redis_client()
    if client is None:
        return
    ttl_seconds = max(60, settings.host_failure_cache_ttl_minutes * 60)
    try:
        client.setex(_blocked_key(host_key), ttl_seconds, "1")
        client.setex(_count_key(host_key), ttl_seconds, str(settings.host_failure_threshold))
    except Exception:
        return


def clear_host_failures(url_or_host: str) -> None:
    host_key = normalize_host_key(url_or_host)
    if not host_key:
        return
    client = _redis_client()
    if client is None:
        return
    try:
        client.delete(_count_key(host_key), _blocked_key(host_key))
    except Exception:
        return
