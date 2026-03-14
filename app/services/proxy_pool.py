from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid5, NAMESPACE_URL

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import ProxyEndpoint, ProxyKind, ProxyLease


LEASE_MINUTES = 10
MAX_HEALTH_SCORE = 100
settings = get_settings()


@dataclass
class ProxyCapacity:
    active_proxy_count: int
    configured_capacity: int


def _supports(proxy: ProxyEndpoint, workload: ProxyKind) -> bool:
    return proxy.supports_browser if workload == ProxyKind.BROWSER else proxy.supports_http


def _capacity(proxy: ProxyEndpoint, workload: ProxyKind) -> int:
    return proxy.max_browser_leases if workload == ProxyKind.BROWSER else proxy.max_http_leases


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_in_cooldown(proxy: ProxyEndpoint, now: datetime) -> bool:
    cooldown_until = _ensure_utc(proxy.cooldown_until)
    return cooldown_until is not None and cooldown_until > now


def _clamp_health(value: int) -> int:
    return max(0, min(MAX_HEALTH_SCORE, value))


def _proxy_session_id(proxy: ProxyEndpoint, owner: str, workload: ProxyKind) -> str:
    seed = f"{proxy.id}:{proxy.label}:{owner}:{workload.value}"
    return uuid5(NAMESPACE_URL, seed).hex[:16]


def render_proxy_url(
    proxy: ProxyEndpoint | None,
    *,
    owner: str,
    workload: ProxyKind,
) -> str | None:
    if proxy is None:
        return None
    session_id = _proxy_session_id(proxy, owner, workload)
    return (
        proxy.proxy_url
        .replace("{session_id}", session_id)
        .replace("{lease_id}", session_id)
        .replace("{owner}", owner)
        .replace("{workload}", workload.value)
        .replace("{proxy_id}", str(proxy.id))
    )


def expire_old_leases(session: Session) -> None:
    now = datetime.now(timezone.utc)
    for lease in session.scalars(select(ProxyLease).where(ProxyLease.expires_at <= now)).all():
        session.delete(lease)
    session.flush()


def lease_counts(session: Session) -> dict[int, dict[str, int]]:
    now = datetime.now(timezone.utc)
    rows = session.execute(
        select(
            ProxyLease.proxy_id,
            ProxyLease.workload,
            func.count(ProxyLease.id),
        )
        .where(ProxyLease.expires_at > now)
        .group_by(ProxyLease.proxy_id, ProxyLease.workload)
    ).all()
    counts: dict[int, dict[str, int]] = {}
    for proxy_id, workload, count in rows:
        bucket = counts.setdefault(proxy_id, {"crawler": 0, "browser": 0})
        bucket[workload.value] = int(count or 0)
    return counts


def active_proxy_count(session: Session, workload: ProxyKind = ProxyKind.BROWSER) -> int:
    now = datetime.now(timezone.utc)
    proxies = [
        proxy
        for proxy in list_proxies(session)
        if proxy.is_active and _supports(proxy, workload) and not _is_in_cooldown(proxy, now)
    ]
    return len(proxies)


def effective_proxy_capacity(session: Session, workload: ProxyKind = ProxyKind.BROWSER) -> int:
    now = datetime.now(timezone.utc)
    proxies = [
        proxy
        for proxy in list_proxies(session)
        if proxy.is_active and _supports(proxy, workload) and not _is_in_cooldown(proxy, now)
    ]
    return sum(_capacity(proxy, workload) for proxy in proxies)


def capacity_snapshot(session: Session, workload: ProxyKind = ProxyKind.BROWSER) -> ProxyCapacity:
    return ProxyCapacity(
        active_proxy_count=active_proxy_count(session, workload),
        configured_capacity=effective_proxy_capacity(session, workload),
    )


def list_proxies(session: Session) -> list[ProxyEndpoint]:
    return session.scalars(select(ProxyEndpoint).order_by(ProxyEndpoint.label)).all()


def acquire_proxy(
    session: Session,
    *,
    owner: str,
    workload: ProxyKind = ProxyKind.BROWSER,
) -> ProxyEndpoint | None:
    now = datetime.now(timezone.utc)
    expire_old_leases(session)
    counts = lease_counts(session)
    candidates = [
        proxy
        for proxy in list_proxies(session)
        if proxy.is_active and _supports(proxy, workload) and not _is_in_cooldown(proxy, now)
    ]
    if workload == ProxyKind.BROWSER:
        candidates.sort(
            key=lambda proxy: (
                proxy.health_score * -1,
                counts.get(proxy.id, {}).get("browser", 0),
                counts.get(proxy.id, {}).get("crawler", 0),
                proxy.last_used_at is not None,
                _ensure_utc(proxy.last_used_at) or datetime.min.replace(tzinfo=timezone.utc),
                proxy.id,
            )
        )
    else:
        candidates.sort(
            key=lambda proxy: (
                1 if counts.get(proxy.id, {}).get("browser", 0) > 0 else 0,
                proxy.health_score * -1,
                counts.get(proxy.id, {}).get("crawler", 0),
                proxy.last_used_at is not None,
                _ensure_utc(proxy.last_used_at) or datetime.min.replace(tzinfo=timezone.utc),
                proxy.id,
            )
        )

    for proxy in candidates:
        current = counts.get(proxy.id, {"crawler": 0, "browser": 0})
        if current.get(workload.value, 0) >= _capacity(proxy, workload):
            continue
        lease = ProxyLease(
            proxy_id=proxy.id,
            owner=owner,
            workload=workload,
            expires_at=now + timedelta(minutes=LEASE_MINUTES),
        )
        session.add(lease)
        proxy.leased_by = owner if workload == ProxyKind.BROWSER else proxy.leased_by
        proxy.leased_at = now
        proxy.lease_expires_at = now + timedelta(minutes=LEASE_MINUTES)
        proxy.last_used_at = now
        session.add(proxy)
        session.flush()
        return proxy
    return None


def release_proxy(
    session: Session,
    proxy_id: int | None,
    *,
    owner: str | None = None,
    workload: ProxyKind | None = None,
    failed: bool = False,
    record_result: bool = True,
) -> None:
    if proxy_id is None:
        return
    expire_old_leases(session)
    stmt = select(ProxyLease).where(ProxyLease.proxy_id == proxy_id)
    if owner is not None:
        stmt = stmt.where(ProxyLease.owner == owner)
    if workload is not None:
        stmt = stmt.where(ProxyLease.workload == workload)
    leases = session.scalars(stmt).all()
    for lease in leases:
        session.delete(lease)

    proxy = session.get(ProxyEndpoint, proxy_id)
    if proxy is None:
        return
    remaining = lease_counts(session).get(proxy_id, {"crawler": 0, "browser": 0})
    if remaining["crawler"] + remaining["browser"] == 0:
        proxy.leased_by = None
        proxy.leased_at = None
        proxy.lease_expires_at = None
    elif remaining["browser"] == 0:
        proxy.leased_by = None
    now = datetime.now(timezone.utc)
    proxy.last_used_at = now
    if not record_result:
        session.add(proxy)
        return
    if failed:
        proxy.failure_count += 1
        proxy.consecutive_failures += 1
        proxy.last_failure_at = now
        proxy.cooldown_until = now + timedelta(minutes=settings.proxy_failure_cooldown_minutes)
        proxy.health_score = _clamp_health(proxy.health_score - settings.proxy_health_failure_penalty)
        if proxy.consecutive_failures >= settings.proxy_auto_disable_threshold:
            proxy.is_active = False
            proxy.auto_disabled_at = now
            proxy.leased_by = None
            proxy.leased_at = None
            proxy.lease_expires_at = None
    else:
        proxy.success_count += 1
        proxy.consecutive_failures = 0
        proxy.last_success_at = now
        proxy.health_score = _clamp_health(proxy.health_score + settings.proxy_health_success_recovery)
        if proxy.cooldown_until is not None and proxy.cooldown_until <= now:
            proxy.cooldown_until = None
    session.add(proxy)


def upsert_proxy(
    session: Session,
    *,
    label: str,
    proxy_url: str,
    kind: ProxyKind,
    supports_http: bool,
    supports_browser: bool,
    max_http_leases: int,
    max_browser_leases: int,
    is_active: bool,
    notes: str | None,
) -> ProxyEndpoint:
    existing = session.scalar(
        select(ProxyEndpoint).where(
            (ProxyEndpoint.label == label) | (ProxyEndpoint.proxy_url == proxy_url)
        )
    )
    if existing is None:
        existing = ProxyEndpoint(
            label=label,
            proxy_url=proxy_url,
            kind=kind,
            supports_http=supports_http,
            supports_browser=supports_browser,
            max_http_leases=max(1, max_http_leases),
            max_browser_leases=max(1, max_browser_leases),
            is_active=is_active,
            notes=notes,
        )
    else:
        existing.label = label
        existing.proxy_url = proxy_url
        existing.kind = kind
        existing.supports_http = supports_http
        existing.supports_browser = supports_browser
        existing.max_http_leases = max(1, max_http_leases)
        existing.max_browser_leases = max(1, max_browser_leases)
        existing.is_active = is_active
        if existing.is_active:
            existing.auto_disabled_at = None
        existing.notes = notes
    session.add(existing)
    session.flush()
    return existing
