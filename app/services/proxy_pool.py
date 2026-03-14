from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.models import ProxyEndpoint, ProxyKind


LEASE_MINUTES = 10


def active_proxy_count(session: Session, kind: ProxyKind = ProxyKind.BROWSER) -> int:
    return session.scalar(
        select(func.count())
        .select_from(ProxyEndpoint)
        .where(ProxyEndpoint.kind == kind, ProxyEndpoint.is_active.is_(True))
    ) or 0


def list_proxies(session: Session) -> list[ProxyEndpoint]:
    return session.scalars(select(ProxyEndpoint).order_by(ProxyEndpoint.kind, ProxyEndpoint.label)).all()


def acquire_proxy(
    session: Session,
    *,
    owner: str,
    kind: ProxyKind = ProxyKind.BROWSER,
) -> ProxyEndpoint | None:
    now = datetime.now(timezone.utc)
    candidate = session.scalar(
        select(ProxyEndpoint)
        .where(
            ProxyEndpoint.kind == kind,
            ProxyEndpoint.is_active.is_(True),
            or_(
                ProxyEndpoint.lease_expires_at.is_(None),
                ProxyEndpoint.lease_expires_at < now,
            ),
        )
        .order_by(ProxyEndpoint.last_used_at.is_not(None), ProxyEndpoint.last_used_at.asc(), ProxyEndpoint.id.asc())
        .limit(1)
    )
    if candidate is None:
        return None
    candidate.leased_by = owner
    candidate.leased_at = now
    candidate.lease_expires_at = now + timedelta(minutes=LEASE_MINUTES)
    candidate.last_used_at = now
    session.add(candidate)
    session.flush()
    return candidate


def release_proxy(
    session: Session,
    proxy_id: int | None,
    *,
    failed: bool = False,
) -> None:
    if proxy_id is None:
        return
    proxy = session.get(ProxyEndpoint, proxy_id)
    if proxy is None:
        return
    proxy.leased_by = None
    proxy.leased_at = None
    proxy.lease_expires_at = None
    proxy.last_used_at = datetime.now(timezone.utc)
    if failed:
        proxy.failure_count += 1
    session.add(proxy)


def upsert_proxy(
    session: Session,
    *,
    label: str,
    proxy_url: str,
    kind: ProxyKind,
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
            is_active=is_active,
            notes=notes,
        )
    else:
        existing.label = label
        existing.proxy_url = proxy_url
        existing.kind = kind
        existing.is_active = is_active
        existing.notes = notes
    session.add(existing)
    session.flush()
    return existing

