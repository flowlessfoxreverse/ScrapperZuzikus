from __future__ import annotations

from datetime import date

from sqlalchemy.orm import Session

from app.models import DailyUsage


def get_or_create_daily_usage(session: Session, provider: str, cap: int) -> DailyUsage:
    today = date.today()
    usage = (
        session.query(DailyUsage)
        .filter(DailyUsage.usage_date == today, DailyUsage.provider == provider)
        .one_or_none()
    )
    if usage is None:
        usage = DailyUsage(usage_date=today, provider=provider, cap=cap, units_used=0)
        session.add(usage)
        session.commit()
        session.refresh(usage)
    return usage


def can_consume(session: Session, provider: str, cap: int, units: int = 1) -> tuple[bool, DailyUsage]:
    usage = get_or_create_daily_usage(session, provider=provider, cap=cap)
    return (usage.units_used + units) <= usage.cap, usage


def consume_units(session: Session, provider: str, cap: int, units: int = 1) -> DailyUsage:
    usage = get_or_create_daily_usage(session, provider=provider, cap=cap)
    usage.units_used += units
    usage.cap = cap
    session.add(usage)
    session.commit()
    session.refresh(usage)
    return usage

