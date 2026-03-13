from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import RegionCategoryState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_or_create_region_category_state(session: Session, region_id: int, category_id: int) -> RegionCategoryState:
    state = (
        session.query(RegionCategoryState)
        .filter(
            RegionCategoryState.region_id == region_id,
            RegionCategoryState.category_id == category_id,
        )
        .one_or_none()
    )
    if state is None:
        state = RegionCategoryState(region_id=region_id, category_id=category_id)
        session.add(state)
        session.flush()
    return state


def should_refresh_discovery(state: RegionCategoryState, cooldown_hours: int) -> bool:
    last_success = ensure_utc(state.last_discovery_success_at)
    if last_success is None:
        return True
    if cooldown_hours <= 0:
        return True
    return last_success <= utcnow() - timedelta(hours=cooldown_hours)
