from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import RunStatus, ScrapeRun


ACTIVE_RUN_STATUSES = (RunStatus.PENDING, RunStatus.RUNNING)


def find_active_run(session: Session, region_id: int) -> ScrapeRun | None:
    return session.scalars(
        select(ScrapeRun)
        .where(
            ScrapeRun.region_id == region_id,
            ScrapeRun.status.in_(ACTIVE_RUN_STATUSES),
        )
        .order_by(ScrapeRun.started_at.desc())
    ).first()
