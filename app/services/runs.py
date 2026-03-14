from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import RunCompany, RunCompanyStatus, RunStatus, ScrapeRun
from app.services.run_companies import close_open_run_companies


ACTIVE_RUN_STATUSES = (RunStatus.PENDING, RunStatus.RUNNING)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def find_active_run(session: Session, region_id: int) -> ScrapeRun | None:
    return session.scalars(
        select(ScrapeRun)
        .where(
            ScrapeRun.region_id == region_id,
            ScrapeRun.status.in_(ACTIVE_RUN_STATUSES),
        )
        .order_by(ScrapeRun.started_at.desc())
    ).first()


def request_run_cancellation(session: Session, run_id: int, reason: str | None = None) -> ScrapeRun | None:
    run = session.get(ScrapeRun, run_id)
    if run is None or run.status not in ACTIVE_RUN_STATUSES:
        return run
    run.cancel_requested = True
    run.cancel_requested_at = utcnow()
    run.cancel_reason = reason[:500] if reason else None
    if not run.note or "Cancel requested" not in run.note:
        prefix = "Cancel requested."
        if reason:
            prefix = f"{prefix} {reason[:500]}"
        run.note = prefix[:2000]
    session.add(run)
    running_companies = session.scalar(
        select(func.count()).select_from(RunCompany).where(
            RunCompany.run_id == run_id,
            RunCompany.status == RunCompanyStatus.RUNNING,
        )
    ) or 0
    if run.status == RunStatus.PENDING or running_companies == 0:
        finalize_cancelled_run(session, run, run.note)
    session.flush()
    return run


def finalize_cancelled_run(session: Session, run: ScrapeRun, note: str | None = None) -> None:
    run.cancel_requested = True
    run.cancel_requested_at = run.cancel_requested_at or utcnow()
    message = (note or run.cancel_reason or "Run stopped by request.")[:2000]
    close_open_run_companies(session, run.id, status=RunCompanyStatus.SKIPPED, last_error=message)
    run.status = RunStatus.SKIPPED
    run.finished_at = utcnow()
    run.note = message
    session.add(run)
    session.flush()
