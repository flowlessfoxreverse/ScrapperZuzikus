from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import RunCompany, RunCompanyStatus, RunStatus, ScrapeRun


TERMINAL_RUN_COMPANY_STATUSES = (
    RunCompanyStatus.COMPLETED,
    RunCompanyStatus.FAILED,
    RunCompanyStatus.SKIPPED,
)
TERMINAL_RUN_STATUSES = (
    RunStatus.COMPLETED,
    RunStatus.FAILED,
    RunStatus.SKIPPED,
)
STALE_ACTIVE_RUN_MINUTES = 10


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_or_create_run_company(session: Session, run_id: int, company_id: int) -> RunCompany:
    row = (
        session.query(RunCompany)
        .filter(RunCompany.run_id == run_id, RunCompany.company_id == company_id)
        .one_or_none()
    )
    if row is None:
        row = RunCompany(run_id=run_id, company_id=company_id)
        session.add(row)
        session.flush()
    return row


def queue_company_for_run(session: Session, run_id: int, company_id: int) -> bool:
    row = (
        session.query(RunCompany)
        .filter(RunCompany.run_id == run_id, RunCompany.company_id == company_id)
        .one_or_none()
    )
    if row is None:
        row = RunCompany(run_id=run_id, company_id=company_id, status=RunCompanyStatus.QUEUED)
        session.add(row)
        session.flush()
        return True

    if row.status in {RunCompanyStatus.QUEUED, RunCompanyStatus.RUNNING, RunCompanyStatus.COMPLETED}:
        return False
    row.status = RunCompanyStatus.QUEUED
    row.retry_count = 0
    row.started_at = None
    row.finished_at = None
    row.last_error = None
    session.add(row)
    session.flush()
    return True


def requeue_run_company(session: Session, run_id: int, company_id: int, last_error: str | None = None) -> None:
    row = get_or_create_run_company(session, run_id, company_id)
    row.status = RunCompanyStatus.QUEUED
    row.started_at = None
    row.finished_at = None
    row.last_error = last_error[:2000] if last_error else None
    session.add(row)
    session.flush()


def increment_retry_count(session: Session, run_id: int, company_id: int) -> int:
    row = get_or_create_run_company(session, run_id, company_id)
    row.retry_count = (row.retry_count or 0) + 1
    session.add(row)
    session.flush()
    return row.retry_count


def current_retry_count(session: Session, run_id: int, company_id: int) -> int:
    row = get_or_create_run_company(session, run_id, company_id)
    return int(row.retry_count or 0)


def mark_run_company_running(session: Session, run_id: int, company_id: int) -> RunCompany:
    row = get_or_create_run_company(session, run_id, company_id)
    row.status = RunCompanyStatus.RUNNING
    row.started_at = utcnow()
    row.finished_at = None
    row.last_error = None
    session.add(row)
    session.flush()
    return row


def mark_run_company_finished(
    session: Session,
    run_id: int,
    company_id: int,
    status: RunCompanyStatus,
    last_error: str | None = None,
) -> None:
    row = get_or_create_run_company(session, run_id, company_id)
    row.status = status
    row.finished_at = utcnow()
    row.last_error = last_error[:2000] if last_error else None
    session.add(row)
    session.flush()


def close_open_run_companies(
    session: Session,
    run_id: int,
    status: RunCompanyStatus,
    last_error: str | None = None,
) -> int:
    rows = session.scalars(
        select(RunCompany).where(
            RunCompany.run_id == run_id,
            RunCompany.status.not_in(TERMINAL_RUN_COMPANY_STATUSES),
        )
    ).all()
    closed_at = utcnow()
    for row in rows:
        row.status = status
        row.finished_at = closed_at
        row.last_error = last_error[:2000] if last_error else None
        session.add(row)
    session.flush()
    return len(rows)


def reconcile_terminal_runs(session: Session) -> int:
    runs = session.scalars(
        select(ScrapeRun).where(ScrapeRun.status.in_(TERMINAL_RUN_STATUSES))
    ).all()
    cleaned = 0
    for run in runs:
        if run.status == RunStatus.COMPLETED:
            fallback_status = RunCompanyStatus.SKIPPED
            fallback_error = "Closed automatically because the run was already completed."
        else:
            fallback_status = RunCompanyStatus.FAILED
            fallback_error = run.note or "Closed automatically because the run was already terminal."
        cleaned += close_open_run_companies(
            session,
            run.id,
            fallback_status,
            fallback_error,
        )
    session.flush()
    return cleaned


def reconcile_active_runs(session: Session) -> int:
    runs = session.scalars(
        select(ScrapeRun).where(ScrapeRun.status.not_in(TERMINAL_RUN_STATUSES))
    ).all()
    reconciled = 0
    cutoff = utcnow() - timedelta(minutes=STALE_ACTIVE_RUN_MINUTES)

    for run in runs:
        total = session.scalar(select(func.count()).select_from(RunCompany).where(RunCompany.run_id == run.id)) or 0
        queued = session.scalar(
            select(func.count()).select_from(RunCompany).where(
                RunCompany.run_id == run.id,
                RunCompany.status == RunCompanyStatus.QUEUED,
            )
        ) or 0
        running = session.scalar(
            select(func.count()).select_from(RunCompany).where(
                RunCompany.run_id == run.id,
                RunCompany.status == RunCompanyStatus.RUNNING,
            )
        ) or 0
        terminal = session.scalar(
            select(func.count()).select_from(RunCompany).where(
                RunCompany.run_id == run.id,
                RunCompany.status.in_(TERMINAL_RUN_COMPANY_STATUSES),
            )
        ) or 0

        if queued == 0 and running == 0 and total > 0 and terminal >= total:
            run.status = RunStatus.SKIPPED if run.cancel_requested else RunStatus.COMPLETED
            run.finished_at = run.finished_at or utcnow()
            if not run.note:
                run.note = "Discovery completed."
            session.add(run)
            reconciled += 1
            continue

        if queued == 0 and running == 0 and total == 0 and run.finished_at is not None:
            run.status = RunStatus.SKIPPED if run.cancel_requested else RunStatus.COMPLETED
            if not run.note:
                run.note = "Discovery completed."
            session.add(run)
            reconciled += 1
            continue

        if queued == 0 and running == 0 and run.started_at <= cutoff:
            run.status = RunStatus.FAILED
            run.finished_at = utcnow()
            run.note = "Closed stale run after worker crash."
            close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
            session.add(run)
            reconciled += 1

    session.flush()
    return reconciled


def maybe_complete_run(session: Session, run_id: int) -> None:
    run = session.get(ScrapeRun, run_id)
    if run is None or run.status in TERMINAL_RUN_STATUSES:
        return

    total = session.scalar(select(func.count()).select_from(RunCompany).where(RunCompany.run_id == run_id)) or 0
    completed = session.scalar(
        select(func.count()).select_from(RunCompany).where(
            RunCompany.run_id == run_id,
            RunCompany.status == RunCompanyStatus.COMPLETED,
        )
    ) or 0
    terminal = session.scalar(
        select(func.count()).select_from(RunCompany).where(
            RunCompany.run_id == run_id,
            RunCompany.status.in_(TERMINAL_RUN_COMPANY_STATUSES),
        )
    ) or 0

    run.crawled_count = completed
    if total == 0 or terminal >= total:
        run.status = RunStatus.SKIPPED if run.cancel_requested else RunStatus.COMPLETED
        run.finished_at = utcnow()
        close_open_run_companies(
            session,
            run_id,
            RunCompanyStatus.SKIPPED,
            "Closed automatically because the run was stopped." if run.cancel_requested else "Closed automatically because the run completed.",
        )
    session.add(run)
    session.flush()
    if run.status == RunStatus.COMPLETED:
        from app.services.recipe_performance import sync_variant_production_performance

        sync_variant_production_performance(session, run_id)
