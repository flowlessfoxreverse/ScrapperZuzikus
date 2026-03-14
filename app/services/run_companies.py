from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import RunCompany, RunCompanyStatus, RunStatus, ScrapeRun


TERMINAL_RUN_COMPANY_STATUSES = (
    RunCompanyStatus.COMPLETED,
    RunCompanyStatus.FAILED,
    RunCompanyStatus.SKIPPED,
)


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
    row.started_at = None
    row.finished_at = None
    row.last_error = None
    session.add(row)
    session.flush()
    return True


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


def maybe_complete_run(session: Session, run_id: int) -> None:
    run = session.get(ScrapeRun, run_id)
    if run is None or run.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.SKIPPED}:
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
        run.status = RunStatus.COMPLETED
        run.finished_at = utcnow()
    session.add(run)
    session.flush()
