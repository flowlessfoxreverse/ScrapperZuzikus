from __future__ import annotations

from datetime import datetime, timezone

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal, engine
from app.models import Email, ProxyKind, RunCompanyStatus, RunStatus, ScrapeRun, ValidationStatus
from app.services.pipeline import execute_browser_crawl, execute_crawl, execute_discovery
from app.services.region_catalog import sync_region_catalog
from app.services.runtime_schema import (
    ensure_contact_channel_schema,
    ensure_phone_schema,
    ensure_proxy_pool_schema,
    ensure_recipe_schema,
    ensure_request_metric_schema,
    ensure_run_company_retry_schema,
    ensure_scrape_run_control_columns,
    ensure_source_bridge_schema,
)
from app.services.run_companies import close_open_run_companies


settings = get_settings()
ensure_scrape_run_control_columns(engine)
ensure_proxy_pool_schema(engine)
ensure_contact_channel_schema(engine)
ensure_phone_schema(engine)
ensure_request_metric_schema(engine)
ensure_run_company_retry_schema(engine)
ensure_recipe_schema(engine)
ensure_source_bridge_schema(engine)
redis_broker = RedisBroker(url=settings.redis_url)
dramatiq.set_broker(redis_broker)


@dramatiq.actor(queue_name="discovery")
def run_scrape(run_id: int, force_refresh_category_ids: list[int] | None = None) -> None:
    with SessionLocal() as session:
        try:
            execute_discovery(
                session=session,
                run_id=run_id,
                overpass_cap=settings.overpass_daily_query_cap,
                discovery_cooldown_hours=settings.discovery_cooldown_hours,
                crawl_recrawl_hours=settings.crawl_recrawl_hours,
                force_refresh_category_ids=set(force_refresh_category_ids or []),
                enqueue_crawl=lambda queued_run_id, company_id: crawl_company.send(queued_run_id, company_id),
            )
        except Exception as exc:
            session.rollback()
            run = session.get(ScrapeRun, run_id)
            if run is not None and run.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                run.status = RunStatus.FAILED
                run.finished_at = run.finished_at or datetime.now(timezone.utc)
                run.note = f"Worker crashed during discovery: {str(exc)[:300]}"
                close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
                session.add(run)
                session.commit()
            raise


@dramatiq.actor(queue_name="crawl")
def crawl_company(run_id: int, company_id: int) -> None:
    with SessionLocal() as session:
        try:
            execute_crawl(session=session, run_id=run_id, company_id=company_id)
        except Exception as exc:
            session.rollback()
            run = session.get(ScrapeRun, run_id)
            if run is not None and run.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                run.status = RunStatus.FAILED
                run.finished_at = run.finished_at or datetime.now(timezone.utc)
                run.note = f"Worker crashed during crawl: {str(exc)[:300]}"
                close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
                session.add(run)
                session.commit()
            raise


@dramatiq.actor(queue_name="browser")
def browser_crawl_company(run_id: int, company_id: int) -> None:
    with SessionLocal() as session:
        try:
            execute_browser_crawl(session=session, run_id=run_id, company_id=company_id)
        except Exception as exc:
            session.rollback()
            run = session.get(ScrapeRun, run_id)
            if run is not None and run.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                run.status = RunStatus.FAILED
                run.finished_at = run.finished_at or datetime.now(timezone.utc)
                run.note = f"Worker crashed during browser crawl: {str(exc)[:300]}"
                close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
                session.add(run)
                session.commit()
            raise


@dramatiq.actor(queue_name="retry")
def retry_company(run_id: int, company_id: int, workload: str) -> None:
    with SessionLocal() as session:
        try:
            if workload == ProxyKind.BROWSER.value:
                execute_browser_crawl(session=session, run_id=run_id, company_id=company_id)
            else:
                execute_crawl(session=session, run_id=run_id, company_id=company_id)
        except Exception as exc:
            session.rollback()
            run = session.get(ScrapeRun, run_id)
            if run is not None and run.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                run.status = RunStatus.FAILED
                run.finished_at = run.finished_at or datetime.now(timezone.utc)
                run.note = f"Worker crashed during retry crawl: {str(exc)[:300]}"
                close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
                session.add(run)
                session.commit()
            raise


@dramatiq.actor(queue_name="maintenance")
def sync_region_catalog_task() -> None:
    with SessionLocal() as session:
        sync_region_catalog(session)


@dramatiq.actor(queue_name="email_validation")
def validate_company_emails(company_id: int) -> None:
    """
    Find all emails for a company that are still UNKNOWN, mark them PENDING,
    and submit them to the emailvalidator microservice for async bulk validation.
    The emailvalidator will POST each result back to /api/email-validation-results.
    """
    settings = get_settings()
    if not settings.email_validator_url:
        return

    with SessionLocal() as session:
        emails = session.scalars(
            select(Email).where(
                Email.company_id == company_id,
                Email.validation_status == ValidationStatus.UNKNOWN,
            )
        ).all()

        if not emails:
            return

        # Mark as pending so they aren't re-submitted by concurrent tasks
        for email in emails:
            email.validation_status = ValidationStatus.PENDING
        session.commit()

        webhook_url = None
        base = settings.email_validator_webhook_base_url
        if base:
            webhook_url = base.rstrip("/") + "/api/email-validation-results"

        from app.services.email_validator_client import submit_bulk_validation
        batch_id = submit_bulk_validation(
            [e.email for e in emails],
            validator_url=settings.email_validator_url,
            webhook_url=webhook_url,
            skip_smtp=settings.email_validator_skip_smtp,
        )

        if not batch_id:
            # Submission failed — revert so they can be retried later
            for email in emails:
                email.validation_status = ValidationStatus.UNKNOWN
            session.commit()
