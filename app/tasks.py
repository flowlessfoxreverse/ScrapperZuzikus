from __future__ import annotations

from datetime import datetime, timezone

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from app.config import get_settings
from app.db import SessionLocal
from app.models import RunCompanyStatus, RunStatus, ScrapeRun
from app.services.pipeline import execute_crawl, execute_discovery
from app.services.region_catalog import sync_region_catalog
from app.services.run_companies import close_open_run_companies


settings = get_settings()
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


@dramatiq.actor(queue_name="maintenance")
def sync_region_catalog_task() -> None:
    with SessionLocal() as session:
        sync_region_catalog(session)
