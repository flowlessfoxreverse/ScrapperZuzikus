from __future__ import annotations

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from app.config import get_settings
from app.db import SessionLocal
from app.services.pipeline import execute_crawl, execute_discovery
from app.services.region_catalog import sync_region_catalog


settings = get_settings()
redis_broker = RedisBroker(url=settings.redis_url)
dramatiq.set_broker(redis_broker)


@dramatiq.actor(queue_name="discovery")
def run_scrape(run_id: int) -> None:
    with SessionLocal() as session:
        execute_discovery(
            session=session,
            run_id=run_id,
            overpass_cap=settings.overpass_daily_query_cap,
            discovery_cooldown_hours=settings.discovery_cooldown_hours,
            crawl_recrawl_hours=settings.crawl_recrawl_hours,
            enqueue_crawl=lambda queued_run_id, company_id: crawl_company.send(queued_run_id, company_id),
        )


@dramatiq.actor(queue_name="crawl")
def crawl_company(run_id: int, company_id: int) -> None:
    with SessionLocal() as session:
        execute_crawl(session=session, run_id=run_id, company_id=company_id)


@dramatiq.actor(queue_name="maintenance")
def sync_region_catalog_task() -> None:
    with SessionLocal() as session:
        sync_region_catalog(session)
