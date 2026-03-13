from __future__ import annotations

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from app.config import get_settings
from app.db import SessionLocal
from app.services.pipeline import execute_run


settings = get_settings()
redis_broker = RedisBroker(url=settings.redis_url)
dramatiq.set_broker(redis_broker)


@dramatiq.actor
def run_scrape(run_id: int) -> None:
    with SessionLocal() as session:
        execute_run(
            session=session,
            run_id=run_id,
            overpass_cap=settings.overpass_daily_query_cap,
            discovery_cooldown_hours=settings.discovery_cooldown_hours,
            crawl_recrawl_hours=settings.crawl_recrawl_hours,
        )
