from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.db import Base, SessionLocal, engine
from app.routers.api import router as api_router
from app.routers.ui import router as ui_router
from app.seed import seed_defaults
from app.services.company_dedupe import reconcile_duplicate_companies
from app.services.region_catalog import sync_region_catalog
from app.services.runtime_schema import (
    ensure_contact_channel_schema,
    ensure_phone_schema,
    ensure_proxy_pool_schema,
    ensure_recipe_schema,
    ensure_request_metric_schema,
    ensure_run_company_retry_schema,
    ensure_scrape_run_control_columns,
)
from app.services.run_companies import reconcile_active_runs, reconcile_terminal_runs


settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    Base.metadata.create_all(bind=engine)
    ensure_scrape_run_control_columns(engine)
    ensure_proxy_pool_schema(engine)
    ensure_contact_channel_schema(engine)
    ensure_phone_schema(engine)
    ensure_request_metric_schema(engine)
    ensure_run_company_retry_schema(engine)
    ensure_recipe_schema(engine)
    with SessionLocal() as session:
        seed_defaults(session)
        sync_region_catalog(session)
        reconcile_terminal_runs(session)
        reconcile_active_runs(session)
        reconcile_duplicate_companies(session)
        session.commit()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(ui_router)
app.include_router(api_router)
