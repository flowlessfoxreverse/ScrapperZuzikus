"""
Email Validator Service — FastAPI application entry point.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG", "false").lower() == "true" else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create DB tables
    try:
        from app.db.models import Base, engine
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("DB tables ready")
    except Exception as exc:
        logger.warning("DB table creation failed: %s", exc)

    # 2. Load disposable blocklist
    try:
        from app.validators.disposable import reload_disposable_list
        count = reload_disposable_list()
        logger.info("Disposable blocklist: %d domains", count)
    except Exception as exc:
        logger.warning("Disposable list reload failed, using seed: %s", exc)

    # 3. Init proxy pool
    try:
        from app.workers.proxy_pool import get_pool
        pool = get_pool()
        logger.info("Proxy pool ready: %d proxies", pool.size)
    except Exception as exc:
        logger.warning("Proxy pool init failed: %s", exc)

    yield

    try:
        from app.db.models import engine
        await engine.dispose()
    except Exception:
        pass


app = FastAPI(
    title="Email Validator API",
    description=(
        "Layered email validation service. "
        "POST /validate for fast sync check (no SMTP). "
        "POST /validate/full for full SMTP validation."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next) -> Response:
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.monotonic()
    response = await call_next(request)
    ms = int((time.monotonic() - start) * 1000)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time"] = f"{ms}ms"
    return response


from app.api.routes import router  # noqa: E402
app.include_router(router)
