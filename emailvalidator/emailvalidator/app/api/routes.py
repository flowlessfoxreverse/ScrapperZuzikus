"""
API Routes

POST /validate          — sync, layers 1–4 only (fast, no SMTP)
POST /validate/full     — async, full pipeline incl. SMTP
POST /bulk              — async bulk validation (up to 10,000)
GET  /result/{job_id}   — poll async job result
GET  /health            — service health check
GET  /admin/proxy-stats — proxy pool usage
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import redis.asyncio as aioredis

from app.api.deps import cache_get, cache_set, get_db, get_redis
from app.workers.tasks import celery_app
from app.workers.proxy_pool import get_pool
from app.api.schemas import (
    BulkRequest,
    BulkResponse,
    EmailStatus,
    FullValidateRequest,
    HealthResponse,
    JobResponse,
    JobStatus,
    ProxyStatEntry,
    ProxyStatsResponse,
    ResultResponse,
    ValidateRequest,
    ValidationDetail,
    ValidationResponse,
)
from app.db.models import ValidationJob, ValidationResult
from app.validators.disposable import blocklist_size
from app.validators.syntax import validate_syntax
from app.validators.domain import validate_domain
from app.validators.disposable import check_disposable
from app.validators.typo import check_typo
from app.validators.scorer import ScoreInput, score as compute_score
from app.validators.types import EmailStatus as InternalEmailStatus

router = APIRouter()

# Result cache TTL for the fast /validate endpoint (domain-level results
# are stable for 24h; we cache at the email level for 6h)
_FAST_CACHE_TTL = 6 * 3600
_FULL_CACHE_TTL = 24 * 3600


# ── POST /validate ────────────────────────────────────────────────────────────

@router.post(
    "/validate",
    response_model=ValidationResponse,
    summary="Validate a single email (fast, no SMTP)",
    description=(
        "Runs layers 1–4: syntax, domain/MX, disposable, and typo checks. "
        "Returns in ~50–100ms. Results are cached for 6 hours."
    ),
)
async def validate_email(
    body: ValidateRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> ValidationResponse:
    email = body.email.lower().strip()
    cache_key = f"v:fast:{email}"

    # Check Redis cache first
    cached = await cache_get(redis, cache_key)
    if cached:
        data = json.loads(cached)
        data["cached"] = True
        return ValidationResponse(**data)

    result = await _run_fast_pipeline(email)

    # Cache and persist
    serialized = result.model_dump_json()
    await cache_set(redis, cache_key, serialized, ttl=_FAST_CACHE_TTL)
    await _persist_result(db, result)

    return result


# ── POST /validate/full ───────────────────────────────────────────────────────

@router.post(
    "/validate/full",
    response_model=JobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Full validation with SMTP (async)",
    description=(
        "Enqueues a full validation job including SMTP probe and catch-all "
        "detection. Returns a job ID immediately. Poll /result/{job_id} for "
        "the result, typically ready in 3–15 seconds."
    ),
)
async def validate_full(
    body: FullValidateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    email = body.email.lower().strip()
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Create job record
    job = ValidationJob(
        id=job_id,
        email=email,
        status="pending",
        webhook_url=str(body.webhook_url) if body.webhook_url else None,
        queued_at=now,
    )
    db.add(job)
    await db.flush()

    celery_app.send_task(
        "app.workers.tasks.validate_email_task",
        kwargs={
            "email": email,
            "skip_smtp": False,
            "webhook_url": str(body.webhook_url) if body.webhook_url else None,
            "request_id": job_id,
        },
        task_id=job_id,
    )

    base_url = str(request.base_url).rstrip("/")
    return JobResponse(
        job_id=job_id,
        email=email,
        status=JobStatus.PENDING,
        queued_at=now,
        poll_url=f"{base_url}/result/{job_id}",
    )


# ── POST /bulk ────────────────────────────────────────────────────────────────

@router.post(
    "/bulk",
    response_model=BulkResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Bulk validation (up to 10,000 emails)",
)
async def validate_bulk(
    body: BulkRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> BulkResponse:
    batch_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    emails = list(dict.fromkeys(body.emails))

    celery_app.send_task(
        "app.workers.tasks.validate_bulk_task",
        kwargs={
            "emails": emails,
            "skip_smtp": body.skip_smtp,
            "webhook_url": str(body.webhook_url) if body.webhook_url else None,
            "batch_id": batch_id,
        },
        task_id=batch_id,
    )

    seconds_per = 0.1 if body.skip_smtp else 3
    base_url = str(request.base_url).rstrip("/")

    return BulkResponse(
        batch_id=batch_id,
        total=len(emails),
        queued_at=now,
        estimated_seconds=int(len(emails) * seconds_per),
        poll_url=f"{base_url}/result/{batch_id}",
    )


# ── GET /result/{job_id} ──────────────────────────────────────────────────────

@router.get(
    "/result/{job_id}",
    response_model=ResultResponse,
    summary="Poll async validation result",
)
async def get_result(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> ResultResponse:
    # Check Celery result backend (Redis) first — faster than DB
    task_result = celery_app.AsyncResult(job_id)

    if task_result.state == "PENDING":
        # Look up in DB for queued_at
        job = await db.get(ValidationJob, job_id)
        queued_at = job.queued_at if job else datetime.now(timezone.utc)
        return ResultResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            queued_at=queued_at,
        )

    if task_result.state == "STARTED":
        job = await db.get(ValidationJob, job_id)
        queued_at = job.queued_at if job else datetime.now(timezone.utc)
        return ResultResponse(
            job_id=job_id,
            status=JobStatus.PROCESSING,
            queued_at=queued_at,
        )

    if task_result.state == "FAILURE":
        job = await db.get(ValidationJob, job_id)
        queued_at = job.queued_at if job else datetime.now(timezone.utc)
        return ResultResponse(
            job_id=job_id,
            status=JobStatus.FAILED,
            error=str(task_result.result),
            queued_at=queued_at,
        )

    if task_result.state == "SUCCESS":
        raw = task_result.result
        job = await db.get(ValidationJob, job_id)
        queued_at = job.queued_at if job else datetime.now(timezone.utc)
        completed_at = job.completed_at if job else datetime.now(timezone.utc)

        result = ValidationResponse(
            email=raw.get("email", ""),
            normalized=raw.get("normalized"),
            status=EmailStatus(raw.get("status", "unknown")),
            score=raw.get("score", 0),
            reasons=raw.get("reasons", []),
            detail=ValidationDetail(
                syntax_valid=raw.get("syntax_valid", False),
                domain_exists=raw.get("domain_exists"),
                mx_found=raw.get("mx_found"),
                is_disposable=raw.get("is_disposable"),
                is_role_based=raw.get("is_role_based"),
                typo_suggestion=raw.get("typo_suggestion"),
                smtp_verdict=raw.get("smtp_verdict"),
                primary_mx=raw.get("primary_mx"),
            ),
            validated_at=completed_at or datetime.now(timezone.utc),
        )
        return ResultResponse(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            result=result,
            queued_at=queued_at,
            completed_at=completed_at,
        )

    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


# ── GET /health ───────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse)
async def health(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> HealthResponse:
    # Check DB
    db_status = "ok"
    try:
        await db.execute(select(1))
    except Exception as exc:
        db_status = f"error: {exc}"

    # Check Redis
    redis_status = "ok"
    try:
        await redis.ping()
    except Exception as exc:
        redis_status = f"error: {exc}"

    # Check workers (inspect active)
    workers_status = "ok"
    try:
        inspect = celery_app.control.inspect(timeout=1.0)
        active = inspect.active()
        if active is None:
            workers_status = "no workers responding"
    except Exception as exc:
        workers_status = f"error: {exc}"

    return HealthResponse(
        status="ok" if all(s == "ok" for s in [db_status, redis_status]) else "degraded",
        db=db_status,
        redis=redis_status,
        workers=workers_status,
        disposable_domains=blocklist_size(),
    )


# ── GET /admin/proxy-stats ────────────────────────────────────────────────────

@router.get("/admin/proxy-stats", response_model=ProxyStatsResponse)
async def proxy_stats() -> ProxyStatsResponse:
    pool = get_pool()
    stats = pool.stats()

    entries = [
        ProxyStatEntry(
            name=s["name"],
            uses_today=s["uses_today"],
            daily_limit=s["daily_limit"],
            utilization_pct=round(s["uses_today"] / s["daily_limit"] * 100, 1),
            is_exhausted=s["is_exhausted"],
            is_direct=s["is_direct"],
            host=s["host"],
        )
        for s in stats
    ]

    return ProxyStatsResponse(
        total_proxies=pool.size,
        available_proxies=len(pool.available),
        proxies=entries,
    )


# ── Internal pipeline helpers ─────────────────────────────────────────────────

async def _run_fast_pipeline(email: str) -> ValidationResponse:
    """Layers 1–4: syntax, domain, disposable, typo. No SMTP."""
    now = datetime.now(timezone.utc)

    # Layer 1
    syntax = validate_syntax(email)
    if not syntax.valid:
        return ValidationResponse(
            email=email,
            normalized=None,
            status=EmailStatus.INVALID,
            score=0,
            reasons=[syntax.error or "Invalid syntax"],
            detail=ValidationDetail(syntax_valid=False),
            validated_at=now,
        )

    # Layer 2
    domain_result = await validate_domain(syntax.domain)

    # Layer 3
    disp = check_disposable(syntax.local, syntax.domain)

    # Layer 4
    typo = check_typo(syntax.local, syntax.domain)

    # Layer 7 (score without SMTP)
    score_input = ScoreInput(
        syntax_valid=True,
        domain_exists=domain_result.domain_exists,
        mx_found=domain_result.mx_found,
        is_disposable=disp.is_disposable,
        is_role_based=disp.is_role_based,
        has_typo=typo.has_typo,
        smtp_verdict=None,
        normalized_email=syntax.normalized,
    )
    score_result = compute_score(score_input)

    # Map internal status to API status
    status_map = {
        InternalEmailStatus.VALID:   EmailStatus.VALID,
        InternalEmailStatus.INVALID: EmailStatus.INVALID,
        InternalEmailStatus.RISKY:   EmailStatus.RISKY,
        InternalEmailStatus.UNKNOWN: EmailStatus.UNKNOWN,
    }

    return ValidationResponse(
        email=email,
        normalized=syntax.normalized,
        status=status_map[score_result.status],
        score=score_result.score,
        reasons=score_result.reasons,
        detail=ValidationDetail(
            syntax_valid=True,
            domain_exists=domain_result.domain_exists,
            mx_found=domain_result.mx_found,
            is_disposable=disp.is_disposable,
            is_role_based=disp.is_role_based,
            typo_suggestion=typo.suggestion,
            smtp_verdict=None,
            primary_mx=domain_result.primary_mx,
        ),
        validated_at=now,
    )


async def _persist_result(db: AsyncSession, response: ValidationResponse) -> None:
    """Store a validation result in PostgreSQL. Silent on error."""
    try:
        row = ValidationResult(
            email=response.email,
            normalized=response.normalized,
            status=response.status.value,
            score=response.score,
            syntax_valid=response.detail.syntax_valid,
            domain_exists=response.detail.domain_exists,
            mx_found=response.detail.mx_found,
            is_disposable=response.detail.is_disposable,
            is_role_based=response.detail.is_role_based,
            typo_suggestion=response.detail.typo_suggestion,
            smtp_verdict=response.detail.smtp_verdict,
            primary_mx=response.detail.primary_mx,
            reasons=response.reasons,
            validated_at=response.validated_at,
        )
        db.add(row)
    except Exception:
        pass  # persistence is best-effort; never fail the API response
