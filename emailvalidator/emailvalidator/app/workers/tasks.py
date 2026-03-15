"""
Phase 3 — 8a: Retry Queue

Celery-based worker queue for async email validation.
Handles two cases that can't be done synchronously:

  1. Greylisted emails (4xx responses) — must retry 5–15 minutes later
  2. Bulk validation — thousands of emails processed in the background
     without blocking API responses

── Queue architecture ────────────────────────────────────────────────────────

    API request  →  enqueue_validation()  →  Redis queue
                                                  ↓
                               Celery worker picks up task
                                                  ↓
                         Run full pipeline (layers 1-7) with proxy
                                                  ↓
                         Store result in PostgreSQL result cache
                                                  ↓
                         Webhook callback (optional) or poll /result/{id}

── Retry strategy for greylisting ───────────────────────────────────────────

    Attempt 1: immediate
    Attempt 2: +5 minutes   (most greylisting resolves within 5 min)
    Attempt 3: +15 minutes
    Attempt 4: +60 minutes  (slow mail servers)
    Attempt 5: +4 hours     (last attempt before marking unknown)

    If all 5 attempts return greylisted → status = unknown, can_retry = False

── Task routing ──────────────────────────────────────────────────────────────

    Two queues:
      - "validation"  — normal priority, most emails
      - "retry"       — dedicated queue for greylisted retries
                        (separate so retries don't block fresh work)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ── Celery app ────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")

celery_app = Celery(
    "emailvalidator",
    broker=CELERY_BROKER_URL,
    backend=REDIS_URL,
)

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # Queues
    task_queues={
        "validation": {"exchange": "validation", "routing_key": "validation"},
        "retry":      {"exchange": "retry",      "routing_key": "retry"},
    },
    task_default_queue="validation",
    task_default_exchange="validation",
    task_default_routing_key="validation",

    # Result expiry — keep results for 48h in Redis
    result_expires=172_800,

    # Worker settings
    worker_prefetch_multiplier=1,   # one task at a time per worker process
    task_acks_late=True,            # ack only after completion (survive crashes)
    task_reject_on_worker_lost=True,

    # Retry backoff schedule (seconds)
    # Attempt 1: 0s, 2: 300s, 3: 900s, 4: 3600s, 5: 14400s
    task_annotations={
        "app.workers.tasks.validate_email_task": {
            "max_retries": 4,
            "default_retry_delay": 300,
        }
    },
)


# ── Retry countdown schedule ──────────────────────────────────────────────────

_GREYLIST_RETRY_DELAYS = [
    300,    # 5 min
    900,    # 15 min
    3_600,  # 1 hour
    14_400, # 4 hours
]


def _greylist_delay(attempt: int) -> int:
    """Return seconds to wait before attempt N (0-indexed)."""
    if attempt >= len(_GREYLIST_RETRY_DELAYS):
        return _GREYLIST_RETRY_DELAYS[-1]
    return _GREYLIST_RETRY_DELAYS[attempt]


# ── Core validation task ──────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="app.workers.tasks.validate_email_task",
    queue="validation",
    max_retries=4,
    soft_time_limit=60,   # 60s per task — SMTP probes can be slow
    time_limit=90,
)
def validate_email_task(
    self,
    email: str,
    *,
    skip_smtp: bool = False,
    webhook_url: str | None = None,
    request_id: str | None = None,
) -> dict:
    """
    Full validation pipeline task.

    Runs layers 1–7 with proxy-aware SMTP probing.
    Automatically retries on greylisting with exponential backoff.

    Args:
        email:       Email address to validate.
        skip_smtp:   If True, run only layers 1–4 (no SMTP probe).
        webhook_url: Optional URL to POST results to on completion.
        request_id:  Caller-supplied ID for result correlation.

    Returns:
        dict with full validation result (serialised ScoreResult + raw verdicts).
    """
    attempt = self.request.retries
    logger.info("Validating %s (attempt %d)", email, attempt + 1)

    try:
        result = _run_pipeline(email, skip_smtp=skip_smtp)
    except Exception as exc:
        logger.error("Pipeline error for %s: %s", email, exc)
        raise self.retry(exc=exc, countdown=_greylist_delay(attempt))

    # Check if we need to retry due to greylisting
    smtp_verdict = result.get("smtp_verdict")
    if smtp_verdict == "greylisted" and attempt < self.max_retries:
        delay = _greylist_delay(attempt)
        logger.info(
            "Greylisted %s on attempt %d, retrying in %ds",
            email, attempt + 1, delay
        )
        raise self.retry(
            countdown=delay,
            queue="retry",    # route to dedicated retry queue
            exc=None,
        )

    # Attach metadata
    result["request_id"] = request_id
    result["attempts"] = attempt + 1
    result["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Fire webhook if configured
    if webhook_url:
        _fire_webhook(webhook_url, result)

    return result


def _run_pipeline(email: str, *, skip_smtp: bool) -> dict:
    """
    Run the full validation pipeline synchronously within the worker.

    Imports are deferred to worker process so they don't load into
    the API process unnecessarily.
    """
    import asyncio
    from app.validators.syntax import validate_syntax
    from app.validators.domain import validate_domain
    from app.validators.disposable import check_disposable
    from app.validators.typo import check_typo
    from app.validators.catchall import detect_catch_all
    from app.validators.scorer import ScoreInput, score
    from app.validators.smtp import SMTPVerdict
    from app.validators.catchall import CatchAllVerdict

    # Layer 1 — syntax
    syntax = validate_syntax(email)
    if not syntax.valid:
        return {
            "email": email,
            "status": "invalid",
            "score": 0,
            "syntax_valid": False,
            "error": syntax.error,
        }

    # Layer 2 — domain/MX (async, run in event loop)
    loop = asyncio.new_event_loop()
    try:
        domain_result = loop.run_until_complete(
            validate_domain(syntax.domain)
        )
    finally:
        loop.close()

    # Layer 3 — disposable + role-based
    disp = check_disposable(syntax.local, syntax.domain)

    # Layer 4 — typo
    typo = check_typo(syntax.local, syntax.domain)

    # Layers 5 + 6 — SMTP + catch-all (optional)
    smtp_verdict = None
    if not skip_smtp and domain_result.mx_found and domain_result.primary_mx:
        loop2 = asyncio.new_event_loop()
        try:
            from app.workers.proxy_pool import get_pool
            pool = get_pool()
            proxy = pool.get_proxy()

            if proxy:
                from app.validators.catchall import detect_catch_all
                from app.validators.smtp import probe_mailbox, DEFAULT_HELO_HOSTNAME, DEFAULT_FROM_ADDRESS

                # Override probe kwargs with proxy's HELO/FROM
                catchall_result = loop2.run_until_complete(
                    detect_catch_all(
                        syntax.normalized,
                        domain_result.primary_mx,
                        helo_hostname=proxy.helo_hostname,
                        from_address=proxy.from_address,
                    )
                )
                pool.record_use(proxy.name)
                smtp_verdict = catchall_result.verdict.value
            else:
                smtp_verdict = "unknown"
                logger.warning("No proxies available for SMTP probe of %s", email)
        finally:
            loop2.close()

    # Layer 7 — score
    from app.validators.smtp import SMTPVerdict
    from app.validators.catchall import CatchAllVerdict

    # Map string verdict back to enum for scorer
    verdict_enum = None
    if smtp_verdict:
        try:
            verdict_enum = CatchAllVerdict(smtp_verdict)
        except ValueError:
            try:
                verdict_enum = SMTPVerdict(smtp_verdict)
            except ValueError:
                pass

    score_input = ScoreInput(
        syntax_valid=syntax.valid,
        domain_exists=domain_result.domain_exists,
        mx_found=domain_result.mx_found,
        is_disposable=disp.is_disposable,
        is_role_based=disp.is_role_based,
        has_typo=typo.has_typo,
        smtp_verdict=verdict_enum,
        normalized_email=syntax.normalized,
    )
    score_result = score(score_input)

    return {
        "email": email,
        "normalized": syntax.normalized,
        "status": score_result.status.value,
        "score": score_result.score,
        "reasons": score_result.reasons,
        "syntax_valid": syntax.valid,
        "domain_exists": domain_result.domain_exists,
        "mx_found": domain_result.mx_found,
        "is_disposable": disp.is_disposable,
        "is_role_based": disp.is_role_based,
        "typo_suggestion": typo.suggestion if typo.has_typo else None,
        "smtp_verdict": smtp_verdict,
        "primary_mx": domain_result.primary_mx,
    }


def _fire_webhook(url: str, result: dict) -> None:
    """POST validation result to a webhook URL. Best-effort, non-blocking."""
    try:
        import urllib.request, json
        data = json.dumps(result).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
        logger.info("Webhook fired to %s", url)
    except Exception as exc:
        logger.warning("Webhook failed for %s: %s", url, exc)


# ── Bulk validation task ──────────────────────────────────────────────────────

@celery_app.task(
    name="app.workers.tasks.validate_bulk_task",
    queue="validation",
    soft_time_limit=3600,
)
def validate_bulk_task(
    emails: list[str],
    *,
    skip_smtp: bool = False,
    webhook_url: str | None = None,
    batch_id: str | None = None,
) -> dict:
    """
    Validate a list of emails by fanning out to individual tasks.
    Returns a batch summary; individual results are stored by each sub-task.
    """
    task_ids = []
    for email in emails:
        task = validate_email_task.apply_async(
            kwargs={
                "email": email,
                "skip_smtp": skip_smtp,
                "webhook_url": webhook_url,
            },
            queue="validation",
        )
        task_ids.append(task.id)

    return {
        "batch_id": batch_id,
        "total": len(emails),
        "task_ids": task_ids,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
