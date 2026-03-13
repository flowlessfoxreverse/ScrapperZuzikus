from __future__ import annotations

from sqlalchemy.orm import Session

from app.models import RequestMetric


def record_request_metric(
    session: Session,
    *,
    provider: str,
    request_kind: str,
    method: str,
    url: str,
    duration_ms: int,
    status_code: int | None = None,
    error: str | None = None,
    run_id: int | None = None,
    company_id: int | None = None,
) -> None:
    session.add(
        RequestMetric(
            run_id=run_id,
            company_id=company_id,
            provider=provider,
            request_kind=request_kind,
            method=method,
            url=url[:500],
            status_code=status_code,
            duration_ms=duration_ms,
            error=error[:2000] if error else None,
        )
    )
