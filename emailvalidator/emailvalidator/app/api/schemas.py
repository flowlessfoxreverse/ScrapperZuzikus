"""
API Schemas — Pydantic models for all request and response bodies.

Keeping schemas separate from DB models means the API contract
stays stable even as internal storage evolves.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, EmailStr, Field, HttpUrl, field_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

class EmailStatus(str, Enum):
    VALID    = "valid"
    INVALID  = "invalid"
    RISKY    = "risky"
    UNKNOWN  = "unknown"


class JobStatus(str, Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"


# ── Single validation ─────────────────────────────────────────────────────────

class ValidateRequest(BaseModel):
    email: str = Field(..., description="Email address to validate")

    @field_validator("email")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class ValidationDetail(BaseModel):
    """Per-layer signal breakdown returned with every result."""
    syntax_valid: bool
    domain_exists: bool | None = None
    mx_found: bool | None = None
    is_disposable: bool | None = None
    is_role_based: bool | None = None
    typo_suggestion: str | None = None
    smtp_verdict: str | None = None
    primary_mx: str | None = None


class ValidationResponse(BaseModel):
    """
    Response for POST /validate (sync, layers 1–4)
    and GET /result/{job_id} (async, all layers).
    """
    email: str
    normalized: str | None = None
    status: EmailStatus
    score: int = Field(..., ge=0, le=100)
    reasons: list[str] = Field(default_factory=list)
    detail: ValidationDetail
    cached: bool = False
    validated_at: datetime


# ── Async job ─────────────────────────────────────────────────────────────────

class FullValidateRequest(BaseModel):
    email: str = Field(..., description="Email address for full SMTP validation")
    webhook_url: str | None = Field(
        None,
        description="Optional URL to POST result to on completion",
    )

    @field_validator("email")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class JobResponse(BaseModel):
    """Returned immediately when an async job is queued."""
    job_id: str
    email: str
    status: JobStatus
    queued_at: datetime
    poll_url: str


# ── Bulk validation ───────────────────────────────────────────────────────────

class BulkRequest(BaseModel):
    emails: list[str] = Field(
        ...,
        min_length=1,
        max_length=10_000,
        description="List of email addresses (max 10,000 per request)",
    )
    skip_smtp: bool = Field(
        False,
        description="If true, run only layers 1–4 (faster, no SMTP probe)",
    )
    webhook_url: str | None = Field(
        None,
        description="URL to POST each result to as it completes",
    )

    @field_validator("emails")
    @classmethod
    def strip_emails(cls, v: list[str]) -> list[str]:
        return [e.strip() for e in v if e.strip()]


class BulkResponse(BaseModel):
    batch_id: str
    total: int
    queued_at: datetime
    estimated_seconds: int = Field(
        description="Rough estimate: 2s per email for full, 0.1s for skip_smtp"
    )
    poll_url: str


# ── Result polling ────────────────────────────────────────────────────────────

class ResultResponse(BaseModel):
    job_id: str
    status: JobStatus
    result: ValidationResponse | None = None
    error: str | None = None
    queued_at: datetime
    completed_at: datetime | None = None


# ── Admin ─────────────────────────────────────────────────────────────────────

class ProxyStatEntry(BaseModel):
    name: str
    uses_today: int
    daily_limit: int
    utilization_pct: float
    is_exhausted: bool
    is_direct: bool
    host: str | None


class ProxyStatsResponse(BaseModel):
    total_proxies: int
    available_proxies: int
    proxies: list[ProxyStatEntry]


# ── Health ────────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    db: str
    redis: str
    workers: str
    disposable_domains: int
    version: str = "0.1.0"
