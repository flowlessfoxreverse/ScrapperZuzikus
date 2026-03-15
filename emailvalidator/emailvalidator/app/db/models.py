"""
Database models and async session management.

Uses SQLAlchemy 2.0 async style throughout.
Tables:
  - validation_results   stores every completed validation
  - validation_jobs      tracks async job status
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, Index, Integer, String, Text,
    func, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://emailval:emailval@db:5432/emailval",
)

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,     # reconnect on stale connections
    echo=os.getenv("DEBUG", "false").lower() == "true",
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


# ── Base ──────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ── Models ────────────────────────────────────────────────────────────────────

class ValidationResult(Base):
    """
    Persisted result for every completed validation.
    Used as a cache — subsequent requests for the same email within
    the TTL window return the cached result without re-probing.
    """
    __tablename__ = "validation_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    normalized: Mapped[str | None] = mapped_column(String(320), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    score: Mapped[int] = mapped_column(Integer, nullable=False)

    # Layer signals stored as flat columns for fast querying
    syntax_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    domain_exists: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    mx_found: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_disposable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_role_based: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    typo_suggestion: Mapped[str | None] = mapped_column(String(320), nullable=True)
    smtp_verdict: Mapped[str | None] = mapped_column(String(32), nullable=True)
    primary_mx: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Full breakdown stored as JSON for extensibility
    reasons: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    validated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        # Fast cache lookup: find recent results for an email
        Index("ix_validation_results_email_validated_at", "email", "validated_at"),
    )


class ValidationJob(Base):
    """
    Tracks the lifecycle of an async validation job.
    Created when a job is queued, updated when it completes.
    """
    __tablename__ = "validation_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)   # UUID
    email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    result_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    webhook_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    queued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
