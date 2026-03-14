from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Boolean, Date, DateTime, Enum as SqlEnum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Vertical(str, Enum):
    VEHICLE = "vehicle"
    TOURISM = "tourism"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationStatus(str, Enum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    RISKY = "risky"


class FormStatus(str, Enum):
    NONE = "none"
    DETECTED = "detected"
    QUEUED = "queued"
    SUBMITTED = "submitted"
    MANUAL_REVIEW = "manual_review"


class SubmissionStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FAILED = "failed"
    MANUAL_REVIEW = "manual_review"


class RunCompanyStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class Region(Base):
    __tablename__ = "regions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    country_code: Mapped[str] = mapped_column(String(2), index=True)
    osm_admin_level: Mapped[int] = mapped_column(Integer, default=2)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    companies: Mapped[list["Company"]] = relationship(back_populates="region")
    runs: Mapped[list["ScrapeRun"]] = relationship(back_populates="region")
    category_states: Mapped[list["RegionCategoryState"]] = relationship(back_populates="region")


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(128))
    vertical: Mapped[Vertical] = mapped_column(SqlEnum(Vertical), index=True)
    osm_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    search_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    companies: Mapped[list["CompanyCategory"]] = relationship(back_populates="category")
    run_items: Mapped[list["RunCategory"]] = relationship(back_populates="category")
    region_states: Mapped[list["RegionCategoryState"]] = relationship(back_populates="category")


class Company(Base):
    __tablename__ = "companies"
    __table_args__ = (
        UniqueConstraint("region_id", "source", "external_ref", name="uq_company_source_ref"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    website_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(64), default="overpass")
    external_ref: Mapped[str] = mapped_column(String(128))
    source_query: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    latitude: Mapped[str | None] = mapped_column(String(32), nullable=True)
    longitude: Mapped[str | None] = mapped_column(String(32), nullable=True)
    crawl_status: Mapped[str] = mapped_column(String(32), default="pending")
    has_contact_form: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    region: Mapped["Region"] = relationship(back_populates="companies")
    categories: Mapped[list["CompanyCategory"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    pages: Mapped[list["Page"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    emails: Mapped[list["Email"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    phones: Mapped[list["Phone"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    forms: Mapped[list["Form"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    run_companies: Mapped[list["RunCompany"]] = relationship(back_populates="company", cascade="all, delete-orphan")


class CompanyCategory(Base):
    __tablename__ = "company_categories"
    __table_args__ = (
        UniqueConstraint("company_id", "category_id", name="uq_company_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)

    company: Mapped["Company"] = relationship(back_populates="categories")
    category: Mapped["Category"] = relationship(back_populates="companies")


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    url: Mapped[str] = mapped_column(String(500))
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    crawled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    has_contact_form: Mapped[bool] = mapped_column(Boolean, default=False)
    crawl_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    company: Mapped["Company"] = relationship(back_populates="pages")


class Email(Base):
    __tablename__ = "emails"
    __table_args__ = (
        UniqueConstraint("company_id", "email", name="uq_company_email"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="regex")
    source_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    validation_status: Mapped[ValidationStatus] = mapped_column(SqlEnum(ValidationStatus), default=ValidationStatus.UNKNOWN, index=True)
    suppression_status: Mapped[str] = mapped_column(String(32), default="clear")
    technical_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    company: Mapped["Company"] = relationship(back_populates="emails")


class Phone(Base):
    __tablename__ = "phones"
    __table_args__ = (
        UniqueConstraint("company_id", "normalized_number", name="uq_company_phone"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    phone_number: Mapped[str] = mapped_column(String(64), index=True)
    normalized_number: Mapped[str] = mapped_column(String(32), index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="regex")
    source_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    technical_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    company: Mapped["Company"] = relationship(back_populates="phones")


class Form(Base):
    __tablename__ = "forms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    page_url: Mapped[str] = mapped_column(String(500))
    action_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    method: Mapped[str] = mapped_column(String(16), default="get")
    has_captcha: Mapped[bool] = mapped_column(Boolean, default=False)
    is_js_challenge: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[FormStatus] = mapped_column(SqlEnum(FormStatus), default=FormStatus.DETECTED)
    schema_json: Mapped[dict] = mapped_column(JSON, default=dict)
    last_checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    company: Mapped["Company"] = relationship(back_populates="forms")
    submissions: Mapped[list["Submission"]] = relationship(back_populates="form", cascade="all, delete-orphan")


class Submission(Base):
    __tablename__ = "submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    form_id: Mapped[int] = mapped_column(ForeignKey("forms.id"), index=True)
    status: Mapped[SubmissionStatus] = mapped_column(SqlEnum(SubmissionStatus), default=SubmissionStatus.PENDING)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    response_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    manual_review_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    form: Mapped["Form"] = relationship(back_populates="submissions")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    status: Mapped[RunStatus] = mapped_column(SqlEnum(RunStatus), default=RunStatus.PENDING, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    overpass_queries_used: Mapped[int] = mapped_column(Integer, default=0)
    discovered_count: Mapped[int] = mapped_column(Integer, default=0)
    crawled_count: Mapped[int] = mapped_column(Integer, default=0)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    region: Mapped["Region"] = relationship(back_populates="runs")
    categories: Mapped[list["RunCategory"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    companies: Mapped[list["RunCompany"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class RunCategory(Base):
    __tablename__ = "run_categories"
    __table_args__ = (
        UniqueConstraint("run_id", "category_id", name="uq_run_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)

    run: Mapped["ScrapeRun"] = relationship(back_populates="categories")
    category: Mapped["Category"] = relationship(back_populates="run_items")


class DailyUsage(Base):
    __tablename__ = "daily_usage"
    __table_args__ = (
        UniqueConstraint("usage_date", "provider", name="uq_usage_date_provider"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usage_date: Mapped[date] = mapped_column(Date, index=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    units_used: Mapped[int] = mapped_column(Integer, default=0)
    cap: Mapped[int] = mapped_column(Integer)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class RegionCategoryState(Base):
    __tablename__ = "region_category_states"
    __table_args__ = (
        UniqueConstraint("region_id", "category_id", name="uq_region_category_state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)
    last_run_id: Mapped[int | None] = mapped_column(ForeignKey("scrape_runs.id"), nullable=True)
    last_discovery_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_discovery_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_result_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="never_run")
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    region: Mapped["Region"] = relationship(back_populates="category_states")
    category: Mapped["Category"] = relationship(back_populates="region_states")


class RunCompany(Base):
    __tablename__ = "run_companies"
    __table_args__ = (
        UniqueConstraint("run_id", "company_id", name="uq_run_company"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    status: Mapped[RunCompanyStatus] = mapped_column(SqlEnum(RunCompanyStatus), default=RunCompanyStatus.QUEUED, index=True)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["ScrapeRun"] = relationship(back_populates="companies")
    company: Mapped["Company"] = relationship(back_populates="run_companies")


class RequestMetric(Base):
    __tablename__ = "request_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(ForeignKey("scrape_runs.id"), nullable=True, index=True)
    company_id: Mapped[int | None] = mapped_column(ForeignKey("companies.id"), nullable=True, index=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    request_kind: Mapped[str] = mapped_column(String(32), index=True)
    method: Mapped[str] = mapped_column(String(8))
    url: Mapped[str] = mapped_column(String(500))
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
