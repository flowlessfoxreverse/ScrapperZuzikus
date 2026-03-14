from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote_plus
from urllib.parse import urlparse
import re

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.models import Category, Company, ContactChannel, ContactChannelType, Email, Phone, ProxyEndpoint, ProxyKind, Region, RequestMetric, RunCategory, RunStatus, ScrapeRun, ValidationStatus, Vertical
from app.schemas import EmailRow
from app.services.host_suppression import normalize_host_key
from app.services.overpass import fetch_status
from app.services.proxy_pool import active_proxy_count, effective_proxy_capacity, lease_counts, list_proxies, release_proxy, upsert_proxy
from app.services.region_catalog import country_catalog, upsert_country_with_subdivisions
from app.services.runs import find_active_run, request_run_cancellation
from app.tasks import run_scrape, sync_region_catalog_task


templates = Jinja2Templates(directory="app/templates")
router = APIRouter(tags=["ui"])
RECENT_RUNS_PAGE_SIZE = 25
settings = get_settings()


@dataclass
class RegionStatsRow:
    id: int
    name: str
    code: str
    total_companies: int
    total_emails: int
    valid_emails: int
    last_run_status: str | None


@dataclass
class CountryOption:
    code: str
    name: str
    region_id: int
    province_count: int
    total_companies: int
    total_emails: int


@dataclass
class CompanyAuditRow:
    id: int
    company_name: str
    company_city: str | None
    company_website: str | None
    region_name: str
    crawl_status: str
    has_contact_form: bool
    email_count: int
    latest_email: str | None
    phone_count: int
    latest_phone: str | None
    whatsapp_count: int
    latest_whatsapp: str | None
    telegram_count: int
    latest_telegram: str | None


@dataclass
class ProxyRow:
    id: int
    label: str
    proxy_url: str
    kind: str
    supports_http: bool
    supports_browser: bool
    max_http_leases: int
    max_browser_leases: int
    current_http_leases: int
    current_browser_leases: int
    is_active: bool
    leased_by: str | None
    success_count: int
    failure_count: int
    consecutive_failures: int
    health_score: int
    cooldown_until: datetime | None
    auto_disabled_at: datetime | None
    status_label: str
    notes: str | None
    proxied_request_count: int
    proxied_error_count: int
    avg_duration_ms: int
    last_seen_at: datetime | None


@dataclass
class MetricSummaryRow:
    provider: str
    request_kind: str
    transport: str
    request_count: int
    avg_duration_ms: int
    max_duration_ms: int
    error_count: int


@dataclass
class HostMetricRow:
    host: str
    provider: str
    request_count: int
    avg_duration_ms: int
    max_duration_ms: int
    proxied_requests: int


@dataclass
class ErrorMetricRow:
    provider: str
    request_kind: str
    error: str
    request_count: int
    last_seen_at: datetime


@dataclass
class ProxyMetricRow:
    proxy_label: str
    provider: str
    request_count: int
    error_count: int
    avg_duration_ms: int
    max_duration_ms: int
    last_seen_at: datetime


@dataclass
class SignalMetricRow:
    provider: str
    signal: str
    request_count: int
    proxied_count: int
    error_count: int
    last_seen_at: datetime


def proxy_status_label(proxy: ProxyEndpoint) -> str:
    now = datetime.now(timezone.utc)
    if proxy.auto_disabled_at is not None or not proxy.is_active:
        return "auto-disabled" if proxy.auto_disabled_at is not None else "disabled"
    if proxy.cooldown_until is not None and proxy.cooldown_until > now:
        return f"cooldown until {proxy.cooldown_until.astimezone(timezone.utc).strftime('%H:%M UTC')}"
    return "active"


def summarize_run_note(note: str | None) -> str:
    if not note:
        return "-"
    if len(note) <= 140:
        return note

    if "Partial category failures:" in note:
        failures = note.split("Partial category failures:", 1)[1]
        categories = []
        for raw in failures.split(" ; "):
            category = raw.split(":", 1)[0].strip()
            if category:
                categories.append(category)
        categories = list(dict.fromkeys(categories))
        if categories:
            shown = ", ".join(categories[:3])
            remainder = len(categories) - min(len(categories), 3)
            suffix = f" +{remainder} more" if remainder > 0 else ""
            return f"Discovery completed with warnings: {shown}{suffix}."
        return "Discovery completed with category warnings."

    if "Overpass connection failed" in note or "Connection refused" in note:
        categories = re.findall(r"([a-z0-9-]+): Overpass connection failed", note)
        if categories:
            shown = ", ".join(categories[:3])
            remainder = len(categories) - min(len(categories), 3)
            suffix = f" +{remainder} more" if remainder > 0 else ""
            return f"Failed: Overpass unavailable for {shown}{suffix}."
        return "Failed: Overpass was unavailable."

    if "Overpass returned non-JSON payload" in note:
        categories = re.findall(r"([a-z0-9-]+): Overpass returned non-JSON payload", note)
        if categories:
            return f"Completed with warnings: unsupported Overpass response for {', '.join(categories[:3])}."
        return "Completed with warnings: unsupported Overpass response."

    if note.startswith("Worker crashed during discovery:"):
        return "Failed: discovery worker crashed."
    if note.startswith("Worker crashed during crawl:"):
        return "Failed: crawl worker crashed."

    return f"{note[:137]}..."


def build_email_rows(
    db: Session,
    region_id: int | None = None,
    country_code: str | None = None,
) -> list[EmailRow]:
    phone_summary_stmt = (
        select(
            Phone.company_id,
            func.count(func.distinct(Phone.id)).label("phone_count"),
            func.max(Phone.phone_number).label("latest_phone"),
        )
        .group_by(Phone.company_id)
    )
    phone_summary = {
        company_id: (phone_count or 0, latest_phone)
        for company_id, phone_count, latest_phone in db.execute(phone_summary_stmt).all()
    }
    channel_summary_stmt = (
        select(
            ContactChannel.company_id,
            func.count(func.distinct(case((ContactChannel.channel_type == ContactChannelType.WHATSAPP, ContactChannel.id)))).label("whatsapp_count"),
            func.max(case((ContactChannel.channel_type == ContactChannelType.WHATSAPP, ContactChannel.channel_value))).label("latest_whatsapp"),
            func.count(func.distinct(case((ContactChannel.channel_type == ContactChannelType.TELEGRAM, ContactChannel.id)))).label("telegram_count"),
            func.max(case((ContactChannel.channel_type == ContactChannelType.TELEGRAM, ContactChannel.channel_value))).label("latest_telegram"),
        )
        .group_by(ContactChannel.company_id)
    )
    channel_summary = {
        company_id: (
            whatsapp_count or 0,
            latest_whatsapp,
            telegram_count or 0,
            latest_telegram,
        )
        for company_id, whatsapp_count, latest_whatsapp, telegram_count, latest_telegram in db.execute(channel_summary_stmt).all()
    }

    stmt = (
        select(Email, Region)
        .join(Company, Email.company_id == Company.id)
        .join(Region, Company.region_id == Region.id)
        .order_by(desc(Email.last_seen_at))
    )
    if region_id:
        stmt = stmt.where(Region.id == region_id)
    elif country_code:
        stmt = stmt.where(Region.country_code == country_code)

    rows = []
    for email, region in db.execute(stmt).all():
        rows.append(
            EmailRow(
                id=email.id,
                email=email.email,
                company_name=email.company.name,
                company_city=email.company.city,
                company_website=email.company.website_url,
                company_phone_count=phone_summary.get(email.company_id, (0, None))[0],
                company_latest_phone=phone_summary.get(email.company_id, (0, None))[1],
                company_whatsapp_count=channel_summary.get(email.company_id, (0, None, 0, None))[0],
                company_latest_whatsapp=channel_summary.get(email.company_id, (0, None, 0, None))[1],
                company_telegram_count=channel_summary.get(email.company_id, (0, None, 0, None))[2],
                company_latest_telegram=channel_summary.get(email.company_id, (0, None, 0, None))[3],
                region_name=region.name,
                validation_status=email.validation_status,
                suppression_status=email.suppression_status,
                source_type=email.source_type,
                source_page_url=email.source_page_url,
                crawl_status=email.company.crawl_status,
                has_contact_form=email.company.has_contact_form,
                technical_metadata=email.technical_metadata,
            )
        )
    return rows


def build_company_audit_rows(
    db: Session,
    region_id: int | None = None,
    country_code: str | None = None,
) -> list[CompanyAuditRow]:
    email_count = func.count(func.distinct(Email.id))
    latest_email = func.max(Email.email)
    phone_count = func.count(func.distinct(Phone.id))
    latest_phone = func.max(Phone.phone_number)
    whatsapp_count = func.count(func.distinct(case((ContactChannel.channel_type == ContactChannelType.WHATSAPP, ContactChannel.id))))
    latest_whatsapp = func.max(case((ContactChannel.channel_type == ContactChannelType.WHATSAPP, ContactChannel.channel_value)))
    telegram_count = func.count(func.distinct(case((ContactChannel.channel_type == ContactChannelType.TELEGRAM, ContactChannel.id))))
    latest_telegram = func.max(case((ContactChannel.channel_type == ContactChannelType.TELEGRAM, ContactChannel.channel_value)))
    stmt = (
        select(
            Company,
            Region,
            email_count.label("email_count"),
            latest_email.label("latest_email"),
            phone_count.label("phone_count"),
            latest_phone.label("latest_phone"),
            whatsapp_count.label("whatsapp_count"),
            latest_whatsapp.label("latest_whatsapp"),
            telegram_count.label("telegram_count"),
            latest_telegram.label("latest_telegram"),
        )
        .join(Region, Company.region_id == Region.id)
        .outerjoin(Email, Email.company_id == Company.id)
        .outerjoin(Phone, Phone.company_id == Company.id)
        .outerjoin(ContactChannel, ContactChannel.company_id == Company.id)
        .group_by(Company.id, Region.id)
        .order_by(Region.name.asc(), Company.name.asc())
    )
    if region_id:
        stmt = stmt.where(Region.id == region_id)
    elif country_code:
        stmt = stmt.where(Region.country_code == country_code)

    rows: list[CompanyAuditRow] = []
    for (
        company,
        region,
        email_count_value,
        latest_email_value,
        phone_count_value,
        latest_phone_value,
        whatsapp_count_value,
        latest_whatsapp_value,
        telegram_count_value,
        latest_telegram_value,
    ) in db.execute(stmt).all():
        rows.append(
            CompanyAuditRow(
                id=company.id,
                company_name=company.name,
                company_city=company.city,
                company_website=company.website_url,
                region_name=region.name,
                crawl_status=company.crawl_status,
                has_contact_form=company.has_contact_form,
                email_count=email_count_value or 0,
                latest_email=latest_email_value,
                phone_count=phone_count_value or 0,
                latest_phone=latest_phone_value,
                whatsapp_count=whatsapp_count_value or 0,
                latest_whatsapp=latest_whatsapp_value,
                telegram_count=telegram_count_value or 0,
                latest_telegram=latest_telegram_value,
            )
        )
    return rows


def build_country_options(db: Session) -> list[CountryOption]:
    countries = db.scalars(
        select(Region)
        .where(Region.is_active.is_(True), Region.osm_admin_level == 2)
        .order_by(Region.name)
    ).all()
    company_counts = {
        country: total
        for country, total in db.execute(
            select(Region.country_code, func.count(Company.id))
            .select_from(Company)
            .join(Region, Company.region_id == Region.id)
            .group_by(Region.country_code)
        ).all()
    }
    email_counts = {
        country: total
        for country, total in db.execute(
            select(Region.country_code, func.count(Email.id))
            .select_from(Email)
            .join(Company, Email.company_id == Company.id)
            .join(Region, Company.region_id == Region.id)
            .group_by(Region.country_code)
        ).all()
    }
    options: list[CountryOption] = []
    for country in countries:
        province_count = db.scalar(
            select(func.count())
            .select_from(Region)
            .where(
                Region.is_active.is_(True),
                Region.country_code == country.country_code,
                Region.osm_admin_level > 2,
            )
        ) or 0
        options.append(
            CountryOption(
                code=country.country_code,
                name=country.name,
                region_id=country.id,
                province_count=province_count,
                total_companies=company_counts.get(country.country_code, 0),
                total_emails=email_counts.get(country.country_code, 0),
            )
        )
    return options


def build_region_stats(db: Session, country_code: str | None = None) -> list[RegionStatsRow]:
    rows = []
    stmt = select(Region).where(Region.is_active.is_(True), Region.osm_admin_level > 2).order_by(Region.name)
    if country_code:
        stmt = stmt.where(Region.country_code == country_code)
    regions = db.scalars(stmt).all()
    for region in regions:
        total_companies = db.scalar(select(func.count()).select_from(Company).where(Company.region_id == region.id)) or 0
        total_emails = db.scalar(
            select(func.count()).select_from(Email).join(Company, Email.company_id == Company.id).where(Company.region_id == region.id)
        ) or 0
        valid_emails = db.scalar(
            select(func.count()).select_from(Email).join(Company, Email.company_id == Company.id).where(
                Company.region_id == region.id,
                Email.validation_status == ValidationStatus.VALID,
            )
        ) or 0
        last_run = db.scalar(
            select(ScrapeRun.status).where(ScrapeRun.region_id == region.id).order_by(ScrapeRun.started_at.desc()).limit(1)
        )
        if total_companies == 0 and total_emails == 0 and last_run is None:
            continue
        rows.append(
            RegionStatsRow(
                id=region.id,
                name=region.name,
                code=region.code,
                total_companies=total_companies,
                total_emails=total_emails,
                valid_emails=valid_emails,
                last_run_status=last_run.value if last_run else None,
            )
        )
    return rows


def build_recent_runs_page(db: Session, *, offset: int = 0, limit: int = RECENT_RUNS_PAGE_SIZE) -> tuple[list[ScrapeRun], bool]:
    rows = db.scalars(
        select(ScrapeRun)
        .order_by(desc(ScrapeRun.started_at))
        .offset(offset)
        .limit(limit + 1)
    ).all()
    has_more = len(rows) > limit
    return rows[:limit], has_more


def build_request_metric_views(
    db: Session,
    *,
    limit: int = 2000,
) -> tuple[list[MetricSummaryRow], list[HostMetricRow], list[ErrorMetricRow], list[ProxyMetricRow], list[SignalMetricRow]]:
    metrics = db.scalars(
        select(RequestMetric)
        .order_by(RequestMetric.created_at.desc())
        .limit(limit)
    ).all()

    summary_buckets: dict[tuple[str, str, str], dict[str, int]] = {}
    host_buckets: dict[tuple[str, str], dict[str, int]] = {}
    error_buckets: dict[tuple[str, str, str], dict[str, object]] = {}
    proxy_buckets: dict[tuple[str, str], dict[str, object]] = {}
    signal_buckets: dict[tuple[str, str], dict[str, object]] = {}
    signal_kinds = {"suppressed_host", "js_shell", "early_stopped", "browser_escalation", "anti_bot_challenge"}

    for metric in metrics:
        transport = "proxy" if metric.used_proxy else "direct"
        summary_key = (metric.provider, metric.request_kind, transport)
        summary_bucket = summary_buckets.setdefault(
            summary_key,
            {"count": 0, "duration_total": 0, "max_duration": 0, "error_count": 0},
        )
        summary_bucket["count"] += 1
        summary_bucket["duration_total"] += metric.duration_ms
        summary_bucket["max_duration"] = max(summary_bucket["max_duration"], metric.duration_ms)
        if metric.error:
            summary_bucket["error_count"] += 1

        host = normalize_host_key(metric.url) or urlparse(metric.url).netloc or urlparse(f"https://{metric.url}").netloc or metric.url[:80]
        host_key = (host, metric.provider)
        host_bucket = host_buckets.setdefault(
            host_key,
            {"count": 0, "duration_total": 0, "max_duration": 0, "proxied_count": 0},
        )
        host_bucket["count"] += 1
        host_bucket["duration_total"] += metric.duration_ms
        host_bucket["max_duration"] = max(host_bucket["max_duration"], metric.duration_ms)
        if metric.used_proxy:
            host_bucket["proxied_count"] += 1

        if metric.error:
            error_key = (metric.provider, metric.request_kind, metric.error[:180])
            error_bucket = error_buckets.setdefault(
                error_key,
                {"count": 0, "last_seen_at": metric.created_at},
            )
            error_bucket["count"] += 1
            if metric.created_at > error_bucket["last_seen_at"]:
                error_bucket["last_seen_at"] = metric.created_at

        if metric.used_proxy:
            proxy_label = metric.proxy_label or (f"proxy-{metric.proxy_id}" if metric.proxy_id else "unknown-proxy")
            proxy_key = (proxy_label, metric.provider)
            proxy_bucket = proxy_buckets.setdefault(
                proxy_key,
                {
                    "count": 0,
                    "error_count": 0,
                    "duration_total": 0,
                    "max_duration": 0,
                    "last_seen_at": metric.created_at,
                },
            )
            proxy_bucket["count"] += 1
            proxy_bucket["duration_total"] += metric.duration_ms
            proxy_bucket["max_duration"] = max(proxy_bucket["max_duration"], metric.duration_ms)
            if metric.error:
                proxy_bucket["error_count"] += 1
            if metric.created_at > proxy_bucket["last_seen_at"]:
                proxy_bucket["last_seen_at"] = metric.created_at

        if metric.request_kind in signal_kinds:
            signal_key = (metric.provider, metric.request_kind)
            signal_bucket = signal_buckets.setdefault(
                signal_key,
                {"count": 0, "proxied_count": 0, "error_count": 0, "last_seen_at": metric.created_at},
            )
            signal_bucket["count"] += 1
            if metric.used_proxy:
                signal_bucket["proxied_count"] += 1
            if metric.error:
                signal_bucket["error_count"] += 1
            if metric.created_at > signal_bucket["last_seen_at"]:
                signal_bucket["last_seen_at"] = metric.created_at

    summaries = [
        MetricSummaryRow(
            provider=provider,
            request_kind=request_kind,
            transport=transport,
            request_count=values["count"],
            avg_duration_ms=round(values["duration_total"] / values["count"]),
            max_duration_ms=values["max_duration"],
            error_count=values["error_count"],
        )
        for (provider, request_kind, transport), values in summary_buckets.items()
    ]
    summaries.sort(key=lambda row: (row.provider, row.request_kind, row.transport))

    hosts = [
        HostMetricRow(
            host=host,
            provider=provider,
            request_count=values["count"],
            avg_duration_ms=round(values["duration_total"] / values["count"]),
            max_duration_ms=values["max_duration"],
            proxied_requests=values["proxied_count"],
        )
        for (host, provider), values in host_buckets.items()
    ]
    hosts.sort(key=lambda row: (-row.avg_duration_ms, -row.request_count, row.host))

    errors = [
        ErrorMetricRow(
            provider=provider,
            request_kind=request_kind,
            error=error,
            request_count=values["count"],
            last_seen_at=values["last_seen_at"],
        )
        for (provider, request_kind, error), values in error_buckets.items()
    ]
    errors.sort(key=lambda row: (-row.request_count, row.provider, row.request_kind))
    proxies = [
        ProxyMetricRow(
            proxy_label=proxy_label,
            provider=provider,
            request_count=values["count"],
            error_count=values["error_count"],
            avg_duration_ms=round(values["duration_total"] / values["count"]),
            max_duration_ms=values["max_duration"],
            last_seen_at=values["last_seen_at"],
        )
        for (proxy_label, provider), values in proxy_buckets.items()
    ]
    proxies.sort(key=lambda row: (-row.request_count, row.proxy_label, row.provider))
    signals = [
        SignalMetricRow(
            provider=provider,
            signal=signal,
            request_count=values["count"],
            proxied_count=values["proxied_count"],
            error_count=values["error_count"],
            last_seen_at=values["last_seen_at"],
        )
        for (provider, signal), values in signal_buckets.items()
    ]
    signals.sort(key=lambda row: (-row.request_count, row.provider, row.signal))
    return summaries, hosts[:20], errors[:20], proxies[:20], signals[:20]


def build_proxy_usage_map(db: Session, *, limit: int = 5000) -> dict[int, dict[str, object]]:
    metrics = db.scalars(
        select(RequestMetric)
        .where(RequestMetric.used_proxy.is_(True), RequestMetric.proxy_id.is_not(None))
        .order_by(RequestMetric.created_at.desc())
        .limit(limit)
    ).all()
    usage: dict[int, dict[str, object]] = {}
    for metric in metrics:
        if metric.proxy_id is None:
            continue
        bucket = usage.setdefault(
            metric.proxy_id,
            {"count": 0, "error_count": 0, "duration_total": 0, "last_seen_at": None},
        )
        bucket["count"] += 1
        bucket["duration_total"] += metric.duration_ms
        if metric.error:
            bucket["error_count"] += 1
        if bucket["last_seen_at"] is None or metric.created_at > bucket["last_seen_at"]:
            bucket["last_seen_at"] = metric.created_at
    return usage


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    region_id: int | None = None,
    country_code: str | None = None,
    show_all: int = 0,
    message: str | None = None,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    countries = build_country_options(db)
    categories = db.scalars(select(Category).where(Category.is_active.is_(True)).order_by(Category.vertical, Category.label)).all()
    detail_region = db.get(Region, region_id) if region_id else None
    selected_country_code = country_code or (detail_region.country_code if detail_region else None)
    if not selected_country_code and countries:
        selected_country_code = countries[0].code
    selected_country = next((country for country in countries if country.code == selected_country_code), None)

    country_region = db.scalar(
        select(Region).where(
            Region.is_active.is_(True),
            Region.country_code == selected_country_code,
            Region.osm_admin_level == 2,
        )
    ) if selected_country_code else None
    provinces = db.scalars(
        select(Region)
        .where(
            Region.is_active.is_(True),
            Region.country_code == selected_country_code,
            Region.osm_admin_level > 2,
        )
        .order_by(Region.name)
    ).all() if selected_country_code else []

    default_region_ids = [region.id for region in provinces]
    if not default_region_ids and country_region is not None:
        default_region_ids = [country_region.id]

    emails = build_email_rows(
        db,
        region_id=detail_region.id if detail_region else None,
        country_code=selected_country_code if not detail_region else None,
    )
    company_rows = build_company_audit_rows(
        db,
        region_id=detail_region.id if detail_region else None,
        country_code=selected_country_code if not detail_region else None,
    ) if show_all else []
    runs, runs_has_more = build_recent_runs_page(db, offset=0)
    region_stats = build_region_stats(db, selected_country_code)
    overpass_status = fetch_status()
    browser_proxy_slots = active_proxy_count(db, ProxyKind.BROWSER)
    crawler_proxy_slots = active_proxy_count(db, ProxyKind.CRAWLER)
    browser_thread_capacity = settings.browser_worker_processes * settings.browser_worker_threads
    crawler_thread_capacity = settings.crawl_worker_processes * settings.crawl_worker_threads
    effective_browser_capacity = min(effective_proxy_capacity(db, ProxyKind.BROWSER), browser_thread_capacity)
    effective_crawler_capacity = min(effective_proxy_capacity(db, ProxyKind.CRAWLER), crawler_thread_capacity)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "countries": countries,
            "selected_country": selected_country,
            "categories": categories,
            "selected_country_code": selected_country_code,
            "country_region": country_region,
            "provinces": provinces,
            "default_region_ids": default_region_ids,
            "detail_region": detail_region,
            "show_all": bool(show_all),
            "emails": emails,
            "company_rows": company_rows,
            "runs": runs,
            "runs_has_more": runs_has_more,
            "runs_page_size": RECENT_RUNS_PAGE_SIZE,
            "summarize_run_note": summarize_run_note,
            "region_stats": region_stats,
            "overpass_status": overpass_status,
            "browser_proxy_slots": browser_proxy_slots,
            "crawler_proxy_slots": crawler_proxy_slots,
            "browser_thread_capacity": browser_thread_capacity,
            "crawler_thread_capacity": crawler_thread_capacity,
            "effective_browser_capacity": effective_browser_capacity,
            "effective_crawler_capacity": effective_crawler_capacity,
            "message": message,
            "validation_statuses": list(ValidationStatus),
        },
    )


@router.post("/runs", response_class=HTMLResponse)
def queue_run(
    country_code: str = Form(...),
    region_ids: list[int] | None = Form(None),
    category_ids: list[int] = Form(...),
    force_refresh: str | None = Form(None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    selected_regions = []
    if region_ids:
        selected_regions = db.scalars(
            select(Region)
            .where(
                Region.id.in_(region_ids),
                Region.is_active.is_(True),
            )
            .order_by(Region.name)
        ).all()

    if not selected_regions:
        fallback_region = db.scalar(
            select(Region).where(
                Region.is_active.is_(True),
                Region.country_code == country_code,
                Region.osm_admin_level == 2,
            )
        )
        if fallback_region is not None:
            selected_regions = [fallback_region]

    if not selected_regions:
        message = quote_plus("No provinces available for the selected country.")
        return RedirectResponse(url=f"/?country_code={country_code}&message={message}", status_code=303)

    queued_runs: list[int] = []
    skipped_regions: list[str] = []
    force_refresh_category_ids = category_ids if force_refresh == "1" else []

    for region in selected_regions:
        active_run = find_active_run(db, region.id)
        if active_run is not None:
            skipped_regions.append(region.name)
            continue

        run = ScrapeRun(region_id=region.id)
        db.add(run)
        db.flush()
        for category_id in category_ids:
            db.add(RunCategory(run_id=run.id, category_id=category_id))
        db.commit()
        queued_runs.append(run.id)
        run_scrape.send(run.id, force_refresh_category_ids=force_refresh_category_ids)

    if queued_runs and skipped_regions:
        message = f"Queued {len(queued_runs)} province runs. Skipped active regions: {', '.join(skipped_regions[:5])}"
        if len(skipped_regions) > 5:
            message += f" and {len(skipped_regions) - 5} more."
    elif queued_runs:
        message = f"Queued {len(queued_runs)} province runs for {country_code}."
    else:
        message = "All selected provinces already have active runs."

    return RedirectResponse(
        url=f"/?country_code={country_code}&message={quote_plus(message)}",
        status_code=303,
    )


@router.post("/runs/{run_id}/cancel", response_class=HTMLResponse)
def cancel_run_html(
    run_id: int,
    country_code: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    run = db.get(ScrapeRun, run_id)
    if run is None:
        message = quote_plus("Run not found.")
        return RedirectResponse(url=f"/?country_code={country_code}&message={message}", status_code=303)
    if run.status not in {RunStatus.PENDING, RunStatus.RUNNING}:
        message = quote_plus(f"Run {run.id} is already {run.status.value}.")
        return RedirectResponse(url=f"/?country_code={country_code}&message={message}", status_code=303)

    request_run_cancellation(db, run_id, "Stopped by user request.")
    db.commit()
    message = quote_plus(f"Stop requested for run {run.id}.")
    return RedirectResponse(url=f"/?country_code={country_code}&message={message}", status_code=303)


@router.get("/runs/recent", response_class=HTMLResponse)
def recent_runs_partial(
    request: Request,
    offset: int = 0,
    country_code: str = "",
    db: Session = Depends(get_db),
) -> HTMLResponse:
    runs, runs_has_more = build_recent_runs_page(db, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name="partials/recent_runs_rows.html",
        context={
            "runs": runs,
            "runs_has_more": runs_has_more,
            "runs_offset": offset,
            "runs_page_size": RECENT_RUNS_PAGE_SIZE,
            "selected_country_code": country_code,
            "summarize_run_note": summarize_run_note,
        },
    )


@router.post("/emails/{email_id}/status", response_class=HTMLResponse)
def update_email_status_html(
    request: Request,
    email_id: int,
    validation_status: ValidationStatus = Form(...),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    email = db.get(Email, email_id)
    email.validation_status = validation_status
    db.add(email)
    db.commit()
    return templates.TemplateResponse(
        request=request,
        name="partials/email_status.html",
        context={"email": email},
    )


@router.get("/categories", response_class=HTMLResponse)
def category_editor(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    categories = db.scalars(select(Category).order_by(Category.vertical, Category.label)).all()
    return templates.TemplateResponse(
        request=request,
        name="categories.html",
        context={"categories": categories},
    )


@router.get("/regions", response_class=HTMLResponse)
def region_editor(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="regions.html",
        context={
            "regions": build_country_options(db),
            "country_catalog": country_catalog(),
        },
    )


@router.get("/proxies", response_class=HTMLResponse)
def proxy_editor(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    browser_proxy_slots = active_proxy_count(db, ProxyKind.BROWSER)
    crawler_proxy_slots = active_proxy_count(db, ProxyKind.CRAWLER)
    browser_thread_capacity = settings.browser_worker_processes * settings.browser_worker_threads
    crawler_thread_capacity = settings.crawl_worker_processes * settings.crawl_worker_threads
    current_leases = lease_counts(db)
    usage = build_proxy_usage_map(db)
    proxies = [
        ProxyRow(
            id=proxy.id,
            label=proxy.label,
            proxy_url=proxy.proxy_url,
            kind=proxy.kind.value,
            supports_http=proxy.supports_http,
            supports_browser=proxy.supports_browser,
            max_http_leases=proxy.max_http_leases,
            max_browser_leases=proxy.max_browser_leases,
            current_http_leases=current_leases.get(proxy.id, {}).get("crawler", 0),
            current_browser_leases=current_leases.get(proxy.id, {}).get("browser", 0),
            is_active=proxy.is_active,
            leased_by=proxy.leased_by,
            success_count=proxy.success_count,
            failure_count=proxy.failure_count,
            consecutive_failures=proxy.consecutive_failures,
            health_score=proxy.health_score,
            cooldown_until=proxy.cooldown_until,
            auto_disabled_at=proxy.auto_disabled_at,
            status_label=proxy_status_label(proxy),
            notes=proxy.notes,
            proxied_request_count=int(usage.get(proxy.id, {}).get("count", 0)),
            proxied_error_count=int(usage.get(proxy.id, {}).get("error_count", 0)),
            avg_duration_ms=(
                round(usage[proxy.id]["duration_total"] / usage[proxy.id]["count"])
                if proxy.id in usage and usage[proxy.id]["count"]
                else 0
            ),
            last_seen_at=usage.get(proxy.id, {}).get("last_seen_at"),
        )
        for proxy in list_proxies(db)
    ]
    return templates.TemplateResponse(
        request=request,
        name="proxies.html",
        context={
            "proxies": proxies,
            "browser_proxy_slots": browser_proxy_slots,
            "crawler_proxy_slots": crawler_proxy_slots,
            "browser_thread_capacity": browser_thread_capacity,
            "crawler_thread_capacity": crawler_thread_capacity,
            "effective_browser_capacity": min(effective_proxy_capacity(db, ProxyKind.BROWSER), browser_thread_capacity),
            "effective_crawler_capacity": min(effective_proxy_capacity(db, ProxyKind.CRAWLER), crawler_thread_capacity),
        },
    )


@router.get("/metrics", response_class=HTMLResponse)
def request_metrics_view(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    summaries, slow_hosts, recent_errors, proxy_usage, signals = build_request_metric_views(db)
    return templates.TemplateResponse(
        request=request,
        name="metrics.html",
        context={
            "summaries": summaries,
            "slow_hosts": slow_hosts,
            "recent_errors": recent_errors,
            "proxy_usage": proxy_usage,
            "signals": signals,
        },
    )


@router.post("/proxies", response_class=HTMLResponse)
def create_proxy_html(
    label: str = Form(...),
    proxy_url: str = Form(...),
    supports_http: str | None = Form(None),
    supports_browser: str | None = Form(None),
    max_http_leases: int = Form(8),
    max_browser_leases: int = Form(1),
    is_active: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    supports_http_enabled = supports_http == "1"
    supports_browser_enabled = supports_browser == "1"
    if not supports_http_enabled and not supports_browser_enabled:
        supports_http_enabled = True
    kind = ProxyKind.BROWSER if supports_browser_enabled else ProxyKind.CRAWLER
    upsert_proxy(
        db,
        label=label.strip(),
        proxy_url=proxy_url.strip(),
        kind=kind,
        supports_http=supports_http_enabled,
        supports_browser=supports_browser_enabled,
        max_http_leases=max_http_leases,
        max_browser_leases=max_browser_leases,
        is_active=is_active == "1",
        notes=(notes or "").strip() or None,
    )
    db.commit()
    return RedirectResponse(url="/proxies", status_code=303)


@router.post("/proxies/{proxy_id}/toggle", response_class=HTMLResponse)
def toggle_proxy_html(
    proxy_id: int,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    proxy = db.get(ProxyEndpoint, proxy_id)
    if proxy is not None:
        proxy.is_active = not proxy.is_active
        if not proxy.is_active:
            release_proxy(db, proxy.id, record_result=False)
        else:
            proxy.auto_disabled_at = None
            proxy.cooldown_until = None
            proxy.consecutive_failures = 0
        db.add(proxy)
        db.commit()
    return RedirectResponse(url="/proxies", status_code=303)


@router.post("/regions/sync", response_class=HTMLResponse)
def sync_regions_html() -> RedirectResponse:
    sync_region_catalog_task.send()
    return RedirectResponse(url="/regions", status_code=303)


@router.post("/categories", response_class=HTMLResponse)
def create_category_html(
    slug: str = Form(...),
    label: str = Form(...),
    vertical: str = Form(...),
    osm_tags: str = Form(...),
    search_terms: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    tag_pairs = []
    for row in osm_tags.splitlines():
        if "=" not in row:
            continue
        key, value = row.split("=", 1)
        tag_pairs.append({key.strip(): value.strip()})
    terms = [item.strip() for item in search_terms.split(",") if item.strip()]
    db.add(Category(slug=slug, label=label, vertical=Vertical(vertical), osm_tags=tag_pairs, search_terms=terms))
    db.commit()
    return RedirectResponse(url="/categories", status_code=303)


@router.post("/regions", response_class=HTMLResponse)
def create_region_html(
    code: str = Form(...),
    name: str = Form(...),
    country_code: str = Form(...),
    osm_admin_level: int = Form(2),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    selected_country_code = country_code.upper()
    upsert_country_with_subdivisions(db, selected_country_code, is_active=True)
    return RedirectResponse(url="/regions", status_code=303)
