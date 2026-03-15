from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import median
from types import SimpleNamespace
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
from app.models import Category, Company, ContactChannel, ContactChannelType, Email, NicheCluster, Phone, ProxyEndpoint, ProxyKind, QueryRecipe, QueryRecipePlanVariantOutcome, QueryRecipeRecommendationPolicy, QueryRecipeRecommendationPolicyAudit, QueryRecipeValidation, QueryRecipeVariant, QueryRecipeVariantRunStat, QueryRecipeVariantTemplate, QueryRecipeVersion, RecipeAdapter, RecipeSourceStrategy, RecipeStatus, Region, RequestMetric, RunCategory, RunStatus, ScrapeRun, TaxonomyVertical, ValidationStatus
from app.schemas import EmailRow
from app.services.category_recipes import latest_recipe_version, sync_recipe_to_category, upsert_recipe_backed_category
from app.services.host_suppression import normalize_host_key
from app.services.overpass import fetch_status
from app.services.proxy_pool import active_proxy_count, effective_proxy_capacity, lease_counts, list_proxies, release_proxy, upsert_proxy
from app.services.recipe_clusters import record_cluster_decision
from app.services.recipe_drafts import ClusterCandidate, DraftProposal
from app.services.recipe_lint import RecipeLintResult, lint_recipe_content, parse_tag_block
from app.services.recipe_planner import plan_recipe_prompt
from app.services.recipe_prompt_variants import (
    record_plan_variant_activation,
    record_plan_variant_decisions,
    record_prompt_variant_activation,
    record_prompt_variant_decisions,
    sync_plan_variant_outcomes,
)
from app.services.taxonomy import list_active_clusters, list_active_verticals
from app.services.recipe_validation import get_validation_quota_snapshot, validate_recipe_version
from app.services.recipe_variants import (
    derive_recommendation_state,
    prompt_fingerprint,
    prompt_variant_recipe_map,
    recommendation_policy_map,
    resolve_recommendation_policy,
    upsert_prompt_variants,
)
from app.services.region_catalog import country_catalog, upsert_country_with_subdivisions
from app.services.runs import find_active_run, request_run_cancellation
from app.tasks import run_scrape, sync_region_catalog_task


templates = Jinja2Templates(directory="app/templates")
router = APIRouter(tags=["ui"])
RECENT_RUNS_PAGE_SIZE = 25
settings = get_settings()
RECIPE_PLANNER_PROVIDER_OPTIONS = ("heuristic", "openai")


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


@dataclass
class RecipeRow:
    id: int
    slug: str
    label: str
    vertical: str
    cluster_slug: str | None
    source_strategy: str | None
    sub_intent: str | None
    source_template_key: str | None
    status: str
    adapter: str | None
    version_number: int | None
    validation_count: int
    latest_score: int | None
    latest_validation_status: str | None
    production_run_count: int
    production_score: int
    production_discovered_total: int
    production_crawled_total: int
    production_email_company_total: int
    production_phone_company_total: int
    latest_total_results: int | None
    latest_website_rate: float | None
    last_validated_at: datetime | None
    last_production_at: datetime | None
    cache_expires_at: datetime | None
    sampled_regions: list[str]
    lint_passed: bool
    lint_errors: list[str]
    lint_warnings: list[str]
    linked_category_label: str | None
    linked_category_active: bool
    source_variant_key: str | None
    source_variant_prompt: str | None
    recommendation_state: str
    recommendation_state_score: int
    recommendation_reasons: list[str]
    recommendation_policy_key: str
    recommendation_policy_label: str
    recommendation_blockers: list[str]
    created_at: datetime


@dataclass
class CategoryRow:
    id: int
    label: str
    slug: str
    vertical: str
    osm_tags: list[dict[str, str]]
    search_terms: list[str]
    is_active: bool
    linked_recipe_slug: str | None
    linked_recipe_status: str | None
    linked_recipe_adapter: str | None
    linked_recipe_source_strategy: str | None
    linked_recipe_version: int | None
    linked_recipe_template: bool


@dataclass
class RecipeStrategyAnalyticsRow:
    source_strategy: str
    template_count: int
    active_recipe_count: int
    variant_count: int
    avg_validation_score: int
    avg_production_score: int
    avg_rank_score: int


@dataclass
class RecipeClusterAnalyticsRow:
    cluster_slug: str
    variant_count: int
    active_recipe_count: int
    avg_validation_score: int
    avg_production_score: int
    avg_rank_score: int


@dataclass
class RecipeMarketAnalyticsRow:
    country_code: str
    run_count: int
    variant_count: int
    avg_score: int


@dataclass
class RecipeStrategyMarketAnalyticsRow:
    country_code: str
    source_strategy: str
    run_count: int
    avg_score: int


@dataclass
class RecipeTopVariantRow:
    label: str
    cluster_slug: str | None
    source_strategy: str
    template_key: str | None
    rank_score: int
    validation_score: int
    production_score: int
    production_runs: int


@dataclass
class RecommendationPolicyRow:
    policy_key: str
    label: str
    source_strategy: str
    recommended_validation_score: int
    recommended_validation_runs: int
    recommended_production_score: int
    recommended_production_runs: int
    recommended_activation_count: int
    trusted_validation_score: int
    trusted_validation_runs: int
    trusted_production_score: int
    trusted_production_runs: int
    trusted_activation_count: int
    suppression_validation_score_max: int
    suppression_validation_runs_min: int
    suppression_production_score_max: int
    suppression_production_runs_min: int
    is_active: bool


@dataclass
class RecommendationPolicySimulationRow:
    policy_key: str
    policy_label: str
    current_state_mix: dict[str, int]
    simulated_state_mix: dict[str, int]
    suggested_thresholds: dict[str, int]
    form_values: dict[str, object]
    impact_parts: list[str]
    summary: str


@dataclass
class RecommendationPolicyAuditRow:
    policy_key: str
    policy_label: str
    change_kind: str
    change_summary: str
    experiment_note: str | None
    changed_at: datetime
    before_json: dict[str, object]
    after_json: dict[str, object]
    snapshot_json: dict[str, object]
    current_json: dict[str, object]
    delta_parts: list[str]
    before_window_json: dict[str, object]
    after_window_json: dict[str, object]
    window_delta_parts: list[str]


@dataclass
class PlannerVariantCompareRow:
    variant_key: str
    label: str
    status: str
    cluster_slug: str | None
    selected_rank: int | None
    selected_score: int | None
    selected_template_score: int | None
    selected_prompt_score: int | None
    heuristic_rank: int | None
    heuristic_score: int | None
    heuristic_template_score: int | None
    heuristic_prompt_score: int | None
    score_delta: int | None
    selected_historical_selected: int
    selected_historical_drafted: int
    selected_historical_activated: int
    heuristic_historical_selected: int
    heuristic_historical_drafted: int
    heuristic_historical_activated: int


@dataclass
class PlannerConversionSummaryRow:
    planner_label: str
    provider: str
    model_name: str
    plan_count: int
    variant_rows: int
    selected_count: int
    drafted_count: int
    activated_count: int
    selected_rate: int
    drafted_rate: int
    activated_rate: int


def taxonomy_context(db: Session) -> tuple[list[TaxonomyVertical], list[NicheCluster]]:
    return list_active_verticals(db), list_active_clusters(db)


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


def build_recipe_rows(db: Session) -> list[RecipeRow]:
    recipes = db.scalars(select(QueryRecipe).order_by(QueryRecipe.vertical, QueryRecipe.label)).all()
    policy_map = recommendation_policy_map(db)
    rows: list[RecipeRow] = []
    for recipe in recipes:
        version = recipe.versions[0] if recipe.versions else None
        latest_validation = version.validations[0] if version and version.validations else None
        linked_category = db.scalar(
            select(Category).where(
                (Category.seeded_recipe_id == recipe.id) | (Category.slug == recipe.slug)
            ).limit(1)
        )
        lint_result = (
            lint_recipe_content(
                osm_tags=version.osm_tags,
                exclude_tags=version.exclude_tags,
                search_terms=version.search_terms,
                website_keywords=version.website_keywords,
            )
            if version
            else RecipeLintResult(False, ["Recipe has no version."], [])
        )
        recommendation_state = "experimental"
        recommendation_state_score = 0
        recommendation_reasons = ["No source variant is linked yet."]
        recommendation_policy_key = "global"
        recommendation_policy_label = "Global Baseline"
        recommendation_blockers = ["No source variant is linked yet."]
        if recipe.source_variant and version:
            policy = resolve_recommendation_policy(policy_map, version.source_strategy) or version.source_strategy
            recommendation = derive_recommendation_state(
                source_strategy=policy,
                observed_validation_score=recipe.source_variant.observed_validation_score,
                historical_validation_count=recipe.source_variant.validation_count,
                production_score=recipe.source_variant.observed_production_score,
                production_run_count=recipe.source_variant.production_run_count,
                planner_selection_count=recipe.source_variant.planner_selection_count,
                planner_draft_count=recipe.source_variant.planner_draft_count,
                planner_activation_count=recipe.source_variant.planner_activation_count,
                prompt_selection_count=recipe.source_variant.prompt_selection_count,
                prompt_draft_count=recipe.source_variant.prompt_draft_count,
                prompt_activation_count=recipe.source_variant.prompt_activation_count,
                market_production_score=0,
                market_production_run_count=0,
                strategy_production_score=0,
                strategy_production_run_count=0,
            )
            recommendation_state = recommendation.state
            recommendation_state_score = recommendation.score
            recommendation_reasons = recommendation.reasons
            recommendation_policy_key = recommendation.policy_key
            recommendation_policy_label = recommendation.policy_label
            recommendation_blockers = recommendation.blockers
        rows.append(
            RecipeRow(
                id=recipe.id,
                slug=recipe.slug,
                label=recipe.label,
                vertical=recipe.vertical,
                cluster_slug=recipe.cluster_slug,
                source_strategy=version.source_strategy.value if version else None,
                sub_intent=recipe.source_variant.sub_intent if recipe.source_variant else None,
                source_template_key=recipe.source_variant.template_key if recipe.source_variant else None,
                status=recipe.status.value,
                adapter=version.adapter.value if version else None,
                version_number=version.version_number if version else None,
                validation_count=len(version.validations) if version else 0,
                latest_score=latest_validation.score if latest_validation else None,
                latest_validation_status=version.status.value if version else None,
                production_run_count=recipe.source_variant.production_run_count if recipe.source_variant else 0,
                production_score=recipe.source_variant.observed_production_score if recipe.source_variant else 0,
                production_discovered_total=recipe.source_variant.production_discovered_total if recipe.source_variant else 0,
                production_crawled_total=recipe.source_variant.production_crawled_total if recipe.source_variant else 0,
                production_email_company_total=recipe.source_variant.production_email_company_total if recipe.source_variant else 0,
                production_phone_company_total=recipe.source_variant.production_phone_company_total if recipe.source_variant else 0,
                latest_total_results=(
                    latest_validation.metrics_json.get("total_results")
                    if latest_validation and latest_validation.metrics_json
                    else None
                ),
                latest_website_rate=(
                    latest_validation.metrics_json.get("website_rate")
                    if latest_validation and latest_validation.metrics_json
                    else None
                ),
                last_validated_at=latest_validation.created_at if latest_validation else None,
                last_production_at=recipe.source_variant.last_production_at if recipe.source_variant else None,
                cache_expires_at=latest_validation.expires_at if latest_validation else None,
                sampled_regions=latest_validation.sample_regions if latest_validation else [],
                lint_passed=lint_result.passed,
                lint_errors=lint_result.errors,
                lint_warnings=lint_result.warnings,
                linked_category_label=linked_category.label if linked_category else None,
                linked_category_active=linked_category.is_active if linked_category else False,
                source_variant_key=recipe.source_variant.variant_key if recipe.source_variant else None,
                source_variant_prompt=recipe.source_variant.prompt_text if recipe.source_variant else None,
                recommendation_state=recommendation_state,
                recommendation_state_score=recommendation_state_score,
                recommendation_reasons=recommendation_reasons,
                recommendation_policy_key=recommendation_policy_key,
                recommendation_policy_label=recommendation_policy_label,
                recommendation_blockers=recommendation_blockers,
                created_at=recipe.created_at,
            )
        )
    return rows


def build_recommendation_policy_rows(db: Session) -> list[RecommendationPolicyRow]:
    rows = recommendation_policy_map(db)
    ordered_keys = ["global"] + [strategy.value for strategy in RecipeSourceStrategy]
    result: list[RecommendationPolicyRow] = []
    for key in ordered_keys:
        policy = rows.get(key)
        if policy is None:
            continue
        result.append(
            RecommendationPolicyRow(
                policy_key=policy.policy_key,
                label=policy.label,
                source_strategy=policy.source_strategy.value if policy.source_strategy else "global",
                recommended_validation_score=policy.recommended_validation_score,
                recommended_validation_runs=policy.recommended_validation_runs,
                recommended_production_score=policy.recommended_production_score,
                recommended_production_runs=policy.recommended_production_runs,
                recommended_activation_count=policy.recommended_activation_count,
                trusted_validation_score=policy.trusted_validation_score,
                trusted_validation_runs=policy.trusted_validation_runs,
                trusted_production_score=policy.trusted_production_score,
                trusted_production_runs=policy.trusted_production_runs,
                trusted_activation_count=policy.trusted_activation_count,
                suppression_validation_score_max=policy.suppression_validation_score_max,
                suppression_validation_runs_min=policy.suppression_validation_runs_min,
                suppression_production_score_max=policy.suppression_production_score_max,
                suppression_production_runs_min=policy.suppression_production_runs_min,
                is_active=policy.is_active,
            )
        )
    return result


def _percentile_int(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    if len(ordered) == 1:
        return int(ordered[0])
    position = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * percentile)))
    return int(ordered[position])


def recommendation_policy_state_distribution(
    variants: list[QueryRecipeVariant],
    policy: QueryRecipeRecommendationPolicy,
) -> dict[str, int]:
    counts = {
        "trusted": 0,
        "recommended": 0,
        "experimental": 0,
        "suppressed": 0,
    }
    for variant in variants:
        recommendation = derive_recommendation_state(
            source_strategy=policy,
            observed_validation_score=max(0, variant.observed_validation_score or 0),
            historical_validation_count=max(0, variant.validation_count or 0),
            production_score=max(0, variant.observed_production_score or 0),
            production_run_count=max(0, variant.production_run_count or 0),
            planner_selection_count=max(0, variant.planner_selection_count or 0),
            planner_draft_count=max(0, variant.planner_draft_count or 0),
            planner_activation_count=max(0, variant.planner_activation_count or 0),
            prompt_selection_count=max(0, variant.prompt_selection_count or 0),
            prompt_draft_count=max(0, variant.prompt_draft_count or 0),
            prompt_activation_count=max(0, variant.prompt_activation_count or 0),
            market_production_score=max(0, variant.market_production_score or 0),
            market_production_run_count=max(0, variant.market_production_run_count or 0),
            strategy_production_score=max(0, variant.strategy_production_score or 0),
            strategy_production_run_count=max(0, variant.strategy_production_run_count or 0),
        )
        counts[recommendation.state] = counts.get(recommendation.state, 0) + 1
    return counts


def recommendation_policy_performance_snapshot(
    db: Session,
    policy: QueryRecipeRecommendationPolicy,
) -> dict[str, object]:
    variant_query = select(QueryRecipeVariant)
    if policy.source_strategy is not None:
        variant_query = variant_query.where(QueryRecipeVariant.source_strategy == policy.source_strategy)
    variants = db.scalars(variant_query).all()
    if not variants:
        return {
            "variant_count": 0,
            "avg_validation_score": 0,
            "avg_production_score": 0,
            "avg_rank_score": 0,
            "trusted_count": 0,
            "recommended_count": 0,
            "suppressed_count": 0,
        }

    validation_scores = [max(0, variant.observed_validation_score or 0) for variant in variants]
    production_scores = [max(0, variant.observed_production_score or 0) for variant in variants]
    rank_scores = [max(0, variant.rank_score or 0) for variant in variants]
    trusted_count = 0
    recommended_count = 0
    suppressed_count = 0
    for variant in variants:
        recommendation = derive_recommendation_state(
            source_strategy=policy,
            observed_validation_score=max(0, variant.observed_validation_score or 0),
            historical_validation_count=max(0, variant.validation_count or 0),
            production_score=max(0, variant.observed_production_score or 0),
            production_run_count=max(0, variant.production_run_count or 0),
            planner_selection_count=max(0, variant.planner_selection_count or 0),
            planner_draft_count=max(0, variant.planner_draft_count or 0),
            planner_activation_count=max(0, variant.planner_activation_count or 0),
            prompt_selection_count=max(0, variant.prompt_selection_count or 0),
            prompt_draft_count=max(0, variant.prompt_draft_count or 0),
            prompt_activation_count=max(0, variant.prompt_activation_count or 0),
            market_production_score=max(0, variant.market_production_score or 0),
            market_production_run_count=max(0, variant.market_production_run_count or 0),
            strategy_production_score=max(0, variant.strategy_production_score or 0),
            strategy_production_run_count=max(0, variant.strategy_production_run_count or 0),
        )
        if recommendation.state == "trusted":
            trusted_count += 1
        elif recommendation.state == "recommended":
            recommended_count += 1
        elif recommendation.state == "suppressed":
            suppressed_count += 1

    return {
        "variant_count": len(variants),
        "avg_validation_score": round(sum(validation_scores) / len(validation_scores)),
        "avg_production_score": round(sum(production_scores) / len(production_scores)),
        "avg_rank_score": round(sum(rank_scores) / len(rank_scores)),
        "trusted_count": trusted_count,
        "recommended_count": recommended_count,
        "suppressed_count": suppressed_count,
    }


def build_recommendation_policy_simulation_rows(db: Session) -> list[RecommendationPolicySimulationRow]:
    policy_rows = recommendation_policy_map(db)
    ordered_keys = ["global"] + [strategy.value for strategy in RecipeSourceStrategy]
    rows: list[RecommendationPolicySimulationRow] = []
    for key in ordered_keys:
        policy = policy_rows.get(key)
        if policy is None:
            continue
        variant_query = select(QueryRecipeVariant)
        if policy.source_strategy is not None:
            variant_query = variant_query.where(QueryRecipeVariant.source_strategy == policy.source_strategy)
        variants = db.scalars(variant_query).all()
        if not variants:
            rows.append(
                RecommendationPolicySimulationRow(
                    policy_key=policy.policy_key,
                    policy_label=policy.label,
                    current_state_mix={"trusted": 0, "recommended": 0, "experimental": 0, "suppressed": 0},
                    simulated_state_mix={"trusted": 0, "recommended": 0, "experimental": 0, "suppressed": 0},
                    suggested_thresholds={},
                    form_values={},
                    impact_parts=["No variants yet"],
                    summary="No recipe variants exist for this policy scope yet.",
                )
            )
            continue

        validation_scores = [max(0, variant.observed_validation_score or 0) for variant in variants if (variant.validation_count or 0) > 0]
        production_scores = [max(0, variant.observed_production_score or 0) for variant in variants if (variant.production_run_count or 0) > 0]
        activation_counts = [
            max(0, (variant.planner_activation_count or 0) + (variant.prompt_activation_count or 0))
            for variant in variants
        ]

        suggested_recommended_validation = max(
            25,
            min(95, _percentile_int(validation_scores, 0.55) if validation_scores else policy.recommended_validation_score),
        )
        suggested_trusted_validation = max(
            suggested_recommended_validation + 5,
            min(100, _percentile_int(validation_scores, 0.8) if validation_scores else policy.trusted_validation_score),
        )
        suggested_recommended_production = max(
            5,
            min(95, _percentile_int(production_scores, 0.55) if production_scores else policy.recommended_production_score),
        )
        suggested_trusted_production = max(
            suggested_recommended_production + 5,
            min(100, _percentile_int(production_scores, 0.8) if production_scores else policy.trusted_production_score),
        )
        suggested_suppression_validation = max(
            0,
            min(suggested_recommended_validation - 5, _percentile_int(validation_scores, 0.2) if validation_scores else policy.suppression_validation_score_max),
        )
        suggested_suppression_production = max(
            0,
            min(suggested_recommended_production - 3, _percentile_int(production_scores, 0.2) if production_scores else policy.suppression_production_score_max),
        )
        suggested_recommended_activations = max(0, min(3, round(median(activation_counts)) if activation_counts else policy.recommended_activation_count))
        suggested_trusted_activations = max(
            suggested_recommended_activations + 1,
            min(5, _percentile_int(activation_counts, 0.75) if activation_counts else policy.trusted_activation_count),
        )

        simulated_policy = SimpleNamespace(
            policy_key=policy.policy_key,
            label=policy.label + " (simulated)",
            source_strategy=policy.source_strategy,
            recommended_validation_score=suggested_recommended_validation,
            recommended_validation_runs=policy.recommended_validation_runs,
            recommended_production_score=suggested_recommended_production,
            recommended_production_runs=policy.recommended_production_runs,
            recommended_activation_count=suggested_recommended_activations,
            trusted_validation_score=suggested_trusted_validation,
            trusted_validation_runs=policy.trusted_validation_runs,
            trusted_production_score=suggested_trusted_production,
            trusted_production_runs=policy.trusted_production_runs,
            trusted_activation_count=suggested_trusted_activations,
            suppression_validation_score_max=suggested_suppression_validation,
            suppression_validation_runs_min=policy.suppression_validation_runs_min,
            suppression_production_score_max=suggested_suppression_production,
            suppression_production_runs_min=policy.suppression_production_runs_min,
            is_active=policy.is_active,
        )

        current_mix = recommendation_policy_state_distribution(variants, policy)
        simulated_mix = recommendation_policy_state_distribution(variants, simulated_policy)
        impact_parts = [
            f"{state} {simulated_mix.get(state, 0) - current_mix.get(state, 0):+d}"
            for state in ("trusted", "recommended", "experimental", "suppressed")
            if simulated_mix.get(state, 0) != current_mix.get(state, 0)
        ]
        if not impact_parts:
            impact_parts = ["No state change"]

        summary_parts: list[str] = []
        if simulated_mix.get("trusted", 0) > current_mix.get("trusted", 0):
            summary_parts.append("Loosens promotion enough to trust more variants.")
        if simulated_mix.get("suppressed", 0) < current_mix.get("suppressed", 0):
            summary_parts.append("Reduces unnecessary suppression.")
        if simulated_mix.get("recommended", 0) < current_mix.get("recommended", 0):
            summary_parts.append("Tightens recommendations around stronger-performing variants.")
        summary = " ".join(summary_parts) if summary_parts else "Current thresholds are already close to observed performance."

        rows.append(
            RecommendationPolicySimulationRow(
                policy_key=policy.policy_key,
                policy_label=policy.label,
                current_state_mix=current_mix,
                simulated_state_mix=simulated_mix,
                suggested_thresholds={
                    "recommended_validation_score": suggested_recommended_validation,
                    "recommended_production_score": suggested_recommended_production,
                    "recommended_activation_count": suggested_recommended_activations,
                    "trusted_validation_score": suggested_trusted_validation,
                    "trusted_production_score": suggested_trusted_production,
                    "trusted_activation_count": suggested_trusted_activations,
                    "suppression_validation_score_max": suggested_suppression_validation,
                    "suppression_production_score_max": suggested_suppression_production,
                },
                form_values={
                    "label": policy.label,
                    "is_active": "true" if policy.is_active else "false",
                    "recommended_validation_score": suggested_recommended_validation,
                    "recommended_validation_runs": policy.recommended_validation_runs,
                    "recommended_production_score": suggested_recommended_production,
                    "recommended_production_runs": policy.recommended_production_runs,
                    "recommended_activation_count": suggested_recommended_activations,
                    "trusted_validation_score": suggested_trusted_validation,
                    "trusted_validation_runs": policy.trusted_validation_runs,
                    "trusted_production_score": suggested_trusted_production,
                    "trusted_production_runs": policy.trusted_production_runs,
                    "trusted_activation_count": suggested_trusted_activations,
                    "suppression_validation_score_max": suggested_suppression_validation,
                    "suppression_validation_runs_min": policy.suppression_validation_runs_min,
                    "suppression_production_score_max": suggested_suppression_production,
                    "suppression_production_runs_min": policy.suppression_production_runs_min,
                },
                impact_parts=impact_parts,
                summary=summary,
            )
        )
    return rows


def recommendation_policy_window_snapshot(
    db: Session,
    policy: QueryRecipeRecommendationPolicy,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, object]:
    row = db.execute(
        select(
            func.count(QueryRecipeVariantRunStat.id),
            func.avg(QueryRecipeVariantRunStat.score),
            func.sum(QueryRecipeVariantRunStat.discovered_count),
            func.sum(QueryRecipeVariantRunStat.crawled_count),
            func.sum(QueryRecipeVariantRunStat.contact_company_count),
            func.sum(QueryRecipeVariantRunStat.email_company_count),
            func.sum(QueryRecipeVariantRunStat.phone_company_count),
        )
        .select_from(QueryRecipeVariantRunStat)
        .join(QueryRecipeVariant, QueryRecipeVariant.id == QueryRecipeVariantRunStat.variant_id)
        .where(QueryRecipeVariantRunStat.created_at >= start_at)
        .where(QueryRecipeVariantRunStat.created_at < end_at)
        .where(
            QueryRecipeVariant.source_strategy == policy.source_strategy
            if policy.source_strategy is not None
            else True
        )
    ).one()
    run_count = int(row[0] or 0)
    avg_score = round(row[1] or 0)
    discovered_total = int(row[2] or 0)
    crawled_total = int(row[3] or 0)
    contact_total = int(row[4] or 0)
    email_total = int(row[5] or 0)
    phone_total = int(row[6] or 0)
    crawl_rate = round((crawled_total / discovered_total) * 100) if discovered_total else 0
    contact_rate = round((contact_total / discovered_total) * 100) if discovered_total else 0
    email_rate = round((email_total / discovered_total) * 100) if discovered_total else 0
    phone_rate = round((phone_total / discovered_total) * 100) if discovered_total else 0
    return {
        "run_count": run_count,
        "avg_score": avg_score,
        "discovered_total": discovered_total,
        "crawl_rate": crawl_rate,
        "contact_rate": contact_rate,
        "email_rate": email_rate,
        "phone_rate": phone_rate,
    }


def build_recommendation_policy_audit_rows(db: Session, limit: int = 20) -> list[RecommendationPolicyAuditRow]:
    policy_rows = recommendation_policy_map(db)
    audits = db.scalars(
        select(QueryRecipeRecommendationPolicyAudit)
        .order_by(QueryRecipeRecommendationPolicyAudit.changed_at.desc())
        .limit(limit)
    ).all()
    current_snapshots = {
        policy_key: recommendation_policy_performance_snapshot(db, policy)
        for policy_key, policy in policy_rows.items()
    }
    rows: list[RecommendationPolicyAuditRow] = []
    window_days = 7
    now = datetime.now(timezone.utc)
    for audit in audits:
        policy = policy_rows.get(audit.policy_key)
        if policy is None:
            continue
        before_window = recommendation_policy_window_snapshot(
            db,
            policy,
            audit.changed_at - timedelta(days=window_days),
            audit.changed_at,
        )
        after_window = recommendation_policy_window_snapshot(
            db,
            policy,
            audit.changed_at,
            min(audit.changed_at + timedelta(days=window_days), now),
        )
        current_snapshot = current_snapshots.get(audit.policy_key, {})
        rows.append(
            RecommendationPolicyAuditRow(
                policy_key=audit.policy_key,
                policy_label=audit.policy_label,
                change_kind=getattr(audit, "change_kind", "manual"),
                change_summary=audit.change_summary,
                experiment_note=getattr(audit, "experiment_note", None),
                changed_at=audit.changed_at,
                before_json=audit.before_json or {},
                after_json=audit.after_json or {},
                snapshot_json=audit.performance_snapshot_json or {},
                current_json=current_snapshot,
                delta_parts=[
                    f"{key} {current_snapshot.get(key, 0) - int((audit.performance_snapshot_json or {}).get(key, 0)):+d}"
                    for key in ("avg_validation_score", "avg_production_score", "avg_rank_score", "trusted_count", "recommended_count", "suppressed_count")
                ],
                before_window_json=before_window,
                after_window_json=after_window,
                window_delta_parts=[
                    f"{key} {int(after_window.get(key, 0)) - int(before_window.get(key, 0)):+d}"
                    for key in ("run_count", "avg_score", "crawl_rate", "contact_rate", "email_rate", "phone_rate")
                ],
            )
        )
    return rows


def create_recommendation_policy_audit(
    db: Session,
    *,
    policy: QueryRecipeRecommendationPolicy,
    before_state: dict[str, object],
    after_state: dict[str, object],
    change_summary: str,
    change_kind: str,
    experiment_note: str | None = None,
) -> None:
    snapshot = recommendation_policy_performance_snapshot(db, policy)
    db.add(
        QueryRecipeRecommendationPolicyAudit(
            policy_key=policy.policy_key,
            policy_label=policy.label,
            change_kind=change_kind,
            change_summary=change_summary,
            experiment_note=experiment_note,
            before_json=before_state,
            after_json=after_state,
            performance_snapshot_json=snapshot,
        )
    )


def build_category_rows(db: Session) -> list[CategoryRow]:
    categories = db.scalars(select(Category).order_by(Category.vertical, Category.label)).all()
    rows: list[CategoryRow] = []
    for category in categories:
        recipe = category.seeded_recipe
        if recipe is None:
            recipe = db.scalar(select(QueryRecipe).where(QueryRecipe.slug == category.slug).limit(1))
        version = latest_recipe_version(recipe)
        rows.append(
            CategoryRow(
                id=category.id,
                label=category.label,
                slug=category.slug,
                vertical=category.vertical,
                osm_tags=category.osm_tags,
                search_terms=category.search_terms,
                is_active=category.is_active,
                linked_recipe_slug=recipe.slug if recipe else None,
                linked_recipe_status=recipe.status.value if recipe else None,
                linked_recipe_adapter=version.adapter.value if version else None,
                linked_recipe_source_strategy=version.source_strategy.value if version else None,
                linked_recipe_version=version.version_number if version else None,
                linked_recipe_template=recipe.is_platform_template if recipe else False,
            )
        )
    return rows


def build_recipe_analytics(
    db: Session,
) -> tuple[
    list[RecipeStrategyAnalyticsRow],
    list[RecipeClusterAnalyticsRow],
    list[RecipeMarketAnalyticsRow],
    list[RecipeStrategyMarketAnalyticsRow],
    list[RecipeTopVariantRow],
]:
    strategy_rows_raw = db.execute(
        select(
            QueryRecipeVariant.source_strategy,
            func.count(QueryRecipeVariant.id),
            func.avg(QueryRecipeVariant.observed_validation_score),
            func.avg(QueryRecipeVariant.observed_production_score),
            func.avg(QueryRecipeVariant.rank_score),
        )
        .group_by(QueryRecipeVariant.source_strategy)
        .order_by(QueryRecipeVariant.source_strategy)
    ).all()
    active_recipe_counts_by_strategy = {
        source_strategy: count_value
        for source_strategy, count_value in db.execute(
            select(QueryRecipeVersion.source_strategy, func.count(QueryRecipe.id))
            .select_from(QueryRecipe)
            .join(QueryRecipeVersion, QueryRecipeVersion.recipe_id == QueryRecipe.id)
            .where(QueryRecipe.status == RecipeStatus.ACTIVE)
            .group_by(QueryRecipeVersion.source_strategy)
        ).all()
    }
    template_counts_by_strategy = {
        source_strategy: count_value
        for source_strategy, count_value in db.execute(
            select(QueryRecipeVariantTemplate.source_strategy, func.count(QueryRecipeVariantTemplate.id))
            .where(QueryRecipeVariantTemplate.is_active.is_(True))
            .group_by(QueryRecipeVariantTemplate.source_strategy)
        ).all()
    }
    strategy_rows = [
        RecipeStrategyAnalyticsRow(
            source_strategy=source_strategy.value if isinstance(source_strategy, RecipeSourceStrategy) else str(source_strategy),
            template_count=template_counts_by_strategy.get(source_strategy, 0),
            active_recipe_count=active_recipe_counts_by_strategy.get(source_strategy, 0),
            variant_count=int(variant_count or 0),
            avg_validation_score=round(avg_validation or 0),
            avg_production_score=round(avg_production or 0),
            avg_rank_score=round(avg_rank or 0),
        )
        for source_strategy, variant_count, avg_validation, avg_production, avg_rank in strategy_rows_raw
    ]

    cluster_rows = [
        RecipeClusterAnalyticsRow(
            cluster_slug=cluster_slug or "-",
            variant_count=int(variant_count or 0),
            active_recipe_count=int(active_recipe_count or 0),
            avg_validation_score=round(avg_validation or 0),
            avg_production_score=round(avg_production or 0),
            avg_rank_score=round(avg_rank or 0),
        )
        for cluster_slug, variant_count, active_recipe_count, avg_validation, avg_production, avg_rank in db.execute(
            select(
                QueryRecipeVariant.cluster_slug,
                func.count(QueryRecipeVariant.id),
                func.count(QueryRecipe.id),
                func.avg(QueryRecipeVariant.observed_validation_score),
                func.avg(QueryRecipeVariant.observed_production_score),
                func.avg(QueryRecipeVariant.rank_score),
            )
            .select_from(QueryRecipeVariant)
            .outerjoin(QueryRecipe, QueryRecipe.source_variant_id == QueryRecipeVariant.id)
            .group_by(QueryRecipeVariant.cluster_slug)
            .order_by(desc(func.avg(QueryRecipeVariant.observed_production_score)), QueryRecipeVariant.cluster_slug)
        ).all()
        if cluster_slug
    ]

    market_rows = [
        RecipeMarketAnalyticsRow(
            country_code=country_code,
            run_count=int(run_count or 0),
            variant_count=int(variant_count or 0),
            avg_score=round(avg_score or 0),
        )
        for country_code, run_count, variant_count, avg_score in db.execute(
            select(
                Region.country_code,
                func.count(QueryRecipeVariantRunStat.id),
                func.count(func.distinct(QueryRecipeVariantRunStat.variant_id)),
                func.avg(QueryRecipeVariantRunStat.score),
            )
            .select_from(QueryRecipeVariantRunStat)
            .join(Region, Region.id == QueryRecipeVariantRunStat.region_id)
            .group_by(Region.country_code)
            .order_by(desc(func.avg(QueryRecipeVariantRunStat.score)), Region.country_code)
        ).all()
        if country_code
    ]

    strategy_market_rows = [
        RecipeStrategyMarketAnalyticsRow(
            country_code=country_code,
            source_strategy=source_strategy.value if isinstance(source_strategy, RecipeSourceStrategy) else str(source_strategy),
            run_count=int(run_count or 0),
            avg_score=round(avg_score or 0),
        )
        for country_code, source_strategy, run_count, avg_score in db.execute(
            select(
                Region.country_code,
                QueryRecipeVariant.source_strategy,
                func.count(QueryRecipeVariantRunStat.id),
                func.avg(QueryRecipeVariantRunStat.score),
            )
            .select_from(QueryRecipeVariantRunStat)
            .join(QueryRecipeVariant, QueryRecipeVariant.id == QueryRecipeVariantRunStat.variant_id)
            .join(Region, Region.id == QueryRecipeVariantRunStat.region_id)
            .group_by(Region.country_code, QueryRecipeVariant.source_strategy)
            .order_by(desc(func.avg(QueryRecipeVariantRunStat.score)), Region.country_code, QueryRecipeVariant.source_strategy)
        ).all()
        if country_code
    ]

    top_variants = [
        RecipeTopVariantRow(
            label=variant.label,
            cluster_slug=variant.cluster_slug,
            source_strategy=variant.source_strategy.value,
            template_key=variant.template_key,
            rank_score=variant.rank_score,
            validation_score=variant.observed_validation_score,
            production_score=variant.observed_production_score,
            production_runs=variant.production_run_count,
        )
        for variant in db.scalars(
            select(QueryRecipeVariant)
            .order_by(
                desc(QueryRecipeVariant.observed_production_score),
                desc(QueryRecipeVariant.observed_validation_score),
                desc(QueryRecipeVariant.rank_score),
            )
            .limit(8)
        ).all()
    ]

    return strategy_rows, cluster_rows[:8], market_rows[:8], strategy_market_rows[:12], top_variants


def source_strategy_thresholds(source_strategy: RecipeSourceStrategy | None) -> dict[str, int]:
    thresholds = {
        "validation_score": settings.recipe_activation_min_validation_score,
        "validation_runs": settings.recipe_activation_min_validation_runs,
        "production_score": settings.recipe_activation_min_production_score,
        "production_runs": settings.recipe_activation_min_production_runs,
    }
    if source_strategy is None:
        return thresholds

    overrides: dict[RecipeSourceStrategy, dict[str, int]] = {
        RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH: {
            "validation_score": 55,
            "validation_runs": 1,
            "production_score": 0,
            "production_runs": 0,
        },
        RecipeSourceStrategy.HYBRID_DISCOVERY: {
            "validation_score": 58,
            "validation_runs": 1,
            "production_score": 5,
            "production_runs": 1,
        },
        RecipeSourceStrategy.WEBSITE_FIRST: {
            "validation_score": 52,
            "validation_runs": 1,
            "production_score": 10,
            "production_runs": 1,
        },
        RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY: {
            "validation_score": 65,
            "validation_runs": 1,
            "production_score": 15,
            "production_runs": 1,
        },
        RecipeSourceStrategy.DIRECTORY_EXPANSION: {
            "validation_score": 60,
            "validation_runs": 1,
            "production_score": 10,
            "production_runs": 1,
        },
    }
    for key, value in overrides.get(source_strategy, {}).items():
        thresholds[key] = max(thresholds[key], value)
    return thresholds


def build_strategy_threshold_rows() -> list[dict[str, object]]:
    return [
        {
            "source_strategy": strategy.value,
            **source_strategy_thresholds(strategy),
        }
        for strategy in RecipeSourceStrategy
    ]


def build_planner_info(plan) -> dict[str, object]:
    return {
        "requested_provider": plan.requested_provider,
        "requested_model": plan.requested_model,
        "provider": plan.provider,
        "model_name": plan.model_name,
        "planner_version": plan.planner_version,
        "cache_hit": plan.cache_hit,
        "cache_expires_at": plan.cache_expires_at,
        "used_fallback": plan.used_fallback,
        "fallback_reason": plan.fallback_reason,
        "plan_id": plan.plan_id,
        "variant_count": len(plan.draft_variants),
        "default_variant_key": plan.draft_proposal.variant_key,
        "top_variants": [variant.label for variant in plan.draft_variants[:3]],
        "cluster_slug": plan.cluster_choice.cluster_slug,
        "cluster_score": plan.cluster_choice.score,
    }


def build_variant_compare_rows(
    db: Session,
    selected_provider: str,
    selected_model: str,
    selected_variants: list[DraftProposal],
    heuristic_provider: str,
    heuristic_model: str,
    heuristic_variants: list[DraftProposal],
) -> list[PlannerVariantCompareRow]:
    selected_by_key = {variant.variant_key: (index + 1, variant) for index, variant in enumerate(selected_variants)}
    heuristic_by_key = {variant.variant_key: (index + 1, variant) for index, variant in enumerate(heuristic_variants)}
    all_keys = set(selected_by_key) | set(heuristic_by_key)

    selected_history = {
        variant_key: (selected_count, drafted_count, activated_count)
        for variant_key, selected_count, drafted_count, activated_count in db.execute(
            select(
                QueryRecipePlanVariantOutcome.variant_key,
                func.count(case((QueryRecipePlanVariantOutcome.was_selected.is_(True), 1))).label("selected_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_drafted.is_(True), 1))).label("drafted_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_activated.is_(True), 1))).label("activated_count"),
            )
            .where(
                QueryRecipePlanVariantOutcome.provider == selected_provider,
                QueryRecipePlanVariantOutcome.model_name == selected_model,
                QueryRecipePlanVariantOutcome.variant_key.in_(list(all_keys)),
            )
            .group_by(QueryRecipePlanVariantOutcome.variant_key)
        ).all()
    }
    heuristic_history = {
        variant_key: (selected_count, drafted_count, activated_count)
        for variant_key, selected_count, drafted_count, activated_count in db.execute(
            select(
                QueryRecipePlanVariantOutcome.variant_key,
                func.count(case((QueryRecipePlanVariantOutcome.was_selected.is_(True), 1))).label("selected_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_drafted.is_(True), 1))).label("drafted_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_activated.is_(True), 1))).label("activated_count"),
            )
            .where(
                QueryRecipePlanVariantOutcome.provider == heuristic_provider,
                QueryRecipePlanVariantOutcome.model_name == heuristic_model,
                QueryRecipePlanVariantOutcome.variant_key.in_(list(all_keys)),
            )
            .group_by(QueryRecipePlanVariantOutcome.variant_key)
        ).all()
    }

    rows: list[PlannerVariantCompareRow] = []
    for key in all_keys:
        selected_entry = selected_by_key.get(key)
        heuristic_entry = heuristic_by_key.get(key)
        selected_variant = selected_entry[1] if selected_entry else None
        heuristic_variant = heuristic_entry[1] if heuristic_entry else None
        if selected_variant and heuristic_variant:
            status = "shared"
        elif selected_variant:
            status = "selected_only"
        else:
            status = "heuristic_only"

        score_delta = None
        if selected_variant is not None and heuristic_variant is not None:
            score_delta = selected_variant.fit_score - heuristic_variant.fit_score
        selected_counts = selected_history.get(key, (0, 0, 0))
        heuristic_counts = heuristic_history.get(key, (0, 0, 0))

        rows.append(
            PlannerVariantCompareRow(
                variant_key=key,
                label=(selected_variant or heuristic_variant).label,
                status=status,
                cluster_slug=(selected_variant or heuristic_variant).cluster_slug,
                selected_rank=selected_entry[0] if selected_entry else None,
                selected_score=selected_variant.fit_score if selected_variant else None,
                selected_template_score=selected_variant.template_score if selected_variant else None,
                selected_prompt_score=selected_variant.prompt_match_score if selected_variant else None,
                heuristic_rank=heuristic_entry[0] if heuristic_entry else None,
                heuristic_score=heuristic_variant.fit_score if heuristic_variant else None,
                heuristic_template_score=heuristic_variant.template_score if heuristic_variant else None,
                heuristic_prompt_score=heuristic_variant.prompt_match_score if heuristic_variant else None,
                score_delta=score_delta,
                selected_historical_selected=int(selected_counts[0] or 0),
                selected_historical_drafted=int(selected_counts[1] or 0),
                selected_historical_activated=int(selected_counts[2] or 0),
                heuristic_historical_selected=int(heuristic_counts[0] or 0),
                heuristic_historical_drafted=int(heuristic_counts[1] or 0),
                heuristic_historical_activated=int(heuristic_counts[2] or 0),
            )
        )

    status_order = {"shared": 0, "selected_only": 1, "heuristic_only": 2}
    rows.sort(
        key=lambda row: (
            status_order.get(row.status, 9),
            -(row.selected_score if row.selected_score is not None else row.heuristic_score or 0),
            row.label,
        )
    )
    return rows


def build_planner_conversion_summary(
    db: Session,
    planner_label: str,
    provider: str,
    model_name: str,
) -> PlannerConversionSummaryRow:
    plan_count, variant_rows, selected_count, drafted_count, activated_count = db.execute(
        select(
            func.count(func.distinct(QueryRecipePlanVariantOutcome.plan_id)),
            func.count(QueryRecipePlanVariantOutcome.id),
            func.count(case((QueryRecipePlanVariantOutcome.was_selected.is_(True), 1))),
            func.count(case((QueryRecipePlanVariantOutcome.was_drafted.is_(True), 1))),
            func.count(case((QueryRecipePlanVariantOutcome.was_activated.is_(True), 1))),
        ).where(
            QueryRecipePlanVariantOutcome.provider == provider,
            QueryRecipePlanVariantOutcome.model_name == model_name,
        )
    ).one()
    row_count = int(variant_rows or 0)

    def rate(value: int) -> int:
        if row_count <= 0:
            return 0
        return round((value / row_count) * 100)

    selected_total = int(selected_count or 0)
    drafted_total = int(drafted_count or 0)
    activated_total = int(activated_count or 0)
    return PlannerConversionSummaryRow(
        planner_label=planner_label,
        provider=provider,
        model_name=model_name,
        plan_count=int(plan_count or 0),
        variant_rows=row_count,
        selected_count=selected_total,
        drafted_count=drafted_total,
        activated_count=activated_total,
        selected_rate=rate(selected_total),
        drafted_rate=rate(drafted_total),
        activated_rate=rate(activated_total),
    )


def activation_gate_errors(recipe: QueryRecipe, version: QueryRecipeVersion) -> list[str]:
    errors: list[str] = []
    thresholds = source_strategy_thresholds(version.source_strategy)
    latest_validation = version.validations[0] if version.validations else None
    if latest_validation is None:
        errors.append("No validation run exists yet.")
        return errors
    if latest_validation.status != RecipeStatus.VALIDATED:
        errors.append("Latest validation did not reach validated status.")
    if (latest_validation.score or 0) < thresholds["validation_score"]:
        errors.append(
            f"Validation score must be at least {thresholds['validation_score']} for {version.source_strategy.value}."
        )
    if len(version.validations) < thresholds["validation_runs"]:
        errors.append(
            f"At least {thresholds['validation_runs']} validation run(s) are required for {version.source_strategy.value}."
        )
    if thresholds["production_runs"] > 0:
        variant = recipe.source_variant
        if variant is None:
            errors.append("Recipe has no source variant to evaluate production readiness.")
        else:
            if variant.production_run_count < thresholds["production_runs"]:
                errors.append(
                    f"At least {thresholds['production_runs']} production run(s) are required for {version.source_strategy.value}."
                )
            if variant.observed_production_score < thresholds["production_score"]:
                errors.append(
                    f"Production yield must be at least {thresholds['production_score']} for {version.source_strategy.value}."
                )
    return errors


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
def category_editor(
    request: Request,
    message: str | None = None,
    error: str | None = None,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    verticals, clusters = taxonomy_context(db)
    return templates.TemplateResponse(
        request=request,
        name="categories.html",
        context={
            "categories": build_category_rows(db),
            "verticals": verticals,
            "clusters": clusters,
            "message": message,
            "error": error,
        },
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


@router.get("/recipes", response_class=HTMLResponse)
def recipe_editor(
    request: Request,
    message: str | None = None,
    error: str | None = None,
    draft_prompt: str | None = None,
    draft_variant_slug: str | None = None,
    planner_provider: str | None = None,
    planner_model: str | None = None,
    compare_with_heuristic: bool = False,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    draft_proposal = None
    draft_variants: list[DraftProposal] = []
    draft_lint = None
    cluster_choice: ClusterCandidate | None = None
    alternate_clusters: list[ClusterCandidate] = []
    planner_info: dict[str, object] | None = None
    planner_compare_info: dict[str, object] | None = None
    planner_variant_compare_rows: list[PlannerVariantCompareRow] = []
    planner_conversion_rows: list[PlannerConversionSummaryRow] = []
    verticals, clusters = taxonomy_context(db)
    strategy_rows, cluster_rows, market_rows, strategy_market_rows, top_variants = build_recipe_analytics(db)
    strategy_threshold_rows = build_strategy_threshold_rows()
    recommendation_policy_rows = build_recommendation_policy_rows(db)
    recommendation_policy_simulation_rows = build_recommendation_policy_simulation_rows(db)
    recommendation_policy_audit_rows = build_recommendation_policy_audit_rows(db)
    if draft_prompt:
        try:
            plan = plan_recipe_prompt(
                db,
                draft_prompt,
                selected_variant_slug=draft_variant_slug,
                requested_provider=planner_provider,
                requested_model=planner_model,
            )
            cluster_choice = plan.cluster_choice
            alternate_clusters = plan.alternate_clusters
            draft_variants = plan.draft_variants
            draft_proposal = plan.draft_proposal
            planner_info = build_planner_info(plan)
            saved_variants = upsert_prompt_variants(db, draft_prompt, draft_variants)
            sync_plan_variant_outcomes(db, plan, saved_variants)
            if compare_with_heuristic and plan.requested_provider != "heuristic":
                compare_plan = plan_recipe_prompt(
                    db,
                    draft_prompt,
                    selected_variant_slug=draft_variant_slug,
                    requested_provider="heuristic",
                )
                planner_compare_info = build_planner_info(compare_plan)
                planner_variant_compare_rows = build_variant_compare_rows(
                    db,
                    plan.provider,
                    plan.model_name,
                    plan.draft_variants,
                    compare_plan.provider,
                    compare_plan.model_name,
                    compare_plan.draft_variants,
                )
                planner_conversion_rows = [
                    build_planner_conversion_summary(db, "Selected", plan.provider, plan.model_name),
                    build_planner_conversion_summary(db, "Heuristic", compare_plan.provider, compare_plan.model_name),
                ]
                compare_saved_variants = upsert_prompt_variants(db, draft_prompt, compare_plan.draft_variants)
                sync_plan_variant_outcomes(db, compare_plan, compare_saved_variants)
            db.commit()
            draft_lint = lint_recipe_content(
                osm_tags=draft_proposal.osm_tags,
                exclude_tags=draft_proposal.exclude_tags,
                search_terms=draft_proposal.search_terms,
                website_keywords=draft_proposal.website_keywords,
            )
        except (ValueError, RuntimeError) as exc:
            error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="recipes.html",
        context={
            "recipes": build_recipe_rows(db),
            "verticals": verticals,
            "clusters": clusters,
            "recipe_adapters": list(RecipeAdapter),
            "validation_quota": get_validation_quota_snapshot(db),
            "message": message,
            "error": error,
            "draft_proposal": draft_proposal,
            "draft_variants": draft_variants,
            "variant_recipe_map": prompt_variant_recipe_map(db, draft_prompt or "") if draft_prompt else {},
            "draft_lint": draft_lint,
            "draft_prompt": draft_prompt or "",
            "planner_provider": plan.requested_provider if draft_prompt and not error else (planner_provider or settings.recipe_planner_provider),
            "planner_model": plan.requested_model if draft_prompt and not error else (
                planner_model or (
                    settings.recipe_planner_openai_model
                    if (planner_provider or settings.recipe_planner_provider).strip().lower() == "openai"
                    else settings.recipe_planner_model
                )
            ),
            "planner_provider_options": RECIPE_PLANNER_PROVIDER_OPTIONS,
            "compare_with_heuristic": compare_with_heuristic,
            "planner_info": planner_info,
            "planner_compare_info": planner_compare_info,
            "planner_variant_compare_rows": planner_variant_compare_rows,
            "planner_conversion_rows": planner_conversion_rows,
            "cluster_choice": cluster_choice,
            "alternate_clusters": alternate_clusters,
        "strategy_rows": strategy_rows,
        "cluster_rows": cluster_rows,
        "market_rows": market_rows,
        "strategy_market_rows": strategy_market_rows,
            "top_variants": top_variants,
            "recipe_source_strategies": list(RecipeSourceStrategy),
            "strategy_activation_thresholds": strategy_threshold_rows,
            "recommendation_policy_rows": recommendation_policy_rows,
            "recommendation_policy_simulation_rows": recommendation_policy_simulation_rows,
            "recommendation_policy_audit_rows": recommendation_policy_audit_rows,
            "activation_thresholds": {
                "validation_score": settings.recipe_activation_min_validation_score,
                "validation_runs": settings.recipe_activation_min_validation_runs,
                "production_score": settings.recipe_activation_min_production_score,
                "production_runs": settings.recipe_activation_min_production_runs,
            },
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


@router.post("/recipes", response_class=HTMLResponse)
def create_recipe_html(
    slug: str = Form(...),
    label: str = Form(...),
    vertical: str = Form(...),
    cluster_slug: str = Form(""),
    description: str = Form(""),
    adapter: RecipeAdapter = Form(RecipeAdapter.OVERPASS_PUBLIC),
    source_strategy: RecipeSourceStrategy = Form(RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH),
    osm_tags: str = Form(""),
    exclude_tags: str = Form(""),
    search_terms: str = Form(""),
    website_keywords: str = Form(""),
    language_hints: str = Form(""),
    draft_prompt: str = Form(""),
    source_variant_key: str = Form(""),
    source_plan_id: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    normalized_slug = slug.strip().lower()
    existing = db.scalar(select(QueryRecipe).where(QueryRecipe.slug == normalized_slug))
    tag_pairs, tag_errors = parse_tag_block(osm_tags)
    exclude_pairs, exclude_errors = parse_tag_block(exclude_tags)
    if tag_errors or exclude_errors:
        joined = "; ".join(tag_errors + exclude_errors)
        return RedirectResponse(url=f"/recipes?error={quote_plus(joined[:200])}", status_code=303)
    normalized_source_plan_id = int(source_plan_id) if source_plan_id.strip().isdigit() else None
    source_variant = None
    if draft_prompt.strip() and source_variant_key.strip():
        source_variant = db.scalar(
            select(QueryRecipeVariant).where(
                QueryRecipeVariant.prompt_fingerprint == prompt_fingerprint(draft_prompt),
                QueryRecipeVariant.variant_key == source_variant_key.strip(),
            )
        )
    if existing is None:
        recipe = QueryRecipe(
            slug=normalized_slug,
            label=label.strip(),
            description=description.strip() or None,
            vertical=vertical,
            cluster_slug=cluster_slug.strip() or None,
            source_variant_id=source_variant.id if source_variant is not None else None,
            source_plan_id=normalized_source_plan_id,
            status=RecipeStatus.DRAFT,
            is_platform_template=True,
        )
        db.add(recipe)
        db.flush()
        term_list = [item.strip() for item in search_terms.split(",") if item.strip()]
        keyword_list = [item.strip() for item in website_keywords.split(",") if item.strip()]
        language_list = [item.strip() for item in language_hints.split(",") if item.strip()]
        db.add(
            QueryRecipeVersion(
                recipe_id=recipe.id,
                version_number=1,
                status=RecipeStatus.DRAFT,
                adapter=adapter,
                source_strategy=source_strategy,
                osm_tags=tag_pairs,
                exclude_tags=exclude_pairs,
                search_terms=term_list,
                website_keywords=keyword_list,
                language_hints=language_list,
                notes="Draft recipe created from the recipes console.",
            )
        )
        if draft_prompt.strip() and source_variant is not None:
            record_prompt_variant_decisions(
                db,
                draft_prompt,
                {source_variant.variant_key: source_variant},
                selected_variant_keys=[source_variant.variant_key],
                drafted_variant_keys=[source_variant.variant_key],
            )
            record_plan_variant_decisions(
                db,
                normalized_source_plan_id,
                selected_variant_keys=[source_variant.variant_key],
                drafted_variant_keys=[source_variant.variant_key],
            )
        db.commit()
    return RedirectResponse(url="/recipes", status_code=303)


@router.post("/recipes/bulk-from-prompt", response_class=HTMLResponse)
def create_recipe_variants_html(
    prompt: str = Form(...),
    selected_variant_keys: list[str] = Form([]),
    planner_provider: str | None = Form(None),
    planner_model: str | None = Form(None),
    compare_with_heuristic: bool = Form(False),
    plan_id: int | None = Form(None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    try:
        plan = plan_recipe_prompt(
            db,
            prompt,
            requested_provider=planner_provider,
            requested_model=planner_model,
        )
        proposals = plan.draft_variants
    except (ValueError, RuntimeError) as exc:
        return RedirectResponse(url=f"/recipes?error={quote_plus(str(exc)[:200])}", status_code=303)
    saved_variants = upsert_prompt_variants(db, prompt, proposals)
    sync_plan_variant_outcomes(db, plan, saved_variants)

    if not selected_variant_keys:
        db.commit()
        return RedirectResponse(
            url=(
                "/recipes?"
                f"error=Select%20at%20least%20one%20variant."
                f"&draft_prompt={quote_plus(prompt)}"
                f"&planner_provider={quote_plus(plan.requested_provider)}"
                f"&planner_model={quote_plus(plan.requested_model)}"
                f"&compare_with_heuristic={'true' if compare_with_heuristic else 'false'}"
            ),
            status_code=303,
        )

    selected = [proposal for proposal in proposals if proposal.variant_key in set(selected_variant_keys)]
    if not selected:
        db.commit()
        return RedirectResponse(
            url=(
                "/recipes?"
                f"error=Selected%20variants%20were%20not%20found."
                f"&draft_prompt={quote_plus(prompt)}"
                f"&planner_provider={quote_plus(plan.requested_provider)}"
                f"&planner_model={quote_plus(plan.requested_model)}"
                f"&compare_with_heuristic={'true' if compare_with_heuristic else 'false'}"
            ),
            status_code=303,
        )

    created = 0
    skipped: list[str] = []
    drafted_variant_keys: list[str] = []
    for proposal in selected:
        existing = db.scalar(select(QueryRecipe).where(QueryRecipe.slug == proposal.slug))
        if existing is not None:
            skipped.append(proposal.slug)
            continue
        recipe = QueryRecipe(
            slug=proposal.slug,
            label=proposal.label,
            description=proposal.description or None,
            vertical=proposal.vertical,
            cluster_slug=proposal.cluster_slug,
            source_variant_id=saved_variants[proposal.variant_key].id,
            source_plan_id=plan_id or plan.plan_id,
            status=RecipeStatus.DRAFT,
            is_platform_template=True,
        )
        db.add(recipe)
        db.flush()
        db.add(
            QueryRecipeVersion(
                recipe_id=recipe.id,
                version_number=1,
                status=RecipeStatus.DRAFT,
                adapter=proposal.adapter,
                source_strategy=proposal.source_strategy,
                osm_tags=proposal.osm_tags,
                exclude_tags=proposal.exclude_tags,
                search_terms=proposal.search_terms,
                website_keywords=proposal.website_keywords,
                language_hints=proposal.language_hints,
                notes=f"Draft recipe created from prompt '{proposal.prompt}'.",
            )
        )
        created += 1
        drafted_variant_keys.append(proposal.variant_key)

    record_prompt_variant_decisions(
        db,
        prompt,
        saved_variants,
        selected_variant_keys=[proposal.variant_key for proposal in selected],
        drafted_variant_keys=drafted_variant_keys,
    )
    record_plan_variant_decisions(
        db,
        plan_id or plan.plan_id,
        selected_variant_keys=[proposal.variant_key for proposal in selected],
        drafted_variant_keys=drafted_variant_keys,
    )

    db.commit()

    if created and skipped:
        message = f"Created {created} draft recipes. Skipped existing slugs: {', '.join(skipped[:5])}"
        if len(skipped) > 5:
            message += f" and {len(skipped) - 5} more."
        return RedirectResponse(
            url=(
                f"/recipes?message={quote_plus(message)}"
                f"&draft_prompt={quote_plus(prompt)}"
                f"&planner_provider={quote_plus(plan.requested_provider)}"
                f"&planner_model={quote_plus(plan.requested_model)}"
                f"&compare_with_heuristic={'true' if compare_with_heuristic else 'false'}"
            ),
            status_code=303,
        )
    if created:
        return RedirectResponse(
            url=(
                f"/recipes?message={quote_plus(f'Created {created} draft recipes.')}"
                f"&draft_prompt={quote_plus(prompt)}"
                f"&planner_provider={quote_plus(plan.requested_provider)}"
                f"&planner_model={quote_plus(plan.requested_model)}"
                f"&compare_with_heuristic={'true' if compare_with_heuristic else 'false'}"
            ),
            status_code=303,
        )
    return RedirectResponse(
        url=(
            f"/recipes?error={quote_plus('All selected variants already exist.')}"
            f"&draft_prompt={quote_plus(prompt)}"
            f"&planner_provider={quote_plus(plan.requested_provider)}"
            f"&planner_model={quote_plus(plan.requested_model)}"
            f"&compare_with_heuristic={'true' if compare_with_heuristic else 'false'}"
        ),
        status_code=303,
    )


@router.post("/recipes/draft", response_class=HTMLResponse)
def generate_recipe_draft_html(
    request: Request,
    prompt: str = Form(...),
    selected_variant_slug: str = Form(""),
    planner_provider: str | None = Form(None),
    planner_model: str | None = Form(None),
    compare_with_heuristic: bool = Form(False),
    plan_id: int | None = Form(None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    draft_proposal = None
    draft_variants: list[DraftProposal] = []
    error = None
    draft_lint = None
    cluster_choice: ClusterCandidate | None = None
    alternate_clusters: list[ClusterCandidate] = []
    planner_info: dict[str, object] | None = None
    planner_compare_info: dict[str, object] | None = None
    planner_variant_compare_rows: list[PlannerVariantCompareRow] = []
    planner_conversion_rows: list[PlannerConversionSummaryRow] = []
    verticals, clusters = taxonomy_context(db)
    strategy_rows, cluster_rows, market_rows, strategy_market_rows, top_variants = build_recipe_analytics(db)
    strategy_threshold_rows = build_strategy_threshold_rows()
    recommendation_policy_rows = build_recommendation_policy_rows(db)
    recommendation_policy_simulation_rows = build_recommendation_policy_simulation_rows(db)
    recommendation_policy_audit_rows = build_recommendation_policy_audit_rows(db)
    try:
        plan = plan_recipe_prompt(
            db,
            prompt,
            selected_variant_slug=selected_variant_slug or None,
            requested_provider=planner_provider,
            requested_model=planner_model,
        )
        cluster_choice = plan.cluster_choice
        alternate_clusters = plan.alternate_clusters
        draft_variants = plan.draft_variants
        draft_proposal = plan.draft_proposal
        planner_info = build_planner_info(plan)
        saved_variants = upsert_prompt_variants(db, prompt, draft_variants)
        sync_plan_variant_outcomes(db, plan, saved_variants)
        if compare_with_heuristic and plan.requested_provider != "heuristic":
            compare_plan = plan_recipe_prompt(
                db,
                prompt,
                selected_variant_slug=selected_variant_slug or None,
                requested_provider="heuristic",
            )
            planner_compare_info = build_planner_info(compare_plan)
            planner_variant_compare_rows = build_variant_compare_rows(
                db,
                plan.provider,
                plan.model_name,
                plan.draft_variants,
                compare_plan.provider,
                compare_plan.model_name,
                compare_plan.draft_variants,
            )
            planner_conversion_rows = [
                build_planner_conversion_summary(db, "Selected", plan.provider, plan.model_name),
                build_planner_conversion_summary(db, "Heuristic", compare_plan.provider, compare_plan.model_name),
            ]
            compare_saved_variants = upsert_prompt_variants(db, prompt, compare_plan.draft_variants)
            sync_plan_variant_outcomes(db, compare_plan, compare_saved_variants)
        record_cluster_decision(db, prompt, cluster_choice, alternate_clusters)
        if selected_variant_slug:
            record_prompt_variant_decisions(
                db,
                prompt,
                saved_variants,
                selected_variant_keys=[draft_proposal.variant_key],
            )
            record_plan_variant_decisions(
                db,
                plan_id or plan.plan_id,
                selected_variant_keys=[draft_proposal.variant_key],
            )
        db.commit()
        draft_lint = lint_recipe_content(
            osm_tags=draft_proposal.osm_tags,
            exclude_tags=draft_proposal.exclude_tags,
            search_terms=draft_proposal.search_terms,
            website_keywords=draft_proposal.website_keywords,
        )
    except (ValueError, RuntimeError) as exc:
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="recipes.html",
        context={
            "recipes": build_recipe_rows(db),
            "verticals": verticals,
            "clusters": clusters,
            "recipe_adapters": list(RecipeAdapter),
            "recipe_source_strategies": list(RecipeSourceStrategy),
            "validation_quota": get_validation_quota_snapshot(db),
            "message": None,
            "error": error,
            "draft_proposal": draft_proposal,
            "draft_variants": draft_variants,
            "variant_recipe_map": prompt_variant_recipe_map(db, prompt),
            "draft_lint": draft_lint,
            "draft_prompt": prompt,
            "planner_provider": planner_info["requested_provider"] if planner_info else (planner_provider or settings.recipe_planner_provider),
            "planner_model": planner_info["requested_model"] if planner_info else (
                planner_model or (
                    settings.recipe_planner_openai_model
                    if (planner_provider or settings.recipe_planner_provider).strip().lower() == "openai"
                    else settings.recipe_planner_model
                )
            ),
            "planner_provider_options": RECIPE_PLANNER_PROVIDER_OPTIONS,
            "compare_with_heuristic": compare_with_heuristic,
            "planner_info": planner_info,
            "planner_compare_info": planner_compare_info,
            "planner_variant_compare_rows": planner_variant_compare_rows,
            "planner_conversion_rows": planner_conversion_rows,
            "cluster_choice": cluster_choice,
            "alternate_clusters": alternate_clusters,
        "strategy_rows": strategy_rows,
        "cluster_rows": cluster_rows,
        "market_rows": market_rows,
            "strategy_market_rows": strategy_market_rows,
            "top_variants": top_variants,
            "strategy_activation_thresholds": strategy_threshold_rows,
            "recommendation_policy_rows": recommendation_policy_rows,
            "recommendation_policy_simulation_rows": recommendation_policy_simulation_rows,
            "recommendation_policy_audit_rows": recommendation_policy_audit_rows,
            "activation_thresholds": {
                "validation_score": settings.recipe_activation_min_validation_score,
                "validation_runs": settings.recipe_activation_min_validation_runs,
                "production_score": settings.recipe_activation_min_production_score,
                "production_runs": settings.recipe_activation_min_production_runs,
            },
        },
    )


@router.post("/recipes/{recipe_id}/validate", response_class=HTMLResponse)
def validate_recipe_html(
    recipe_id: int,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    try:
        recipe = db.get(QueryRecipe, recipe_id)
        version = recipe.versions[0] if recipe and recipe.versions else None
        if version is None:
            return RedirectResponse(url="/recipes?error=Recipe%20has%20no%20version%20to%20lint.", status_code=303)
        lint_result = lint_recipe_content(
            osm_tags=version.osm_tags,
            exclude_tags=version.exclude_tags,
            search_terms=version.search_terms,
            website_keywords=version.website_keywords,
        )
        if not lint_result.passed:
            return RedirectResponse(
                url=f"/recipes?error={quote_plus('Lint failed: ' + '; '.join(lint_result.errors)[:180])}",
                status_code=303,
            )
        validation, cache_hit = validate_recipe_version(db, recipe_id)
        score = validation.score if validation.score is not None else "-"
        result_message = (
            f"Validation {'cache hit' if cache_hit else 'completed'}: "
            f"score {score}, status {validation.status.value}."
        )
        return RedirectResponse(url=f"/recipes?message={quote_plus(result_message)}", status_code=303)
    except Exception as exc:
        db.rollback()
        return RedirectResponse(url=f"/recipes?error={quote_plus(str(exc)[:200])}", status_code=303)


@router.post("/recipes/recommendation-policies/{policy_key}", response_class=HTMLResponse)
def update_recommendation_policy_html(
    policy_key: str,
    label: str = Form(...),
    recommended_validation_score: int = Form(...),
    recommended_validation_runs: int = Form(...),
    recommended_production_score: int = Form(...),
    recommended_production_runs: int = Form(...),
    recommended_activation_count: int = Form(...),
    trusted_validation_score: int = Form(...),
    trusted_validation_runs: int = Form(...),
    trusted_production_score: int = Form(...),
    trusted_production_runs: int = Form(...),
    trusted_activation_count: int = Form(...),
    suppression_validation_score_max: int = Form(...),
    suppression_validation_runs_min: int = Form(...),
    suppression_production_score_max: int = Form(...),
    suppression_production_runs_min: int = Form(...),
    is_active: str = Form("true"),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    policy = recommendation_policy_map(db).get(policy_key)
    if policy is None:
        return RedirectResponse(url="/recipes?error=Recommendation%20policy%20not%20found.", status_code=303)

    before_state = {
        "is_active": policy.is_active,
        "recommended_validation_score": policy.recommended_validation_score,
        "recommended_validation_runs": policy.recommended_validation_runs,
        "recommended_production_score": policy.recommended_production_score,
        "recommended_production_runs": policy.recommended_production_runs,
        "recommended_activation_count": policy.recommended_activation_count,
        "trusted_validation_score": policy.trusted_validation_score,
        "trusted_validation_runs": policy.trusted_validation_runs,
        "trusted_production_score": policy.trusted_production_score,
        "trusted_production_runs": policy.trusted_production_runs,
        "trusted_activation_count": policy.trusted_activation_count,
        "suppression_validation_score_max": policy.suppression_validation_score_max,
        "suppression_validation_runs_min": policy.suppression_validation_runs_min,
        "suppression_production_score_max": policy.suppression_production_score_max,
        "suppression_production_runs_min": policy.suppression_production_runs_min,
    }

    policy.label = label.strip() or policy.label
    policy.recommended_validation_score = max(0, min(100, recommended_validation_score))
    policy.recommended_validation_runs = max(0, recommended_validation_runs)
    policy.recommended_production_score = max(0, min(100, recommended_production_score))
    policy.recommended_production_runs = max(0, recommended_production_runs)
    policy.recommended_activation_count = max(0, recommended_activation_count)
    policy.trusted_validation_score = max(0, min(100, trusted_validation_score))
    policy.trusted_validation_runs = max(0, trusted_validation_runs)
    policy.trusted_production_score = max(0, min(100, trusted_production_score))
    policy.trusted_production_runs = max(0, trusted_production_runs)
    policy.trusted_activation_count = max(0, trusted_activation_count)
    policy.suppression_validation_score_max = max(0, min(100, suppression_validation_score_max))
    policy.suppression_validation_runs_min = max(0, suppression_validation_runs_min)
    policy.suppression_production_score_max = max(0, min(100, suppression_production_score_max))
    policy.suppression_production_runs_min = max(0, suppression_production_runs_min)
    policy.is_active = is_active.strip().lower() == "true"

    after_state = {
        "is_active": policy.is_active,
        "recommended_validation_score": policy.recommended_validation_score,
        "recommended_validation_runs": policy.recommended_validation_runs,
        "recommended_production_score": policy.recommended_production_score,
        "recommended_production_runs": policy.recommended_production_runs,
        "recommended_activation_count": policy.recommended_activation_count,
        "trusted_validation_score": policy.trusted_validation_score,
        "trusted_validation_runs": policy.trusted_validation_runs,
        "trusted_production_score": policy.trusted_production_score,
        "trusted_production_runs": policy.trusted_production_runs,
        "trusted_activation_count": policy.trusted_activation_count,
        "suppression_validation_score_max": policy.suppression_validation_score_max,
        "suppression_validation_runs_min": policy.suppression_validation_runs_min,
        "suppression_production_score_max": policy.suppression_production_score_max,
        "suppression_production_runs_min": policy.suppression_production_runs_min,
    }
    changed_fields = [key for key, value in after_state.items() if before_state.get(key) != value]
    if changed_fields:
        create_recommendation_policy_audit(
            db,
            policy=policy,
            before_state=before_state,
            after_state=after_state,
            change_summary="Updated " + ", ".join(changed_fields[:5]) + ("..." if len(changed_fields) > 5 else ""),
            change_kind="manual",
        )
    db.commit()
    return RedirectResponse(
        url=f"/recipes?message={quote_plus(f'Updated recommendation policy {policy.label}.')}",
        status_code=303,
    )


@router.post("/recipes/recommendation-policies/{policy_key}/apply-suggestion", response_class=HTMLResponse)
def apply_recommendation_policy_suggestion_html(
    policy_key: str,
    label: str = Form(...),
    recommended_validation_score: int = Form(...),
    recommended_validation_runs: int = Form(...),
    recommended_production_score: int = Form(...),
    recommended_production_runs: int = Form(...),
    recommended_activation_count: int = Form(...),
    trusted_validation_score: int = Form(...),
    trusted_validation_runs: int = Form(...),
    trusted_production_score: int = Form(...),
    trusted_production_runs: int = Form(...),
    trusted_activation_count: int = Form(...),
    suppression_validation_score_max: int = Form(...),
    suppression_validation_runs_min: int = Form(...),
    suppression_production_score_max: int = Form(...),
    suppression_production_runs_min: int = Form(...),
    is_active: str = Form("true"),
    experiment_note: str = Form("Accepted suggested policy experiment."),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    policy = recommendation_policy_map(db).get(policy_key)
    if policy is None:
        return RedirectResponse(url="/recipes?error=Recommendation%20policy%20not%20found.", status_code=303)

    before_state = {
        "is_active": policy.is_active,
        "recommended_validation_score": policy.recommended_validation_score,
        "recommended_validation_runs": policy.recommended_validation_runs,
        "recommended_production_score": policy.recommended_production_score,
        "recommended_production_runs": policy.recommended_production_runs,
        "recommended_activation_count": policy.recommended_activation_count,
        "trusted_validation_score": policy.trusted_validation_score,
        "trusted_validation_runs": policy.trusted_validation_runs,
        "trusted_production_score": policy.trusted_production_score,
        "trusted_production_runs": policy.trusted_production_runs,
        "trusted_activation_count": policy.trusted_activation_count,
        "suppression_validation_score_max": policy.suppression_validation_score_max,
        "suppression_validation_runs_min": policy.suppression_validation_runs_min,
        "suppression_production_score_max": policy.suppression_production_score_max,
        "suppression_production_runs_min": policy.suppression_production_runs_min,
    }

    policy.label = label.strip() or policy.label
    policy.recommended_validation_score = max(0, min(100, recommended_validation_score))
    policy.recommended_validation_runs = max(0, recommended_validation_runs)
    policy.recommended_production_score = max(0, min(100, recommended_production_score))
    policy.recommended_production_runs = max(0, recommended_production_runs)
    policy.recommended_activation_count = max(0, recommended_activation_count)
    policy.trusted_validation_score = max(0, min(100, trusted_validation_score))
    policy.trusted_validation_runs = max(0, trusted_validation_runs)
    policy.trusted_production_score = max(0, min(100, trusted_production_score))
    policy.trusted_production_runs = max(0, trusted_production_runs)
    policy.trusted_activation_count = max(0, trusted_activation_count)
    policy.suppression_validation_score_max = max(0, min(100, suppression_validation_score_max))
    policy.suppression_validation_runs_min = max(0, suppression_validation_runs_min)
    policy.suppression_production_score_max = max(0, min(100, suppression_production_score_max))
    policy.suppression_production_runs_min = max(0, suppression_production_runs_min)
    policy.is_active = is_active.strip().lower() == "true"

    after_state = {
        "is_active": policy.is_active,
        "recommended_validation_score": policy.recommended_validation_score,
        "recommended_validation_runs": policy.recommended_validation_runs,
        "recommended_production_score": policy.recommended_production_score,
        "recommended_production_runs": policy.recommended_production_runs,
        "recommended_activation_count": policy.recommended_activation_count,
        "trusted_validation_score": policy.trusted_validation_score,
        "trusted_validation_runs": policy.trusted_validation_runs,
        "trusted_production_score": policy.trusted_production_score,
        "trusted_production_runs": policy.trusted_production_runs,
        "trusted_activation_count": policy.trusted_activation_count,
        "suppression_validation_score_max": policy.suppression_validation_score_max,
        "suppression_validation_runs_min": policy.suppression_validation_runs_min,
        "suppression_production_score_max": policy.suppression_production_score_max,
        "suppression_production_runs_min": policy.suppression_production_runs_min,
    }
    changed_fields = [key for key, value in after_state.items() if before_state.get(key) != value]
    if changed_fields:
        create_recommendation_policy_audit(
            db,
            policy=policy,
            before_state=before_state,
            after_state=after_state,
            change_summary="Accepted suggested experiment: " + ", ".join(changed_fields[:5]) + ("..." if len(changed_fields) > 5 else ""),
            change_kind="suggested_accept",
            experiment_note=(experiment_note or "").strip()[:255] or None,
        )
    db.commit()
    return RedirectResponse(
        url=f"/recipes?message={quote_plus(f'Applied suggested policy update for {policy.label}.')}",
        status_code=303,
    )


@router.post("/recipes/{recipe_id}/promote", response_class=HTMLResponse)
def promote_recipe_html(
    recipe_id: int,
    target_status: RecipeStatus = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    recipe = db.get(QueryRecipe, recipe_id)
    version = recipe.versions[0] if recipe and recipe.versions else None
    if recipe is None or version is None:
        return RedirectResponse(url="/recipes?error=Recipe%20not%20found.", status_code=303)

    lint_result = lint_recipe_content(
        osm_tags=version.osm_tags,
        exclude_tags=version.exclude_tags,
        search_terms=version.search_terms,
        website_keywords=version.website_keywords,
    )
    if not lint_result.passed:
        return RedirectResponse(
            url=f"/recipes?error={quote_plus('Cannot promote recipe with lint failures.')}",
            status_code=303,
        )

    allowed_targets = {RecipeStatus.CANDIDATE, RecipeStatus.ACTIVE, RecipeStatus.DEPRECATED}
    if target_status not in allowed_targets:
        return RedirectResponse(url="/recipes?error=Unsupported%20recipe%20transition.", status_code=303)

    if target_status == RecipeStatus.ACTIVE:
        gate_errors = activation_gate_errors(recipe, version)
        if gate_errors:
            return RedirectResponse(
                url=f"/recipes?error={quote_plus('Cannot activate recipe: ' + '; '.join(gate_errors)[:200])}",
                status_code=303,
            )
        record_prompt_variant_activation(db, recipe)
        record_plan_variant_activation(db, recipe)
        sync_recipe_to_category(db, recipe, version)

    recipe.status = target_status
    version.status = target_status
    db.add(recipe)
    db.add(version)
    db.commit()
    return RedirectResponse(
        url=f"/recipes?message={quote_plus(f'Recipe {recipe.slug} marked as {target_status.value}.')}",
        status_code=303,
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
    cluster_slug: str = Form(""),
    osm_tags: str = Form(...),
    search_terms: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    tag_pairs, tag_errors = parse_tag_block(osm_tags)
    if tag_errors:
        return RedirectResponse(
            url=f"/categories?error={quote_plus('; '.join(tag_errors)[:200])}",
            status_code=303,
        )
    terms = [item.strip() for item in search_terms.split(",") if item.strip()]
    category, recipe, version = upsert_recipe_backed_category(
        db,
        slug=slug,
        label=label,
        vertical=vertical,
        cluster_slug=cluster_slug.strip() or None,
        osm_tags=tag_pairs,
        search_terms=terms,
        description=f"Recipe created from category editor for {label.strip()}.",
        adapter=RecipeAdapter.OVERPASS_LOCAL,
        source_strategy=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH,
        notes="Created or updated from category editor.",
        recipe_status=RecipeStatus.ACTIVE,
    )
    db.commit()
    return RedirectResponse(
        url=f"/categories?message={quote_plus(f'Category {category.slug} synced to recipe version {version.version_number}.')}",
        status_code=303,
    )


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
