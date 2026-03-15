from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.models import RunStatus, ValidationStatus


class RegionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    code: str
    name: str
    country_code: str


class RegionCreate(BaseModel):
    code: str
    name: str
    country_code: str
    osm_admin_level: int = 2


class CategoryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    slug: str
    label: str
    vertical: str
    cluster_slug: str | None = None
    is_active: bool


class RunCreate(BaseModel):
    region_id: int
    category_ids: list[int]


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: RunStatus
    region_id: int
    started_at: datetime
    finished_at: datetime | None
    overpass_queries_used: int
    discovered_count: int
    crawled_count: int
    note: str | None


class EmailStatusUpdate(BaseModel):
    validation_status: ValidationStatus


class EmailRow(BaseModel):
    id: int
    email: str
    company_name: str
    company_city: str | None
    company_website: str | None
    company_phone_count: int
    company_latest_phone: str | None
    company_whatsapp_count: int
    company_latest_whatsapp: str | None
    company_telegram_count: int
    company_latest_telegram: str | None
    region_name: str
    validation_status: ValidationStatus
    suppression_status: str
    source_type: str
    source_page_url: str | None
    crawl_status: str
    has_contact_form: bool
    technical_metadata: dict


class CategoryCreate(BaseModel):
    slug: str
    label: str
    vertical: str
    cluster_slug: str | None = None
    osm_tags: list[dict[str, str]]
    search_terms: list[str]


class GoogleMapsResultIn(BaseModel):
    name: str | None = None
    keyword: str | None = None
    address: str | None = None
    phone: str | None = None
    website: str | None = None
    rating: str | float | int | None = None
    reviews: str | int | float | None = None
    place_id: str | None = None
    latitude: str | float | None = None
    longitude: str | float | None = None
    external_id: str | None = None


class GoogleMapsIngestRequest(BaseModel):
    region_id: int
    results: list[GoogleMapsResultIn]
    prompt_text: str | None = None
    category_id: int | None = None
    recipe_id: int | None = None
    recipe_version_id: int | None = None
    run_id: int | None = None
    provider: str | None = "maps_scraper_v3"
    materialize_companies: bool = False
    enqueue_crawl: bool = False


class GoogleMapsIngestResponse(BaseModel):
    source_job_id: int
    source_record_count: int
    query_count: int
    matched_company_count: int
    created_company_count: int
    queued_company_count: int


class EmailValidationCallback(BaseModel):
    """
    Payload sent by the emailvalidator microservice to
    POST /api/email-validation-results when a validation job completes.
    Mirrors the result dict produced by app.workers.tasks._run_pipeline.
    """
    email: str
    status: str  # valid | invalid | risky | unknown
    score: int = 0
    reasons: list[str] = []
    syntax_valid: bool = True
    domain_exists: bool | None = None
    mx_found: bool | None = None
    is_disposable: bool | None = None
    is_role_based: bool | None = None
    typo_suggestion: str | None = None
    smtp_verdict: str | None = None
    primary_mx: str | None = None
    validated_at: str | None = None
    completed_at: str | None = None
    request_id: str | None = None
