from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.models import RunStatus, ValidationStatus, Vertical


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
    vertical: Vertical
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
    vertical: Vertical
    osm_tags: list[dict[str, str]]
    search_terms: list[str]
