from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    Category,
    Company,
    CompanyCategory,
    CompanySource,
    DiscoverySource,
    Phone,
    QueryRecipe,
    QueryRecipeVersion,
    Region,
    ScrapeRun,
    SourceJob,
    SourceJobQuery,
    SourceJobQueryStatus,
    SourceJobStatus,
    SourceRecord,
    SourceRecordMergeStatus,
)
from app.services.company_dedupe import find_company_by_website_key, should_replace_name
from app.services.crawler import normalize_phone_number, sanitize_company_website_url
from app.services.run_companies import queue_company_for_run


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SourceIngestionSummary:
    source_job_id: int
    source_record_count: int
    query_count: int
    matched_company_count: int
    created_company_count: int
    queued_company_count: int
    queued_company_ids: list[int] = field(default_factory=list)


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value in {None, ""}:
            return None
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _normalized_external_id(source: DiscoverySource, record: dict[str, Any]) -> str:
    if source == DiscoverySource.GOOGLE_MAPS:
        external_id = _to_str(record.get("place_id"))
        if external_id:
            return external_id
    return _to_str(record.get("external_id")) or _to_str(record.get("website")) or _to_str(record.get("name")) or "unknown"


def _build_dedupe_key(
    *,
    website_url: str | None,
    normalized_phone: str | None,
    source: DiscoverySource,
    external_id: str,
) -> str:
    if website_url:
        return f"website:{website_url}"
    if normalized_phone:
        return f"phone:{normalized_phone}"
    return f"{source.value}:{external_id}"


def _find_company_by_phone(session: Session, region_id: int, normalized_phone: str | None) -> Company | None:
    if not normalized_phone:
        return None
    return session.scalar(
        select(Company)
        .join(Phone, Phone.company_id == Company.id)
        .where(
            Company.region_id == region_id,
            Phone.normalized_number == normalized_phone,
        )
        .limit(1)
    )


def _get_existing_company_source(session: Session, source: DiscoverySource, external_id: str) -> CompanySource | None:
    return session.scalar(
        select(CompanySource).where(
            CompanySource.source == source,
            CompanySource.external_id == external_id,
        )
    )


def _attach_category(session: Session, company_id: int, category_id: int | None) -> None:
    if category_id is None:
        return
    existing = session.scalar(
        select(CompanyCategory).where(
            CompanyCategory.company_id == company_id,
            CompanyCategory.category_id == category_id,
        )
    )
    if existing is None:
        session.add(CompanyCategory(company_id=company_id, category_id=category_id))


def _materialize_source_record(
    session: Session,
    *,
    source_job: SourceJob,
    source_record: SourceRecord,
    region: Region,
    category_id: int | None,
) -> tuple[Company, bool]:
    existing_link = _get_existing_company_source(session, source_record.source, source_record.external_id)
    if existing_link is not None:
        company = existing_link.company
        created = False
    else:
        normalized_phone = normalize_phone_number(source_record.phone_raw or "", default_region_code=region.country_code)
        company = find_company_by_website_key(session, region.id, source_record.website_url)
        if company is None:
            company = _find_company_by_phone(session, region.id, normalized_phone)

        if company is None:
            company = Company(
                region_id=region.id,
                name=source_record.canonical_name,
                website_url=source_record.website_url,
                city=source_record.city,
                source=source_record.source.value,
                external_ref=source_record.external_id[:128],
                source_query=None,
                source_payload=source_record.raw_payload,
                latitude=source_record.latitude,
                longitude=source_record.longitude,
            )
            session.add(company)
            session.flush()
            created = True
        else:
            created = False
            if should_replace_name(company, source_record.canonical_name, source_record.external_id):
                company.name = source_record.canonical_name
            if source_record.website_url and not company.website_url:
                company.website_url = source_record.website_url
            company.city = company.city or source_record.city
            if not company.source_payload and source_record.raw_payload:
                company.source_payload = source_record.raw_payload

        session.add(
            CompanySource(
                company_id=company.id,
                source=source_record.source,
                external_id=source_record.external_id,
                source_job_id=source_job.id,
                source_record_id=source_record.id,
                confidence=1.0,
                is_primary=False,
            )
        )

    source_record.matched_company_id = company.id
    source_record.merge_status = SourceRecordMergeStatus.MERGED if created else SourceRecordMergeStatus.MATCHED
    session.add(source_record)
    _attach_category(session, company.id, category_id)
    return company, created


def ingest_google_maps_results(
    session: Session,
    *,
    region_id: int,
    results: list[dict[str, Any]],
    prompt_text: str | None = None,
    category_id: int | None = None,
    recipe_id: int | None = None,
    recipe_version_id: int | None = None,
    run_id: int | None = None,
    provider: str | None = "maps_scraper_v3",
    materialize_companies: bool = False,
    enqueue_crawl: bool = False,
) -> SourceIngestionSummary:
    if enqueue_crawl and not materialize_companies:
        raise ValueError("enqueue_crawl requires materialize_companies=True.")

    region = session.get(Region, region_id)
    if region is None:
        raise ValueError(f"Region {region_id} not found.")
    if category_id is not None and session.get(Category, category_id) is None:
        raise ValueError(f"Category {category_id} not found.")
    if recipe_id is not None and session.get(QueryRecipe, recipe_id) is None:
        raise ValueError(f"Recipe {recipe_id} not found.")
    if recipe_version_id is not None and session.get(QueryRecipeVersion, recipe_version_id) is None:
        raise ValueError(f"Recipe version {recipe_version_id} not found.")
    if run_id is not None and session.get(ScrapeRun, run_id) is None:
        raise ValueError(f"Run {run_id} not found.")

    source_job = SourceJob(
        source=DiscoverySource.GOOGLE_MAPS,
        status=SourceJobStatus.RUNNING,
        prompt_text=prompt_text,
        country_code=region.country_code,
        region_id=region.id,
        run_id=run_id,
        category_id=category_id,
        recipe_id=recipe_id,
        recipe_version_id=recipe_version_id,
        provider=provider,
        started_at=utcnow(),
    )
    session.add(source_job)
    session.flush()

    grouped_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in results:
        query_text = _to_str(item.get("keyword")) or prompt_text or "google_maps"
        grouped_records[query_text].append(item)

    source_queries: dict[str, SourceJobQuery] = {}
    for query_text, grouped in grouped_records.items():
        row = SourceJobQuery(
            source_job_id=source_job.id,
            status=SourceJobQueryStatus.COMPLETED,
            query_text=query_text,
            query_kind="keyword",
            raw_request={"keyword": query_text},
            result_count=len(grouped),
            duration_ms=None,
        )
        session.add(row)
        session.flush()
        source_queries[query_text] = row

    created_company_count = 0
    matched_company_count = 0
    queued_company_count = 0
    queued_company_ids: list[int] = []

    for item in results:
        query_text = _to_str(item.get("keyword")) or prompt_text or "google_maps"
        source_query = source_queries[query_text]
        external_id = _normalized_external_id(DiscoverySource.GOOGLE_MAPS, item)
        website_url = sanitize_company_website_url(_to_str(item.get("website")))
        phone_raw = _to_str(item.get("phone"))
        normalized_phone = normalize_phone_number(phone_raw or "", default_region_code=region.country_code)
        dedupe_key = _build_dedupe_key(
            website_url=website_url,
            normalized_phone=normalized_phone,
            source=DiscoverySource.GOOGLE_MAPS,
            external_id=external_id,
        )
        source_record = SourceRecord(
            source_job_id=source_job.id,
            source_query_id=source_query.id,
            source=DiscoverySource.GOOGLE_MAPS,
            external_id=external_id,
            canonical_name=_to_str(item.get("name")) or external_id,
            website_url=website_url,
            phone_raw=phone_raw,
            address_raw=_to_str(item.get("address")),
            city=None,
            latitude=_to_str(item.get("latitude")),
            longitude=_to_str(item.get("longitude")),
            rating=_to_float(item.get("rating")),
            reviews_count=_to_int(item.get("reviews")),
            dedupe_key=dedupe_key,
            merge_status=SourceRecordMergeStatus.PENDING,
            raw_payload=item,
        )
        session.add(source_record)
        session.flush()

        if materialize_companies:
            company, created = _materialize_source_record(
                session,
                source_job=source_job,
                source_record=source_record,
                region=region,
                category_id=category_id,
            )
            if created:
                created_company_count += 1
            else:
                matched_company_count += 1
            if enqueue_crawl and run_id is not None and queue_company_for_run(session, run_id, company.id):
                queued_company_count += 1
                queued_company_ids.append(company.id)
        session.add(source_record)

    source_job.status = SourceJobStatus.COMPLETED
    source_job.finished_at = utcnow()
    source_job.note = f"Ingested {len(results)} Google Maps results."
    session.add(source_job)
    session.flush()

    return SourceIngestionSummary(
        source_job_id=source_job.id,
        source_record_count=len(results),
        query_count=len(source_queries),
        matched_company_count=matched_company_count,
        created_company_count=created_company_count,
        queued_company_count=queued_company_count,
        queued_company_ids=queued_company_ids,
    )
