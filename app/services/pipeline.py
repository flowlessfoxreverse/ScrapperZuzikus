from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Category, Company, CompanyCategory, Email, Form, Page, Region, RunCategory, RunCompanyStatus, RunStatus, ScrapeRun
from app.services.company_dedupe import find_company_by_website_key, should_replace_name
from app.services.discovery_state import ensure_utc, get_or_create_region_category_state, should_refresh_discovery
from app.services.crawler import crawl_site
from app.services.metrics import record_request_metric
from app.services.overpass import fetch_places
from app.services.runs import finalize_cancelled_run
from app.services.run_companies import (
    close_open_run_companies,
    mark_run_company_finished,
    mark_run_company_running,
    maybe_complete_run,
    queue_company_for_run,
)
from app.services.usage import can_consume, consume_units


def upsert_company_from_element(
    session: Session,
    region: Region,
    category: Category,
    element: dict,
    query: str,
) -> Company:
    external_ref = f"{element.get('type', 'nwr')}:{element.get('id')}"
    tags = element.get("tags", {})
    incoming_name = tags.get("name") or external_ref
    incoming_website = tags.get("website") or tags.get("contact:website")
    company = (
        session.query(Company)
        .filter(Company.region_id == region.id, Company.source == "overpass", Company.external_ref == external_ref)
        .one_or_none()
    )
    if company is None:
        company = find_company_by_website_key(
            session,
            region.id,
            incoming_website,
        )
    if company is None:
        company = Company(
            region_id=region.id,
            name=incoming_name,
            website_url=incoming_website,
            city=tags.get("addr:city"),
            source="overpass",
            external_ref=external_ref,
            source_query=query[:255],
            source_payload=element,
            latitude=str(element.get("lat") or element.get("center", {}).get("lat") or ""),
            longitude=str(element.get("lon") or element.get("center", {}).get("lon") or ""),
        )
        session.add(company)
        session.flush()
    else:
        if should_replace_name(company, incoming_name, external_ref):
            company.name = incoming_name
        company.website_url = incoming_website or company.website_url
        company.city = tags.get("addr:city") or company.city
        company.source_query = company.source_query or query[:255]
        company.source_payload = company.source_payload or element

    company_category = (
        session.query(CompanyCategory)
        .filter(CompanyCategory.company_id == company.id, CompanyCategory.category_id == category.id)
        .one_or_none()
    )
    pending_company_category = any(
        isinstance(obj, CompanyCategory)
        and obj.company_id == company.id
        and obj.category_id == category.id
        for obj in session.new
    )
    if company_category is None and not pending_company_category:
        session.add(CompanyCategory(company_id=company.id, category_id=category.id))

    return company


def persist_crawl(session: Session, company: Company, run_id: int | None = None) -> None:
    if not company.website_url:
        company.crawl_status = "no_website"
        session.add(company)
        return

    def on_request(**metric):
        record_request_metric(
            session,
            provider="website",
            request_kind="crawl",
            run_id=run_id,
            company_id=company.id,
            **metric,
        )

    result = crawl_site(company.website_url, on_request=on_request)
    company.crawl_status = result.crawl_status
    company.has_contact_form = any(page.has_contact_form for page in result.pages)
    session.add(company)
    session.flush()

    for page_result in result.pages:
        page = (
            session.query(Page)
            .filter(Page.company_id == company.id, Page.url == page_result.url)
            .one_or_none()
        )
        if page is None:
            page = Page(company_id=company.id, url=page_result.url)
        page.title = page_result.title
        page.status_code = page_result.status_code
        page.has_contact_form = page_result.has_contact_form
        page.crawl_error = page_result.error
        page.crawled_at = datetime.now(timezone.utc)
        session.add(page)
        session.flush()

        for email_value in page_result.emails:
            existing = (
                session.query(Email)
                .filter(Email.company_id == company.id, Email.email == email_value)
                .one_or_none()
            )
            if existing is None:
                session.add(
                    Email(
                        company_id=company.id,
                        email=email_value,
                        source_type="regex",
                        source_page_url=page_result.url,
                        technical_metadata={"title": page_result.title},
                    )
                )
            else:
                existing.source_page_url = page_result.url
                existing.technical_metadata = {"title": page_result.title}
                existing.last_seen_at = datetime.now(timezone.utc)
                session.add(existing)

        for form_data in page_result.forms:
            existing_form = (
                session.query(Form)
                .filter(Form.company_id == company.id, Form.page_url == page_result.url)
                .one_or_none()
            )
            if existing_form is None:
                session.add(
                    Form(
                        company_id=company.id,
                        page_url=page_result.url,
                        action_url=form_data.get("action_url"),
                        method=form_data.get("method") or "get",
                        has_captcha=form_data.get("has_captcha", False),
                        schema_json=form_data,
                    )
                )


def companies_for_category(session: Session, region_id: int, category_id: int) -> list[Company]:
    return session.scalars(
        select(Company)
        .join(CompanyCategory, CompanyCategory.company_id == Company.id)
        .where(
            Company.region_id == region_id,
            CompanyCategory.category_id == category_id,
        )
        .order_by(Company.id.asc())
    ).all()


def should_recrawl_company(session: Session, company: Company, recrawl_hours: int) -> bool:
    if not company.website_url:
        return False
    if company.crawl_status in {"pending", "failed", "blocked_by_robots"}:
        return True

    last_crawled_at = session.scalar(
        select(func.max(Page.crawled_at)).where(Page.company_id == company.id)
    )
    if last_crawled_at is None:
        return True
    last_crawled_at = ensure_utc(last_crawled_at)
    if recrawl_hours <= 0:
        return True
    return last_crawled_at <= datetime.now(timezone.utc) - timedelta(hours=recrawl_hours)


def execute_discovery(
    session: Session,
    run_id: int,
    overpass_cap: int,
    discovery_cooldown_hours: int,
    crawl_recrawl_hours: int,
    force_refresh_category_ids: set[int] | None,
    enqueue_crawl,
) -> None:
    run = session.get(ScrapeRun, run_id)
    if run is None:
        return
    session.refresh(run)
    if run.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.SKIPPED}:
        return
    if run.cancel_requested:
        finalize_cancelled_run(session, run)
        session.commit()
        return

    region = session.get(Region, run.region_id)
    if region is None:
        run.status = RunStatus.FAILED
        run.note = "Region not found."
        run.finished_at = datetime.now(timezone.utc)
        close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
        session.commit()
        return

    run.status = RunStatus.RUNNING
    session.commit()

    category_ids = [item.category_id for item in run.categories]
    categories = session.scalars(select(Category).where(Category.id.in_(category_ids))).all()

    discovered = 0
    crawled = 0
    queries_used = 0
    category_errors: list[str] = []
    any_category_succeeded = False
    force_refresh_category_ids = force_refresh_category_ids or set()

    for category in categories:
        session.refresh(run)
        if run.cancel_requested:
            finalize_cancelled_run(session, run)
            session.commit()
            return
        state = get_or_create_region_category_state(session, region.id, category.id)
        state.last_run_id = run.id
        state.last_discovery_attempt_at = datetime.now(timezone.utc)
        session.add(state)
        session.commit()

        force_refresh = category.id in force_refresh_category_ids
        if force_refresh or should_refresh_discovery(state, discovery_cooldown_hours):
            allowed, usage = can_consume(session, provider="overpass", cap=overpass_cap, units=1)
            if not allowed:
                run.status = RunStatus.SKIPPED
                run.note = f"Daily Overpass cap reached ({usage.units_used}/{usage.cap})."
                run.finished_at = datetime.now(timezone.utc)
                close_open_run_companies(session, run.id, RunCompanyStatus.SKIPPED, run.note)
                state.status = "rate_limited"
                state.note = run.note
                session.add(state)
                break

            try:
                result = fetch_places(
                    region=region,
                    category=category,
                    on_request=lambda **metric: record_request_metric(
                        session,
                        provider="overpass",
                        request_kind="discovery",
                        run_id=run.id,
                        company_id=None,
                        **metric,
                    ),
                )
            except Exception as exc:
                state.status = "failed"
                state.note = str(exc)[:2000]
                session.add(state)
                session.commit()
                category_errors.append(f"{category.slug}: {str(exc)[:300]}")
                continue

            consume_units(session, provider="overpass", cap=overpass_cap, units=1)
            queries_used += 1
            discovered += len(result.elements)
            any_category_succeeded = True
            state.last_discovery_success_at = datetime.now(timezone.utc)
            state.last_result_count = len(result.elements)
            state.status = "fresh_forced" if force_refresh else "fresh"
            state.note = (
                f"Discovery force-refreshed for category {category.slug}."
                if force_refresh
                else f"Discovery refreshed for category {category.slug}."
            )
            session.add(state)

            for element in result.elements:
                upsert_company_from_element(
                    session=session,
                    region=region,
                    category=category,
                    element=element,
                    query=result.query,
                )
            session.commit()
        else:
            discovered += state.last_result_count
            any_category_succeeded = True
            state.status = "cached"
            state.note = f"Discovery reused cached results for category {category.slug}."
            session.add(state)
            session.commit()

        for company in companies_for_category(session, region.id, category.id):
            session.refresh(run)
            if run.cancel_requested:
                finalize_cancelled_run(session, run)
                session.commit()
                return
            if should_recrawl_company(session, company, crawl_recrawl_hours):
                if queue_company_for_run(session, run.id, company.id):
                    enqueue_crawl(run.id, company.id)
                session.commit()

    run.discovered_count = discovered
    run.crawled_count = crawled
    run.overpass_queries_used = queries_used
    if category_errors and not any_category_succeeded:
        run.status = RunStatus.FAILED
        run.finished_at = datetime.now(timezone.utc)
        run.note = " ; ".join(category_errors)[:2000]
        close_open_run_companies(session, run.id, RunCompanyStatus.FAILED, run.note)
        session.add(run)
        session.commit()
        return

    base_note = (
        "Discovery completed."
        if not force_refresh_category_ids
        else f"Discovery completed with force refresh for category ids: {sorted(force_refresh_category_ids)}."
    )
    if category_errors:
        run.note = f"{base_note} Partial category failures: {' ; '.join(category_errors)}"[:2000]
    else:
        run.note = run.note or base_note
    maybe_complete_run(session, run.id)
    session.commit()


def execute_crawl(session: Session, run_id: int, company_id: int) -> None:
    run = session.get(ScrapeRun, run_id)
    company = session.get(Company, company_id)
    if run is None or company is None:
        return
    session.refresh(run)
    if run.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.SKIPPED}:
        return
    if run.cancel_requested:
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.SKIPPED, "Cancelled before crawl start.")
        finalize_cancelled_run(session, run, "Run stopped by request.")
        session.commit()
        return

    if not company.website_url:
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.SKIPPED, "No website available.")
        maybe_complete_run(session, run_id)
        session.commit()
        return

    mark_run_company_running(session, run_id, company_id)
    session.commit()

    try:
        persist_crawl(session=session, company=company, run_id=run_id)
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.COMPLETED)
    except Exception as exc:
        company.crawl_status = "failed"
        session.add(company)
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.FAILED, str(exc))
    session.refresh(run)
    if run.cancel_requested:
        finalize_cancelled_run(session, run, "Run stopped by request.")
    maybe_complete_run(session, run_id)
    session.commit()
