from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Category, Company, CompanyCategory, Email, Form, Page, Region, RunCategory, RunStatus, ScrapeRun
from app.services.crawler import crawl_site
from app.services.overpass import fetch_places
from app.services.usage import can_consume, consume_units


def upsert_company_from_element(
    session: Session,
    region: Region,
    category: Category,
    element: dict,
    query: str,
) -> Company:
    external_ref = f"{element.get('type', 'nwr')}:{element.get('id')}"
    company = (
        session.query(Company)
        .filter(Company.region_id == region.id, Company.source == "overpass", Company.external_ref == external_ref)
        .one_or_none()
    )
    tags = element.get("tags", {})
    if company is None:
        company = Company(
            region_id=region.id,
            name=tags.get("name") or external_ref,
            website_url=tags.get("website") or tags.get("contact:website"),
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
        company.name = tags.get("name") or company.name
        company.website_url = tags.get("website") or tags.get("contact:website") or company.website_url
        company.city = tags.get("addr:city") or company.city
        company.source_payload = element

    company_category = (
        session.query(CompanyCategory)
        .filter(CompanyCategory.company_id == company.id, CompanyCategory.category_id == category.id)
        .one_or_none()
    )
    if company_category is None:
        session.add(CompanyCategory(company_id=company.id, category_id=category.id))

    return company


def persist_crawl(session: Session, company: Company) -> None:
    if not company.website_url:
        company.crawl_status = "no_website"
        session.add(company)
        return

    result = crawl_site(company.website_url)
    company.crawl_status = result.crawl_status
    company.has_contact_form = any(page.has_contact_form for page in result.pages)
    session.add(company)
    session.flush()

    for page_result in result.pages:
        page = Page(
            company_id=company.id,
            url=page_result.url,
            title=page_result.title,
            status_code=page_result.status_code,
            has_contact_form=page_result.has_contact_form,
            crawl_error=page_result.error,
        )
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


def execute_run(session: Session, run_id: int, overpass_cap: int) -> None:
    run = session.get(ScrapeRun, run_id)
    if run is None:
        return

    region = session.get(Region, run.region_id)
    if region is None:
        run.status = RunStatus.FAILED
        run.note = "Region not found."
        session.commit()
        return

    run.status = RunStatus.RUNNING
    session.commit()

    category_ids = [item.category_id for item in run.categories]
    categories = session.scalars(select(Category).where(Category.id.in_(category_ids))).all()

    discovered = 0
    crawled = 0
    queries_used = 0

    for category in categories:
        allowed, usage = can_consume(session, provider="overpass", cap=overpass_cap, units=1)
        if not allowed:
            run.status = RunStatus.SKIPPED
            run.note = f"Daily Overpass cap reached ({usage.units_used}/{usage.cap})."
            break

        result = fetch_places(region=region, category=category)
        consume_units(session, provider="overpass", cap=overpass_cap, units=1)
        queries_used += 1
        discovered += len(result.elements)

        for element in result.elements:
            company = upsert_company_from_element(
                session=session,
                region=region,
                category=category,
                element=element,
                query=result.query,
            )
            if company.website_url:
                persist_crawl(session=session, company=company)
                crawled += 1
            session.commit()

    if run.status == RunStatus.RUNNING:
        run.status = RunStatus.COMPLETED

    run.discovered_count = discovered
    run.crawled_count = crawled
    run.overpass_queries_used = queries_used
    session.commit()

