from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Company, Email, Form, Page, Phone, RequestMetric, RunCompany, RunCompanyStatus, Submission


RUN_COMPANY_STATUS_PRIORITY = {
    RunCompanyStatus.COMPLETED: 5,
    RunCompanyStatus.RUNNING: 4,
    RunCompanyStatus.QUEUED: 3,
    RunCompanyStatus.SKIPPED: 2,
    RunCompanyStatus.FAILED: 1,
}
PLACEHOLDER_PREFIXES = ("node:", "way:", "relation:", "nwr:")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_website_key(url: str | None) -> str | None:
    if not url:
        return None
    candidate = url.strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    host = parsed.netloc.lower().strip()
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]
    path = (parsed.path or "").rstrip("/")
    if path in {"", "/"}:
        path = ""
    return f"{host}{path}"


def is_placeholder_company_name(name: str | None, external_ref: str | None = None) -> bool:
    normalized = (name or "").strip().lower()
    if not normalized:
        return True
    if external_ref and normalized == external_ref.strip().lower():
        return True
    return normalized.startswith(PLACEHOLDER_PREFIXES)


def company_name_score(company: Company) -> tuple[int, int, int, int]:
    name = (company.name or "").strip()
    real_name = 0 if is_placeholder_company_name(name, company.external_ref) else 1
    has_city = 1 if company.city else 0
    has_crawled = 1 if company.crawl_status not in {"pending", "failed", "no_website"} else 0
    return (real_name, has_crawled, has_city, len(name))


def should_replace_name(current: Company, incoming_name: str | None, incoming_external_ref: str) -> bool:
    if not incoming_name:
        return False
    incoming_placeholder = is_placeholder_company_name(incoming_name, incoming_external_ref)
    current_placeholder = is_placeholder_company_name(current.name, current.external_ref)
    if current_placeholder and not incoming_placeholder:
        return True
    if current_placeholder == incoming_placeholder:
        return len(incoming_name.strip()) > len((current.name or "").strip())
    return False


def find_company_by_website_key(
    session: Session,
    region_id: int,
    website_url: str | None,
    exclude_company_id: int | None = None,
) -> Company | None:
    website_key = normalize_website_key(website_url)
    if website_key is None:
        return None

    companies = session.scalars(
        select(Company).where(
            Company.region_id == region_id,
            Company.website_url.is_not(None),
        )
    ).all()
    matches = [
        company
        for company in companies
        if company.id != exclude_company_id and normalize_website_key(company.website_url) == website_key
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda company: (company_name_score(company), -company.id), reverse=True)[0]


def merge_page(target_page: Page, source_page: Page) -> None:
    target_page.title = target_page.title or source_page.title
    target_page.status_code = target_page.status_code or source_page.status_code
    target_page.has_contact_form = target_page.has_contact_form or source_page.has_contact_form
    target_page.crawl_error = target_page.crawl_error or source_page.crawl_error
    target_page.crawled_at = max(
        filter(None, [target_page.crawled_at, source_page.crawled_at]),
        default=utcnow(),
    )


def merge_email(target_email: Email, source_email: Email) -> None:
    target_email.source_page_url = target_email.source_page_url or source_email.source_page_url
    target_email.source_type = target_email.source_type or source_email.source_type
    if source_email.technical_metadata:
        merged_metadata = dict(target_email.technical_metadata or {})
        merged_metadata.update(source_email.technical_metadata)
        target_email.technical_metadata = merged_metadata
    if source_email.first_seen_at:
        target_email.first_seen_at = min(
            filter(None, [target_email.first_seen_at, source_email.first_seen_at]),
            default=source_email.first_seen_at,
        )
    if source_email.last_seen_at:
        target_email.last_seen_at = max(
            filter(None, [target_email.last_seen_at, source_email.last_seen_at]),
            default=source_email.last_seen_at,
        )


def merge_phone(target_phone: Phone, source_phone: Phone) -> None:
    target_phone.source_page_url = target_phone.source_page_url or source_phone.source_page_url
    target_phone.source_type = target_phone.source_type or source_phone.source_type
    if source_phone.technical_metadata:
        merged_metadata = dict(target_phone.technical_metadata or {})
        merged_metadata.update(source_phone.technical_metadata)
        target_phone.technical_metadata = merged_metadata
    if len((source_phone.phone_number or "").strip()) > len((target_phone.phone_number or "").strip()):
        target_phone.phone_number = source_phone.phone_number
    if source_phone.first_seen_at:
        target_phone.first_seen_at = min(
            filter(None, [target_phone.first_seen_at, source_phone.first_seen_at]),
            default=source_phone.first_seen_at,
        )
    if source_phone.last_seen_at:
        target_phone.last_seen_at = max(
            filter(None, [target_phone.last_seen_at, source_phone.last_seen_at]),
            default=source_phone.last_seen_at,
        )


def merge_run_company(target_row: RunCompany, source_row: RunCompany) -> None:
    if RUN_COMPANY_STATUS_PRIORITY[source_row.status] > RUN_COMPANY_STATUS_PRIORITY[target_row.status]:
        target_row.status = source_row.status
    target_row.queued_at = min(
        filter(None, [target_row.queued_at, source_row.queued_at]),
        default=target_row.queued_at,
    )
    target_row.started_at = min(
        filter(None, [target_row.started_at, source_row.started_at]),
        default=target_row.started_at,
    )
    target_row.finished_at = max(
        filter(None, [target_row.finished_at, source_row.finished_at]),
        default=target_row.finished_at,
    )
    target_row.last_error = target_row.last_error or source_row.last_error


def merge_form(target_form: Form, source_form: Form) -> None:
    target_form.action_url = target_form.action_url or source_form.action_url
    target_form.has_captcha = target_form.has_captcha or source_form.has_captcha
    target_form.is_js_challenge = target_form.is_js_challenge or source_form.is_js_challenge
    target_form.last_checked_at = max(
        filter(None, [target_form.last_checked_at, source_form.last_checked_at]),
        default=utcnow(),
    )
    if source_form.schema_json:
        merged_schema = dict(target_form.schema_json or {})
        merged_schema.update(source_form.schema_json)
        target_form.schema_json = merged_schema


def merge_company_into(session: Session, target: Company, source: Company) -> None:
    if should_replace_name(target, source.name, source.external_ref):
        target.name = source.name
    target.website_url = target.website_url or source.website_url
    target.city = target.city or source.city
    target.source_query = target.source_query or source.source_query
    target.latitude = target.latitude or source.latitude
    target.longitude = target.longitude or source.longitude
    target.has_contact_form = target.has_contact_form or source.has_contact_form
    if target.crawl_status in {"pending", "failed", "no_website"} and source.crawl_status not in {"pending", "failed", "no_website"}:
        target.crawl_status = source.crawl_status
    if source.source_payload:
        target.source_payload = target.source_payload or source.source_payload
    target.updated_at = utcnow()
    session.add(target)

    existing_category_ids = {row.category_id for row in target.categories}
    for category_row in list(source.categories):
        if category_row.category_id in existing_category_ids:
            session.delete(category_row)
            continue
        category_row.company_id = target.id
        session.add(category_row)

    existing_pages = {page.url: page for page in target.pages}
    for source_page in list(source.pages):
        target_page = existing_pages.get(source_page.url)
        if target_page is None:
            source_page.company_id = target.id
            session.add(source_page)
            existing_pages[source_page.url] = source_page
            continue
        merge_page(target_page, source_page)
        session.add(target_page)
        session.delete(source_page)

    existing_emails = {email.email.lower(): email for email in target.emails}
    for source_email in list(source.emails):
        target_email = existing_emails.get(source_email.email.lower())
        if target_email is None:
            source_email.company_id = target.id
            session.add(source_email)
            existing_emails[source_email.email.lower()] = source_email
            continue
        merge_email(target_email, source_email)
        session.add(target_email)
        session.delete(source_email)

    existing_phones = {phone.normalized_number: phone for phone in target.phones}
    for source_phone in list(source.phones):
        target_phone = existing_phones.get(source_phone.normalized_number)
        if target_phone is None:
            source_phone.company_id = target.id
            session.add(source_phone)
            existing_phones[source_phone.normalized_number] = source_phone
            continue
        merge_phone(target_phone, source_phone)
        session.add(target_phone)
        session.delete(source_phone)

    existing_forms = {form.page_url: form for form in target.forms}
    for source_form in list(source.forms):
        target_form = existing_forms.get(source_form.page_url)
        if target_form is None:
            source_form.company_id = target.id
            for submission in list(source_form.submissions):
                submission.company_id = target.id
                session.add(submission)
            session.add(source_form)
            existing_forms[source_form.page_url] = source_form
            continue
        merge_form(target_form, source_form)
        session.add(target_form)
        for submission in list(source_form.submissions):
            submission.company_id = target.id
            submission.form_id = target_form.id
            session.add(submission)
        session.delete(source_form)

    existing_run_companies = {row.run_id: row for row in target.run_companies}
    for source_row in list(source.run_companies):
        target_row = existing_run_companies.get(source_row.run_id)
        if target_row is None:
            source_row.company_id = target.id
            session.add(source_row)
            existing_run_companies[source_row.run_id] = source_row
            continue
        merge_run_company(target_row, source_row)
        session.add(target_row)
        session.delete(source_row)

    request_metrics = session.scalars(
        select(RequestMetric).where(RequestMetric.company_id == source.id)
    ).all()
    for metric in request_metrics:
        metric.company_id = target.id
        session.add(metric)

    session.delete(source)
    session.flush()


def reconcile_duplicate_companies(session: Session) -> int:
    companies = session.scalars(
        select(Company).where(Company.website_url.is_not(None)).order_by(Company.region_id, Company.id)
    ).all()
    groups: dict[tuple[int, str], list[Company]] = {}
    for company in companies:
        website_key = normalize_website_key(company.website_url)
        if website_key is None:
            continue
        groups.setdefault((company.region_id, website_key), []).append(company)

    merged = 0
    for duplicates in groups.values():
        if len(duplicates) < 2:
            continue
        ordered = sorted(duplicates, key=lambda company: (company_name_score(company), -company.id), reverse=True)
        canonical = ordered[0]
        for duplicate in ordered[1:]:
            merge_company_into(session, canonical, duplicate)
            merged += 1
    session.flush()
    return merged
