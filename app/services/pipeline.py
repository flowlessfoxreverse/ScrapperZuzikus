from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Category, Company, CompanyCategory, ContactChannel, ContactChannelType, Email, Form, Page, Phone, ProxyKind, Region, RunCategory, RunCompanyStatus, RunStatus, ScrapeRun
from app.services.company_dedupe import find_company_by_website_key, should_replace_name
from app.services.discovery_state import ensure_utc, get_or_create_region_category_state, should_refresh_discovery
from app.services.browser_crawler import browser_crawl_site
from app.services.crawler import crawl_site, normalize_phone_number, normalize_telegram_value, should_browser_escalate
from app.services.metrics import record_request_metric
from app.services.overpass import fetch_places
from app.services.proxy_pool import acquire_proxy, active_proxy_count, release_proxy, render_proxy_url
from app.services.runs import finalize_cancelled_run
from app.services.run_companies import (
    close_open_run_companies,
    current_retry_count,
    increment_retry_count,
    mark_run_company_finished,
    mark_run_company_running,
    maybe_complete_run,
    queue_company_for_run,
    requeue_run_company,
)
from app.services.usage import can_consume, consume_units

settings = get_settings()


def _is_proxy_transport_error(exc: Exception, proxy_url: str | None) -> bool:
    message = str(exc).lower()
    proxy_host = ""
    if proxy_url:
        parsed = urlparse(proxy_url)
        proxy_host = (parsed.hostname or "").lower()
    indicators = (
        "proxy",
        "407",
        "proxy authentication",
        "tunnel connection failed",
        "socks",
        "tunneling",
        "connection reset by peer",
        "temporary failure in name resolution",
        "name or service not known",
        "network is unreachable",
        "no route to host",
        "econnreset",
        "connection aborted",
    )
    if proxy_host and proxy_host in message:
        return True
    return any(indicator in message for indicator in indicators)


def _is_dead_host_error(exc: Exception) -> bool:
    message = str(exc).lower()
    indicators = (
        "no host connection",
        "temporary failure in name resolution",
        "name or service not known",
        "nodename nor servname provided",
        "getaddrinfo failed",
        "failed to resolve",
        "no address associated with hostname",
        "hostname nor servname provided",
        "server misbehaving",
        "dns",
    )
    return any(indicator in message for indicator in indicators)


def _retry_delay_ms(attempt: int) -> int:
    base_delay = max(1, settings.crawl_retry_delay_seconds)
    return int(base_delay * min(attempt, 4) * 1000)


def _schedule_retry(
    session: Session,
    *,
    run_id: int,
    company_id: int,
    workload: ProxyKind,
    reason: str,
) -> bool:
    current_attempt = current_retry_count(session, run_id, company_id)
    if current_attempt >= settings.crawl_retry_attempts:
        return False
    next_attempt = increment_retry_count(session, run_id, company_id)
    requeue_run_company(
        session,
        run_id,
        company_id,
        f"Retry {next_attempt}/{settings.crawl_retry_attempts} queued: {reason}",
    )
    session.commit()
    from app.tasks import retry_company

    retry_company.send_with_options(
        args=(run_id, company_id, workload.value),
        delay=_retry_delay_ms(next_attempt),
    )
    return True


def _find_pending_email(session: Session, company_id: int, normalized_email: str) -> Email | None:
    for obj in session.new:
        if isinstance(obj, Email) and obj.company_id == company_id and obj.email == normalized_email:
            return obj
    return None


def _find_pending_phone(session: Session, company_id: int, normalized_phone: str) -> Phone | None:
    for obj in session.new:
        if isinstance(obj, Phone) and obj.company_id == company_id and obj.normalized_number == normalized_phone:
            return obj
    return None


def _find_pending_contact_channel(
    session: Session,
    company_id: int,
    channel_type: ContactChannelType,
    normalized_value: str,
) -> ContactChannel | None:
    for obj in session.new:
        if (
            isinstance(obj, ContactChannel)
            and obj.company_id == company_id
            and obj.channel_type == channel_type
            and obj.normalized_value == normalized_value
        ):
            return obj
    return None


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

    persist_overpass_contacts(session, company, tags, region.country_code)
    return company


def _merge_provenance_metadata(existing_metadata: dict | None, *, source_type: str, source_page_url: str | None, payload: dict | None) -> dict:
    metadata = dict(existing_metadata or {})
    sources = list(metadata.get("sources") or [])
    source_entry = {
        "source_type": source_type,
        "source_page_url": source_page_url,
    }
    if payload:
        source_entry.update(payload)
    if source_entry not in sources:
        sources.append(source_entry)
    metadata["sources"] = sources
    if payload:
        metadata.update(payload)
    return metadata


def persist_email_value(
    session: Session,
    company: Company,
    email_value: str,
    *,
    source_type: str,
    source_page_url: str | None,
    metadata: dict | None = None,
) -> None:
    normalized_email = email_value.strip().lower()
    if not normalized_email:
        return
    pending = _find_pending_email(session, company.id, normalized_email)
    if pending is not None:
        pending.source_page_url = pending.source_page_url or source_page_url
        pending.source_type = pending.source_type or source_type
        pending.technical_metadata = _merge_provenance_metadata(
            pending.technical_metadata,
            source_type=source_type,
            source_page_url=source_page_url,
            payload=metadata,
        )
        pending.last_seen_at = datetime.now(timezone.utc)
        return
    existing = (
        session.query(Email)
        .filter(Email.company_id == company.id, Email.email == normalized_email)
        .one_or_none()
    )
    if existing is None:
        session.add(
            Email(
                company_id=company.id,
                email=normalized_email,
                source_type=source_type,
                source_page_url=source_page_url,
                technical_metadata=_merge_provenance_metadata({}, source_type=source_type, source_page_url=source_page_url, payload=metadata),
            )
        )
        return
    existing.source_page_url = existing.source_page_url or source_page_url
    existing.source_type = existing.source_type or source_type
    existing.technical_metadata = _merge_provenance_metadata(
        existing.technical_metadata,
        source_type=source_type,
        source_page_url=source_page_url,
        payload=metadata,
    )
    existing.last_seen_at = datetime.now(timezone.utc)
    session.add(existing)


def persist_phone_value(
    session: Session,
    company: Company,
    phone_value: str,
    *,
    source_type: str,
    source_page_url: str | None,
    metadata: dict | None = None,
    default_region_code: str | None = None,
) -> str | None:
    normalized_phone = normalize_phone_number(phone_value, default_region_code=default_region_code)
    if not normalized_phone:
        return None
    pending_phone = _find_pending_phone(session, company.id, normalized_phone)
    if pending_phone is not None:
        if len(phone_value.strip()) > len((pending_phone.phone_number or "").strip()):
            pending_phone.phone_number = phone_value
        pending_phone.source_page_url = pending_phone.source_page_url or source_page_url
        pending_phone.source_type = pending_phone.source_type or source_type
        pending_phone.technical_metadata = _merge_provenance_metadata(
            pending_phone.technical_metadata,
            source_type=source_type,
            source_page_url=source_page_url,
            payload=metadata,
        )
        pending_phone.last_seen_at = datetime.now(timezone.utc)
        return normalized_phone
    existing_phone = (
        session.query(Phone)
        .filter(Phone.company_id == company.id, Phone.normalized_number == normalized_phone)
        .one_or_none()
    )
    merged_metadata = _merge_provenance_metadata(
        existing_phone.technical_metadata if existing_phone else {},
        source_type=source_type,
        source_page_url=source_page_url,
        payload=metadata,
    )
    if existing_phone is None:
        session.add(
            Phone(
                company_id=company.id,
                phone_number=phone_value,
                normalized_number=normalized_phone,
                source_type=source_type,
                source_page_url=source_page_url,
                technical_metadata=merged_metadata,
            )
        )
        return normalized_phone
    if len(phone_value.strip()) > len((existing_phone.phone_number or "").strip()):
        existing_phone.phone_number = phone_value
    existing_phone.source_page_url = existing_phone.source_page_url or source_page_url
    existing_phone.source_type = existing_phone.source_type or source_type
    existing_phone.technical_metadata = merged_metadata
    existing_phone.last_seen_at = datetime.now(timezone.utc)
    session.add(existing_phone)
    return normalized_phone


def persist_contact_channel(
    session: Session,
    company: Company,
    *,
    channel_type: str,
    channel_value: str,
    normalized_value: str,
    source_type: str,
    source_page_url: str | None,
    metadata: dict | None = None,
) -> None:
    channel_type_enum = ContactChannelType(channel_type)
    pending = _find_pending_contact_channel(session, company.id, channel_type_enum, normalized_value)
    if pending is not None:
        if len(channel_value.strip()) > len((pending.channel_value or "").strip()):
            pending.channel_value = channel_value
        pending.source_page_url = pending.source_page_url or source_page_url
        pending.source_type = pending.source_type or source_type
        pending.technical_metadata = _merge_provenance_metadata(
            pending.technical_metadata,
            source_type=source_type,
            source_page_url=source_page_url,
            payload=metadata,
        )
        pending.last_seen_at = datetime.now(timezone.utc)
        return
    existing = (
        session.query(ContactChannel)
        .filter(
            ContactChannel.company_id == company.id,
            ContactChannel.channel_type == channel_type_enum,
            ContactChannel.normalized_value == normalized_value,
        )
        .one_or_none()
    )
    merged_metadata = _merge_provenance_metadata(
        existing.technical_metadata if existing else {},
        source_type=source_type,
        source_page_url=source_page_url,
        payload=metadata,
    )
    if existing is None:
        session.add(
            ContactChannel(
                company_id=company.id,
                channel_type=channel_type_enum,
                channel_value=channel_value,
                normalized_value=normalized_value,
                source_type=source_type,
                source_page_url=source_page_url,
                technical_metadata=merged_metadata,
            )
        )
        return
    if len(channel_value.strip()) > len((existing.channel_value or "").strip()):
        existing.channel_value = channel_value
    existing.source_page_url = existing.source_page_url or source_page_url
    existing.source_type = existing.source_type or source_type
    existing.technical_metadata = merged_metadata
    existing.last_seen_at = datetime.now(timezone.utc)
    session.add(existing)


def persist_overpass_contacts(session: Session, company: Company, tags: dict, default_region_code: str | None) -> None:
    def iter_tag_values(raw_value: str | None) -> list[str]:
        if not raw_value:
            return []
        return [item.strip() for item in raw_value.replace("|", ";").split(";") if item.strip()]

    email_tags = ("email", "contact:email")
    phone_tags = ("phone", "contact:phone", "mobile", "contact:mobile")
    whatsapp_tags = ("contact:whatsapp", "whatsapp")
    telegram_tags = ("contact:telegram", "telegram")

    for key in email_tags:
        for value in iter_tag_values(tags.get(key)):
            persist_email_value(
                session,
                company,
                value,
                source_type="overpass_tag",
                source_page_url=None,
                metadata={"tag": key},
            )

    for key in phone_tags:
        for value in iter_tag_values(tags.get(key)):
            persist_phone_value(
                session,
                company,
                value,
                source_type="overpass_tag",
                source_page_url=None,
                metadata={"tag": key},
                default_region_code=default_region_code,
            )

    for key in whatsapp_tags:
        for value in iter_tag_values(tags.get(key)):
            normalized_value = normalize_phone_number(value, default_region_code=default_region_code)
            if normalized_value:
                persist_contact_channel(
                    session,
                    company,
                    channel_type=ContactChannelType.WHATSAPP,
                    channel_value=value,
                    normalized_value=normalized_value,
                    source_type="overpass_tag",
                    source_page_url=None,
                    metadata={"tag": key},
                )

    for key in telegram_tags:
        for value in iter_tag_values(tags.get(key)):
            normalized_value = normalize_telegram_value(value)
            if normalized_value:
                persist_contact_channel(
                    session,
                    company,
                    channel_type=ContactChannelType.TELEGRAM,
                    channel_value=value,
                    normalized_value=normalized_value,
                    source_type="overpass_tag",
                    source_page_url=None,
                    metadata={"tag": key},
                )


def persist_crawl(
    session: Session,
    company: Company,
    run_id: int | None = None,
    *,
    crawler=crawl_site,
    request_provider: str = "website",
    crawler_kwargs: dict | None = None,
    ) -> object:
    if not company.website_url:
        company.crawl_status = "no_website"
        session.add(company)
        return None
    default_region_code = company.region.country_code if company.region else None

    def on_request(**metric):
        record_request_metric(
            session,
            provider=request_provider,
            request_kind="crawl",
            run_id=run_id,
            company_id=company.id,
            **metric,
        )

    effective_crawler_kwargs = dict(crawler_kwargs or {})
    effective_crawler_kwargs.setdefault("default_region_code", default_region_code)
    result = crawler(company.website_url, on_request=on_request, **effective_crawler_kwargs)
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

        phone_map: dict[str, str] = {}
        for phone_value in page_result.phones:
            normalized_phone = persist_phone_value(
                session,
                company,
                phone_value,
                source_type="regex",
                source_page_url=page_result.url,
                metadata={"title": page_result.title},
                default_region_code=default_region_code,
            )
            if normalized_phone:
                phone_map[normalized_phone] = phone_value

        for email_value in page_result.emails:
            persist_email_value(
                session,
                company,
                email_value,
                source_type="regex",
                source_page_url=page_result.url,
                metadata={"title": page_result.title},
            )

        for channel in page_result.channels:
            metadata = {"title": page_result.title}
            normalized_channel_value = channel.get("normalized_value")
            if channel.get("channel_type") == "whatsapp" and normalized_channel_value in phone_map:
                metadata["linked_phone"] = phone_map[normalized_channel_value]
            persist_contact_channel(
                session,
                company,
                channel_type=channel["channel_type"],
                channel_value=channel["channel_value"],
                normalized_value=normalized_channel_value,
                source_type="channel_link",
                source_page_url=page_result.url,
                metadata=metadata,
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
    return result


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

    proxy = None
    owner = f"run-{run_id}-company-{company_id}-crawler"
    try:
        if active_proxy_count(session, ProxyKind.CRAWLER) > 0:
            proxy = acquire_proxy(session, owner=owner, workload=ProxyKind.CRAWLER)
            if proxy is None:
                requeue_run_company(session, run_id, company_id, "Waiting for crawler proxy slot.")
                session.commit()
                from app.tasks import crawl_company
                crawl_company.send_with_options(args=(run_id, company_id), delay=5_000)
                return
            session.commit()
        proxy_url = render_proxy_url(proxy, owner=owner, workload=ProxyKind.CRAWLER)
        result = persist_crawl(
            session=session,
            company=company,
            run_id=run_id,
            crawler_kwargs={"proxy_url": proxy_url},
        )
        if settings.browser_fallback_enabled and result is not None and should_browser_escalate(result):
            requeue_run_company(session, run_id, company_id, "Escalated to browser crawl.")
            session.commit()
            from app.tasks import browser_crawl_company
            browser_crawl_company.send(run_id, company_id)
            return
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.COMPLETED)
    except Exception as exc:
        session.rollback()
        run = session.get(ScrapeRun, run_id)
        company = session.get(Company, company_id)
        if run is None or company is None:
            return
        proxy_failed = _is_proxy_transport_error(exc, proxy_url if "proxy_url" in locals() else None)
        release_proxy(
            session,
            proxy.id if proxy else None,
            owner=owner,
            workload=ProxyKind.CRAWLER,
            failed=proxy_failed,
            record_result=proxy_failed,
        )
        if _is_dead_host_error(exc):
            company.crawl_status = "failed"
            session.add(company)
            mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.FAILED, f"Dead host: {str(exc)[:500]}")
            session.refresh(run)
            if run.cancel_requested:
                finalize_cancelled_run(session, run, "Run stopped by request.")
            maybe_complete_run(session, run_id)
            session.commit()
            return
        if _schedule_retry(
            session,
            run_id=run_id,
            company_id=company_id,
            workload=ProxyKind.CRAWLER,
            reason=str(exc)[:500],
        ):
            company.crawl_status = "retrying"
            session.add(company)
            session.commit()
            return
        company.crawl_status = "failed"
        session.add(company)
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.FAILED, str(exc))
    else:
        release_proxy(session, proxy.id if proxy else None, owner=owner, workload=ProxyKind.CRAWLER, failed=False)
    session.refresh(run)
    if run.cancel_requested:
        finalize_cancelled_run(session, run, "Run stopped by request.")
    maybe_complete_run(session, run_id)
    session.commit()


def execute_browser_crawl(session: Session, run_id: int, company_id: int) -> None:
    run = session.get(ScrapeRun, run_id)
    company = session.get(Company, company_id)
    if run is None or company is None:
        return
    session.refresh(run)
    if run.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.SKIPPED}:
        return
    if run.cancel_requested:
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.SKIPPED, "Cancelled before browser crawl start.")
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

    proxy = None
    owner = f"run-{run_id}-company-{company_id}"
    try:
        if active_proxy_count(session, ProxyKind.BROWSER) > 0:
            proxy = acquire_proxy(session, owner=owner, workload=ProxyKind.BROWSER)
            if proxy is None:
                requeue_run_company(session, run_id, company_id, "Waiting for proxy worker slot.")
                session.commit()
                from app.tasks import browser_crawl_company
                browser_crawl_company.send_with_options(args=(run_id, company_id), delay=5_000)
                return
            session.commit()
        proxy_url = render_proxy_url(proxy, owner=owner, workload=ProxyKind.BROWSER)
        persist_crawl(
            session=session,
            company=company,
            run_id=run_id,
            crawler=browser_crawl_site,
            request_provider="browser",
            crawler_kwargs={"proxy_url": proxy_url},
        )
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.COMPLETED)
    except Exception as exc:
        session.rollback()
        run = session.get(ScrapeRun, run_id)
        company = session.get(Company, company_id)
        if run is None or company is None:
            return
        proxy_failed = _is_proxy_transport_error(exc, proxy_url if "proxy_url" in locals() else None)
        release_proxy(
            session,
            proxy.id if proxy else None,
            owner=owner,
            workload=ProxyKind.BROWSER,
            failed=proxy_failed,
            record_result=proxy_failed,
        )
        if _is_dead_host_error(exc):
            company.crawl_status = "failed"
            session.add(company)
            mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.FAILED, f"Dead host: {str(exc)[:500]}")
            session.refresh(run)
            if run.cancel_requested:
                finalize_cancelled_run(session, run, "Run stopped by request.")
            maybe_complete_run(session, run_id)
            session.commit()
            return
        if _schedule_retry(
            session,
            run_id=run_id,
            company_id=company_id,
            workload=ProxyKind.BROWSER,
            reason=str(exc)[:500],
        ):
            company.crawl_status = "retrying"
            session.add(company)
            session.commit()
            return
        company.crawl_status = "failed"
        session.add(company)
        mark_run_company_finished(session, run_id, company_id, RunCompanyStatus.FAILED, str(exc))
    else:
        release_proxy(session, proxy.id if proxy else None, owner=owner, workload=ProxyKind.BROWSER, failed=False)

    session.refresh(run)
    if run.cancel_requested:
        finalize_cancelled_run(session, run, "Run stopped by request.")
    maybe_complete_run(session, run_id)
    session.commit()
