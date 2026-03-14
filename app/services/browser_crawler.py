from __future__ import annotations

import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.config import get_settings
from app.services.crawler import (
    BROWSER_FALLBACK_HEADERS,
    CONTACT_PATH_HINTS,
    SOCIAL_HOSTS,
    CrawlPageResult,
    CrawlSiteResult,
    extract_emails,
    extract_forms,
    extract_phones,
    normalize_url,
)


settings = get_settings()


def browser_crawl_site(website_url: str, on_request=None) -> CrawlSiteResult:
    from playwright.sync_api import sync_playwright

    website_url = normalize_url(website_url)
    base = urlparse(website_url)
    candidates = [website_url]
    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(website_url, f"/{path}"))

    seen: set[str] = set()
    pages: list[CrawlPageResult] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=BROWSER_FALLBACK_HEADERS["User-Agent"],
            locale="en-US",
            ignore_https_errors=True,
        )

        try:
            for candidate in candidates[: settings.browser_max_pages_per_site]:
                if candidate in seen:
                    continue
                seen.add(candidate)
                page = context.new_page()
                started = time.perf_counter()
                try:
                    response = page.goto(
                        candidate,
                        wait_until="domcontentloaded",
                        timeout=settings.browser_navigation_timeout_seconds * 1000,
                    )
                    try:
                        page.wait_for_load_state("networkidle", timeout=5_000)
                    except Exception:
                        pass
                    page.wait_for_timeout(settings.browser_wait_after_load_ms)
                    content = page.content()
                    final_url = page.url
                    status_code = response.status if response else 200
                    duration_ms = int((time.perf_counter() - started) * 1000)
                    if on_request:
                        on_request(
                            method="BROWSER",
                            url=final_url,
                            status_code=status_code,
                            duration_ms=duration_ms,
                            error=None,
                        )

                    soup = BeautifulSoup(content, "html.parser")
                    title = soup.title.text.strip() if soup.title and soup.title.text else None
                    emails = extract_emails(soup)
                    phones = extract_phones(soup)
                    social_links: list[str] = []
                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        absolute = urljoin(final_url, href)
                        parsed = urlparse(absolute)
                        if parsed.netloc.endswith(base.netloc) and any(hint in parsed.path.lower() for hint in CONTACT_PATH_HINTS):
                            if absolute not in seen and len(candidates) < settings.browser_max_pages_per_site:
                                candidates.append(absolute)
                        if any(host in absolute for host in SOCIAL_HOSTS):
                            social_links.append(absolute)
                    has_contact_form, forms = extract_forms(soup=soup, page_url=final_url)
                    pages.append(
                        CrawlPageResult(
                            url=final_url,
                            title=title,
                            status_code=status_code,
                            emails=emails[: settings.max_emails_per_company],
                            phones=phones,
                            social_links=sorted(set(social_links)),
                            has_contact_form=has_contact_form,
                            forms=forms,
                        )
                    )
                except Exception as exc:
                    duration_ms = int((time.perf_counter() - started) * 1000)
                    if on_request:
                        on_request(
                            method="BROWSER",
                            url=candidate,
                            status_code=None,
                            duration_ms=duration_ms,
                            error=str(exc),
                        )
                    pages.append(
                        CrawlPageResult(
                            url=candidate,
                            title=None,
                            status_code=None,
                            emails=[],
                            phones=[],
                            social_links=[],
                            has_contact_form=False,
                            forms=[],
                            error=str(exc),
                        )
                    )
                finally:
                    page.close()
        finally:
            context.close()
            browser.close()

    crawl_status = "completed" if any(page.status_code for page in pages) else "failed"
    return CrawlSiteResult(pages=pages, crawl_status=crawl_status)
