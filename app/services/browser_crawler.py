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

STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'th-TH', 'th'] });
Object.defineProperty(navigator, 'plugins', {
  get: () => [{ name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }],
});
Object.defineProperty(navigator, 'mimeTypes', {
  get: () => [{ type: 'application/pdf' }, { type: 'text/pdf' }],
});
window.chrome = window.chrome || { runtime: {} };
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
  window.navigator.permissions.query = (parameters) => (
    parameters && parameters.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery(parameters)
  );
}
"""

CHALLENGE_HINTS = (
    "security verification",
    "verify you are human",
    "verify you're human",
    "verify you're not a robot",
    "verify you are not a robot",
    "attention required",
    "cf-chl",
    "recaptcha",
    "captcha",
    "access denied",
    "temporarily unavailable",
)


def _looks_like_challenge(content: str, title: str | None, status_code: int | None) -> bool:
    haystack = f"{title or ''} {content[:4000]}".lower()
    if status_code in {401, 403, 429, 503}:
        return True
    return any(hint in haystack for hint in CHALLENGE_HINTS)


def _perform_humanish_actions(page) -> None:
    page.mouse.move(120, 160)
    page.wait_for_timeout(350)
    for step in range(settings.browser_stealth_scroll_steps):
        page.mouse.wheel(0, 600)
        page.wait_for_timeout(350 + step * 150)
    page.mouse.move(420, 320)
    page.wait_for_timeout(300)


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
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        context = browser.new_context(
            user_agent=BROWSER_FALLBACK_HEADERS["User-Agent"],
            locale="en-US",
            timezone_id="Asia/Bangkok",
            ignore_https_errors=True,
            viewport={"width": 1366, "height": 900},
            screen={"width": 1366, "height": 900},
            color_scheme="light",
        )
        context.set_extra_http_headers(
            {
                **BROWSER_FALLBACK_HEADERS,
                "Upgrade-Insecure-Requests": "1",
                "Sec-CH-UA": '"Chromium";v="123", "Google Chrome";v="123", "Not:A-Brand";v="99"',
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": '"Windows"',
            }
        )
        context.add_init_script(STEALTH_INIT_SCRIPT)

        try:
            for candidate in candidates[: settings.browser_max_pages_per_site]:
                if candidate in seen:
                    continue
                seen.add(candidate)
                page = context.new_page()
                started = time.perf_counter()
                try:
                    response = None
                    content = ""
                    final_url = candidate
                    status_code = None
                    title = None
                    for attempt in range(settings.browser_retry_attempts + 1):
                        response = page.goto(
                            candidate,
                            wait_until="domcontentloaded",
                            timeout=settings.browser_navigation_timeout_seconds * 1000,
                            referer=website_url,
                        )
                        try:
                            page.wait_for_load_state("networkidle", timeout=5_000)
                        except Exception:
                            pass
                        page.wait_for_timeout(settings.browser_wait_after_load_ms)
                        _perform_humanish_actions(page)
                        content = page.content()
                        final_url = page.url
                        status_code = response.status if response else 200
                        title = page.title().strip() if page.title() else None
                        if not _looks_like_challenge(content, title, status_code):
                            break
                        if attempt < settings.browser_retry_attempts:
                            page.wait_for_timeout(2_000 + attempt * 1_000)
                            page.reload(wait_until="domcontentloaded", timeout=settings.browser_navigation_timeout_seconds * 1000)

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
                    title = title or (soup.title.text.strip() if soup.title and soup.title.text else None)
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
                            error="anti_bot_challenge" if _looks_like_challenge(content, title, status_code) else None,
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
