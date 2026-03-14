from __future__ import annotations

import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.config import get_settings
from app.services.crawler import (
    BROWSER_FALLBACK_HEADERS,
    CONTACT_PATH_HINTS,
    PRIMARY_CONTACT_PATH_HINTS,
    SOCIAL_HOSTS,
    CrawlPageResult,
    CrawlSiteResult,
    build_httpx_client,
    extract_channels,
    extract_emails,
    extract_emails_from_assets,
    extract_forms,
    extract_pdf_page_results,
    extract_phones,
    extract_structured_contacts,
    is_social_or_chat_url,
    sanitize_company_website_url,
    normalize_url,
    should_scan_assets,
)
from app.services.host_suppression import clear_host_failures, is_host_suppressed, normalize_host_key, register_host_failure, suppress_host


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
THIRD_PARTY_BLOCK_HINTS = (
    "doubleclick.net",
    "googleadservices.com",
    "googletagmanager.com",
    "google-analytics.com",
    "facebook.net",
    "hotjar.com",
    "clarity.ms",
    "segment.io",
    "unpkg.com",
    "translate.googleapis.com",
    "gstatic.com",
    "ytimg.com",
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


def _build_playwright_proxy(proxy_url: str | None = None) -> dict[str, str] | None:
    target_proxy_url = proxy_url or settings.browser_proxy_url
    if not target_proxy_url:
        return None
    parsed = urlparse(target_proxy_url)
    if not parsed.scheme or not parsed.hostname:
        return None
    server = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        server = f"{server}:{parsed.port}"
    proxy: dict[str, str] = {"server": server}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    if settings.browser_proxy_bypass:
        proxy["bypass"] = settings.browser_proxy_bypass
    return proxy


def _apply_stealth(page) -> None:
    if not settings.browser_stealth_plugin_enabled:
        return
    try:
        from playwright_stealth import Stealth, stealth_sync
    except Exception:
        try:
            from playwright_stealth import stealth_sync
        except Exception:
            return
        stealth_sync(page)
        return

    try:
        stealth_sync(page)
        return
    except Exception:
        pass

    try:
        stealth = Stealth(init_scripts_only=True)
        if hasattr(stealth, "apply_stealth_sync"):
            stealth.apply_stealth_sync(page)
    except Exception:
        return


def browser_crawl_site(
    website_url: str,
    on_request=None,
    proxy_url: str | None = None,
    default_region_code: str | None = None,
) -> CrawlSiteResult:
    from playwright.sync_api import sync_playwright

    website_url = sanitize_company_website_url(website_url)
    if not website_url:
        return CrawlSiteResult(pages=[], crawl_status="no_website")
    if is_host_suppressed(website_url):
        if on_request:
            on_request(
                request_kind="suppressed_host",
                method="EVENT",
                url=website_url,
                status_code=None,
                duration_ms=0,
                error="suppressed_host",
            )
        return CrawlSiteResult(pages=[], crawl_status="suppressed_host")
    base = urlparse(website_url)
    base_host = normalize_host_key(website_url)
    candidates = [website_url]
    for path in PRIMARY_CONTACT_PATH_HINTS:
        candidates.append(urljoin(website_url, f"/{path}"))

    seen: set[str] = set()
    resolved_seen: set[str] = set()
    pages: list[CrawlPageResult] = []
    duplicate_final_url_attempts = 0

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            proxy=_build_playwright_proxy(proxy_url),
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
        if settings.browser_block_third_party_assets:
            def handle_route(route) -> None:
                request = route.request
                request_url = request.url
                request_host = normalize_host_key(request_url)
                same_origin = request_host == base_host
                resource_type = request.resource_type
                lower_url = request_url.lower()
                if request.is_navigation_request():
                    route.continue_()
                    return
                if resource_type in {"font", "media"}:
                    route.abort()
                    return
                if any(hint in lower_url for hint in THIRD_PARTY_BLOCK_HINTS):
                    route.abort()
                    return
                if not same_origin and resource_type in {"script", "xhr", "fetch", "image", "stylesheet", "manifest", "other"}:
                    route.abort()
                    return
                route.continue_()

            context.route("**/*", handle_route)

        try:
            for candidate in candidates[: settings.browser_max_pages_per_site]:
                if candidate in seen:
                    continue
                seen.add(candidate)
                page = context.new_page()
                started = time.perf_counter()
                try:
                    _apply_stealth(page)
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

                    resolved_key = f"{normalize_host_key(final_url)}|{urlparse(final_url).path.rstrip('/') or '/'}"
                    if resolved_key in resolved_seen:
                        duplicate_final_url_attempts += 1
                        if duplicate_final_url_attempts >= 2:
                            register_host_failure(website_url)
                            if on_request:
                                on_request(
                                    request_kind="early_stopped",
                                    method="EVENT",
                                    url=website_url,
                                    status_code=status_code,
                                    duration_ms=0,
                                    error=f"duplicate_final_url_after_{duplicate_final_url_attempts}",
                                )
                            break
                        continue
                    resolved_seen.add(resolved_key)
                    duplicate_final_url_attempts = 0

                    soup = BeautifulSoup(content, "html.parser")
                    title = title or (soup.title.text.strip() if soup.title and soup.title.text else None)
                    emails = set(extract_emails(soup))
                    phones = list(extract_phones(soup, default_region_code=default_region_code))
                    channels = extract_channels(soup, final_url, default_region_code=default_region_code)
                    structured_emails, structured_phones, structured_channels = extract_structured_contacts(
                        soup,
                        final_url,
                        default_region_code=default_region_code,
                    )
                    emails.update(structured_emails)
                    phones.extend(structured_phones)
                    channels.extend(structured_channels)
                    if not emails and should_scan_assets(soup, final_url):
                        with build_httpx_client(headers=BROWSER_FALLBACK_HEADERS, proxy_url=proxy_url) as asset_client:
                            emails.update(
                                extract_emails_from_assets(
                                    client=asset_client,
                                    soup=soup,
                                    page_url=final_url,
                                    headers=BROWSER_FALLBACK_HEADERS,
                                    on_request=on_request,
                                )
                            )
                    social_links: list[str] = []
                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        absolute = urljoin(final_url, href)
                        parsed = urlparse(absolute)
                        if parsed.netloc.endswith(base.netloc) and any(hint in parsed.path.lower() for hint in CONTACT_PATH_HINTS):
                            if absolute not in seen and len(candidates) < settings.browser_max_pages_per_site:
                                candidates.append(absolute)
                        if is_social_or_chat_url(absolute):
                            social_links.append(absolute)
                    has_contact_form, forms = extract_forms(soup=soup, page_url=final_url)
                    deduped_channels: dict[tuple[str, str], dict[str, str]] = {}
                    for channel in channels:
                        key = (channel["channel_type"], channel["normalized_value"])
                        deduped_channels.setdefault(key, channel)
                    pages.append(
                        CrawlPageResult(
                            url=final_url,
                            title=title,
                            status_code=status_code,
                            emails=sorted(emails)[: settings.max_emails_per_company],
                            phones=sorted(set(phones)),
                            channels=list(deduped_channels.values()),
                            social_links=sorted(set(social_links)),
                            has_contact_form=has_contact_form,
                            forms=forms,
                            error="anti_bot_challenge" if _looks_like_challenge(content, title, status_code) else None,
                        )
                    )
                    if _looks_like_challenge(content, title, status_code) and on_request:
                        on_request(
                            request_kind="anti_bot_challenge",
                            method="EVENT",
                            url=final_url,
                            status_code=status_code,
                            duration_ms=0,
                            error="anti_bot_challenge",
                        )
                    if emails or phones or channels or has_contact_form:
                        duplicate_final_url_attempts = 0
                        clear_host_failures(website_url)
                    with build_httpx_client(headers=BROWSER_FALLBACK_HEADERS, proxy_url=proxy_url) as pdf_client:
                        pages.extend(
                            extract_pdf_page_results(
                                client=pdf_client,
                                soup=soup,
                                page_url=final_url,
                                headers=BROWSER_FALLBACK_HEADERS,
                                on_request=on_request,
                                default_region_code=default_region_code,
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
                            channels=[],
                            social_links=[],
                            has_contact_form=False,
                            forms=[],
                            error=str(exc),
                        )
                    )
                    suppress_host(website_url)
                finally:
                    page.close()
        finally:
            context.close()
            browser.close()

    if any(page.error == "anti_bot_challenge" for page in pages):
        crawl_status = "anti_bot_challenge"
    elif any(page.status_code for page in pages):
        crawl_status = "completed"
    else:
        crawl_status = "failed"
    return CrawlSiteResult(pages=pages, crawl_status=crawl_status)
