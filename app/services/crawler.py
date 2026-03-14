from __future__ import annotations

import re
import time
from dataclasses import dataclass
from html import unescape
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup

from app.config import get_settings


EMAIL_REGEX = re.compile(r"(?i)([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})")
PHONE_REGEX = re.compile(r"(?<!\w)(\+?\d[\d\s()./\-]{6,}\d)")
CONTACT_PATH_HINTS = (
    "contact",
    "contact-us",
    "contactus",
    "about",
    "about-us",
    "reservation",
    "booking",
)
SOCIAL_HOSTS = ("facebook.com", "instagram.com", "linkedin.com", "tiktok.com", "youtube.com")
CAPTCHA_HINTS = ("captcha", "g-recaptcha", "hcaptcha", "cf-turnstile")
ASSET_SCAN_EXTENSIONS = (".js", ".mjs", ".css", ".json")
PHONE_CONTEXT_HINTS = ("phone", "tel", "telephone", "mobile", "call", "contact", "whatsapp", "hotline", "โทร", "มือถือ")
ANTI_BOT_HINTS = (
    "verify you're not a robot",
    "verify you are not a robot",
    "attention required",
    "please enable cookies",
    "cloudflare",
    "g-recaptcha-response",
)
NOISE_EMAIL_DOMAINS = (
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
    "myemail.com",
)
NOISE_EMAIL_LOCALPARTS = {
    "johnsmith",
    "janedoe",
    "test",
    "example",
}
COMMON_MAILBOX_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "ymail.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}

settings = get_settings()
BROWSER_FALLBACK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def build_httpx_client(
    *,
    headers: dict[str, str],
    verify: bool = True,
) -> httpx.Client:
    return httpx.Client(
        timeout=settings.request_timeout_seconds,
        headers=headers,
        follow_redirects=True,
        verify=verify,
        proxy=settings.crawler_proxy_url or None,
    )


@dataclass
class CrawlPageResult:
    url: str
    title: str | None
    status_code: int | None
    emails: list[str]
    phones: list[str]
    social_links: list[str]
    has_contact_form: bool
    forms: list[dict]
    error: str | None = None


@dataclass
class CrawlSiteResult:
    pages: list[CrawlPageResult]
    crawl_status: str


def normalize_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def fetch_robots_allowed(base_url: str) -> bool:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        parser.read()
    except Exception:
        return True
    return parser.can_fetch(settings.user_agent, base_url)


def _is_ssl_verification_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return "CERTIFICATE_VERIFY_FAILED" in message or "SELF-SIGNED CERTIFICATE" in message


def same_site_family(source_url: str, target_url: str) -> bool:
    source_host = urlparse(source_url).netloc.lower().removeprefix("www.")
    target_host = urlparse(target_url).netloc.lower().removeprefix("www.")
    return source_host == target_host or source_host.endswith(f".{target_host}") or target_host.endswith(f".{source_host}")


def fetch_page(
    client: httpx.Client,
    candidate: str,
    *,
    headers: dict[str, str],
    on_request=None,
) -> tuple[httpx.Response, bool]:
    started = time.perf_counter()
    try:
        response = client.get(candidate)
        duration_ms = int((time.perf_counter() - started) * 1000)
        if on_request:
            on_request(
                method="GET",
                url=str(response.url),
                status_code=response.status_code,
                duration_ms=duration_ms,
                error=None,
            )
        return response, False
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        if on_request:
            on_request(
                method="GET",
                url=candidate,
                status_code=None,
                duration_ms=duration_ms,
                error=str(exc),
            )
        if not settings.crawler_insecure_ssl_fallback or not _is_ssl_verification_error(exc):
            raise

    started = time.perf_counter()
    with build_httpx_client(headers=headers, verify=False) as insecure_client:
        response = insecure_client.get(candidate)
    duration_ms = int((time.perf_counter() - started) * 1000)
    if on_request:
        on_request(
            method="GET",
            url=str(response.url),
            status_code=response.status_code,
            duration_ms=duration_ms,
            error="ssl_verification_bypassed",
        )
    return response, True


def should_retry_with_browser_headers(response: httpx.Response, headers: dict[str, str]) -> bool:
    if headers.get("User-Agent") == BROWSER_FALLBACK_HEADERS["User-Agent"]:
        return False
    if response.status_code not in {401, 403}:
        return False
    body = response.text.lower()
    return any(hint in body for hint in ANTI_BOT_HINTS)


def extract_forms(soup: BeautifulSoup, page_url: str) -> tuple[bool, list[dict]]:
    forms = []
    for form in soup.find_all("form"):
        fields = []
        for field in form.find_all(["input", "textarea", "select"]):
            fields.append(
                {
                    "name": field.get("name"),
                    "type": field.get("type", field.name),
                    "placeholder": field.get("placeholder"),
                    "required": field.has_attr("required"),
                    "value": field.get("value"),
                }
            )

        html_blob = str(form).lower()
        forms.append(
            {
                "action_url": urljoin(page_url, form.get("action", "") or ""),
                "method": (form.get("method") or "get").lower(),
                "has_captcha": any(hint in html_blob for hint in CAPTCHA_HINTS),
                "fields": fields,
            }
        )
    return bool(forms), forms


def decode_cloudflare_email(encoded_value: str | None) -> str | None:
    if not encoded_value:
        return None
    try:
        key = int(encoded_value[:2], 16)
        decoded = "".join(
            chr(int(encoded_value[index:index + 2], 16) ^ key)
            for index in range(2, len(encoded_value), 2)
        )
    except Exception:
        return None
    return decoded


def is_noise_email(email: str) -> bool:
    normalized = email.strip().lower()
    localpart, _, _ = normalized.partition("@")
    return any(normalized.endswith(f"@{domain}") for domain in NOISE_EMAIL_DOMAINS) or localpart in NOISE_EMAIL_LOCALPARTS


def is_asset_candidate_email(email: str, page_url: str) -> bool:
    normalized = email.strip().lower()
    _, _, domain = normalized.partition("@")
    page_host = urlparse(page_url).netloc.lower().removeprefix("www.")
    if not domain:
        return False
    if domain == page_host or domain.endswith(f".{page_host}") or page_host.endswith(f".{domain}"):
        return True
    return domain in COMMON_MAILBOX_DOMAINS


def extract_emails(soup: BeautifulSoup) -> list[str]:
    extraction_soup = BeautifulSoup(str(soup), "html.parser")
    for tag in extraction_soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    candidates: set[str] = set()
    visible_text = unescape(extraction_soup.get_text(" ", strip=True))
    candidates.update(match.group(1).lower() for match in EMAIL_REGEX.finditer(visible_text))

    for link in extraction_soup.find_all("a", href=True):
        href = unescape(link["href"]).strip()
        if href.lower().startswith("mailto:"):
            candidates.update(match.group(1).lower() for match in EMAIL_REGEX.finditer(href))
        link_text = unescape(link.get_text(" ", strip=True))
        candidates.update(match.group(1).lower() for match in EMAIL_REGEX.finditer(link_text))

    for protected_email in extraction_soup.select("a.__cf_email__, span.__cf_email__"):
        decoded = decode_cloudflare_email(protected_email.get("data-cfemail"))
        if decoded:
            candidates.add(decoded.lower())

    return sorted(email for email in candidates if not is_noise_email(email))


def normalize_phone_number(value: str) -> str | None:
    candidate = re.sub(r"(?i)(ext|extension|x)\s*\d+$", "", value).strip()
    if not candidate:
        return None
    lowered = candidate.lower()
    if ":" in lowered or re.search(r"\b\d{1,2}[.:]\d{2}\b", lowered):
        return None
    if re.search(r"\b(19|20)\d{2}\b", lowered):
        return None
    has_plus = candidate.startswith("+")
    digits = re.sub(r"\D", "", candidate)
    if len(digits) < 9 or len(digits) > 15:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
        has_plus = True
    if digits.startswith("66") and len(digits) in {10, 11}:
        digits = f"0{digits[2:]}"
        has_plus = False
    if not has_plus and not (digits.startswith("0") or digits.startswith("66")):
        return None
    return f"+{digits}" if has_plus else digits


def is_noise_phone(phone: str) -> bool:
    digits = re.sub(r"\D", "", phone)
    if len(set(digits)) == 1:
        return True
    return digits in {"12345678", "123456789", "1234567890"}


def extract_phones(soup: BeautifulSoup) -> list[str]:
    extraction_soup = BeautifulSoup(str(soup), "html.parser")
    for tag in extraction_soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str) -> None:
        normalized = normalize_phone_number(unescape(raw_value))
        if not normalized or normalized in seen or is_noise_phone(normalized):
            return
        seen.add(normalized)
        ordered.append((normalized, raw_value.strip()))

    visible_text = unescape(extraction_soup.get_text(" ", strip=True))
    for match in PHONE_REGEX.finditer(visible_text):
        start = max(match.start() - 32, 0)
        end = min(match.end() + 32, len(visible_text))
        context = visible_text[start:end].lower()
        if any(hint in context for hint in PHONE_CONTEXT_HINTS):
            add_candidate(match.group(1))

    for link in extraction_soup.find_all("a", href=True):
        href = unescape(link["href"]).strip()
        if href.lower().startswith("tel:"):
            add_candidate(href.split(":", 1)[1])
        if "wa.me/" in href.lower() or "api.whatsapp.com/send" in href.lower():
            digits = "".join(re.findall(r"\d+", href))
            if digits:
                add_candidate(f"+{digits}")
        link_text = unescape(link.get_text(" ", strip=True))
        if any(hint in link_text.lower() for hint in PHONE_CONTEXT_HINTS):
            for match in PHONE_REGEX.finditer(link_text):
                add_candidate(match.group(1))

    return [raw for _, raw in ordered]


def iter_same_origin_assets(soup: BeautifulSoup, page_url: str) -> list[str]:
    page_host = urlparse(page_url).netloc.lower().removeprefix("www.")
    assets: list[str] = []
    seen: set[str] = set()

    def consider(candidate: str | None) -> None:
        if not candidate:
            return
        absolute = urljoin(page_url, candidate)
        parsed = urlparse(absolute)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host or host != page_host:
            return
        path = parsed.path.lower()
        if not path.endswith(ASSET_SCAN_EXTENSIONS):
            return
        if absolute in seen:
            return
        seen.add(absolute)
        assets.append(absolute)

    for tag in soup.find_all("script", src=True):
        consider(tag.get("src"))
    for tag in soup.find_all("link", href=True):
        rel = " ".join(tag.get("rel", [])).lower()
        if rel in {"stylesheet", "preload", "modulepreload"} or "stylesheet" in rel or "preload" in rel:
            consider(tag.get("href"))

    return assets[:4]


def extract_emails_from_assets(
    client: httpx.Client,
    soup: BeautifulSoup,
    page_url: str,
    *,
    headers: dict[str, str],
    on_request=None,
) -> list[str]:
    candidates: set[str] = set()
    for asset_url in iter_same_origin_assets(soup=soup, page_url=page_url):
        try:
            response, _ = fetch_page(client, asset_url, headers=headers, on_request=on_request)
        except Exception:
            continue
        if response.status_code >= 400:
            continue
        content_type = (response.headers.get("content-type") or "").lower()
        if "javascript" not in content_type and "json" not in content_type and "css" not in content_type and "text" not in content_type:
            continue
        candidates.update(match.group(1).lower() for match in EMAIL_REGEX.finditer(unescape(response.text)))
    return sorted(
        email
        for email in candidates
        if not is_noise_email(email) and is_asset_candidate_email(email, page_url)
    )


def should_scan_assets(soup: BeautifulSoup, page_url: str) -> bool:
    visible_text = unescape(soup.get_text(" ", strip=True))
    if len(visible_text) >= 300:
        return False
    shell_ids = {"root", "app", "__next"}
    if any(tag.get("id") in shell_ids for tag in soup.find_all(True)):
        return True
    return bool(iter_same_origin_assets(soup=soup, page_url=page_url))


def should_browser_escalate(result: CrawlSiteResult) -> bool:
    if not result.pages:
        return result.crawl_status in {"blocked_by_robots", "failed"}

    has_contacts = any(page.emails or page.phones or page.has_contact_form for page in result.pages)
    if has_contacts:
        return False

    if any((page.status_code or 0) in {401, 403} for page in result.pages):
        return True

    if any(page.error == "cross_domain_redirect_after_ssl_fallback" for page in result.pages):
        return False

    return result.crawl_status in {"blocked_by_robots", "robots_bypassed", "failed", "completed"}


def crawl_site(website_url: str, on_request=None) -> CrawlSiteResult:
    website_url = normalize_url(website_url)
    robots_blocked = not fetch_robots_allowed(website_url)
    if robots_blocked and not settings.crawler_ignore_robots:
        return CrawlSiteResult(pages=[], crawl_status="blocked_by_robots")

    base = urlparse(website_url)
    candidates = [website_url]
    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(website_url, f"/{path}"))

    seen = set()
    pages = []
    headers = {"User-Agent": settings.user_agent}

    with build_httpx_client(headers=headers) as client:
        for candidate in candidates[: settings.max_pages_per_site]:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                response, insecure_fallback = fetch_page(client, candidate, headers=headers, on_request=on_request)
                if should_retry_with_browser_headers(response, headers):
                    with build_httpx_client(
                        headers=BROWSER_FALLBACK_HEADERS,
                        verify=not insecure_fallback,
                    ) as browser_client:
                        response, insecure_fallback = fetch_page(
                            browser_client,
                            candidate,
                            headers=BROWSER_FALLBACK_HEADERS,
                            on_request=on_request,
                        )
                if insecure_fallback and not same_site_family(candidate, str(response.url)):
                    pages.append(
                        CrawlPageResult(
                            url=str(response.url),
                            title=None,
                            status_code=response.status_code,
                            emails=[],
                            phones=[],
                            social_links=[],
                            has_contact_form=False,
                            forms=[],
                            error="cross_domain_redirect_after_ssl_fallback",
                        )
                    )
                    continue
                soup = BeautifulSoup(response.text, "html.parser")
                title = soup.title.text.strip() if soup.title and soup.title.text else None
                emails = extract_emails(soup)
                phones = extract_phones(soup)
                if not emails and should_scan_assets(soup, str(response.url)):
                    emails = extract_emails_from_assets(
                        client=client,
                        soup=soup,
                        page_url=str(response.url),
                        headers=headers,
                        on_request=on_request,
                    )
                social_links = []
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    absolute = urljoin(candidate, href)
                    parsed = urlparse(absolute)
                    if parsed.netloc.endswith(base.netloc) and any(hint in parsed.path.lower() for hint in CONTACT_PATH_HINTS):
                        if absolute not in seen and len(candidates) < settings.max_pages_per_site:
                            candidates.append(absolute)
                    if any(host in absolute for host in SOCIAL_HOSTS):
                        social_links.append(absolute)
                has_contact_form, forms = extract_forms(soup=soup, page_url=str(response.url))
                pages.append(
                    CrawlPageResult(
                        url=str(response.url),
                        title=title,
                        status_code=response.status_code,
                        emails=emails[: settings.max_emails_per_company],
                        phones=phones,
                        social_links=sorted(set(social_links)),
                        has_contact_form=has_contact_form,
                        forms=forms,
                    )
                )
            except Exception as exc:
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

    crawl_status = "completed" if any(page.status_code for page in pages) else "failed"
    if crawl_status == "completed" and robots_blocked:
        crawl_status = "robots_bypassed"
    return CrawlSiteResult(pages=pages, crawl_status=crawl_status)
