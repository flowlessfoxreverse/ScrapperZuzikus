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
CONTACT_PATH_HINTS = ("contact", "about", "reservation", "booking")
SOCIAL_HOSTS = ("facebook.com", "instagram.com", "linkedin.com", "tiktok.com", "youtube.com")
CAPTCHA_HINTS = ("captcha", "g-recaptcha", "hcaptcha", "cf-turnstile")
NOISE_EMAIL_DOMAINS = (
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
)

settings = get_settings()


@dataclass
class CrawlPageResult:
    url: str
    title: str | None
    status_code: int | None
    emails: list[str]
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
    return any(normalized.endswith(f"@{domain}") for domain in NOISE_EMAIL_DOMAINS)


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


def crawl_site(website_url: str, on_request=None) -> CrawlSiteResult:
    website_url = normalize_url(website_url)
    if not fetch_robots_allowed(website_url):
        return CrawlSiteResult(pages=[], crawl_status="blocked_by_robots")

    base = urlparse(website_url)
    candidates = [website_url]
    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(website_url, f"/{path}"))

    seen = set()
    pages = []
    headers = {"User-Agent": settings.user_agent}

    with httpx.Client(timeout=settings.request_timeout_seconds, headers=headers, follow_redirects=True) as client:
        for candidate in candidates[: settings.max_pages_per_site]:
            if candidate in seen:
                continue
            seen.add(candidate)
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
                soup = BeautifulSoup(response.text, "html.parser")
                title = soup.title.text.strip() if soup.title and soup.title.text else None
                emails = extract_emails(soup)
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
                        social_links=sorted(set(social_links)),
                        has_contact_form=has_contact_form,
                        forms=forms,
                    )
                )
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
                pages.append(
                    CrawlPageResult(
                        url=candidate,
                        title=None,
                        status_code=None,
                        emails=[],
                        social_links=[],
                        has_contact_form=False,
                        forms=[],
                        error=str(exc),
                    )
                )

    crawl_status = "completed" if any(page.status_code for page in pages) else "failed"
    return CrawlSiteResult(pages=pages, crawl_status=crawl_status)
